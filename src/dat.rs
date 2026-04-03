use anyhow::{Context, Result, ensure};
use camino::Utf8Path;
use roxmltree::Document;
use rusqlite::Connection;

use crate::db;
use crate::db::Insertable;

const TAG_HEADER: &str = "header";
const ATTR_HEADER_NAME: &str = "name";
const ATTR_HEADER_DESC: &str = "description";
const ATTR_HEADER_VERSION: &str = "version";
const ATTR_HEADER_AUTHOR: &str = "author";

const TAG_GAME: &str = "game";
const ATTR_GAME_NAME: &str = "name";

const TAG_ROM: &str = "rom";
const ATTR_ROM_NAME: &str = "name";
const ATTR_ROM_SIZE: &str = "size";
const ATTR_ROM_HASH: &str = "sha1";
const ATTR_ROM_CRC: &str = "crc";

pub fn strip_doctype(s: &mut String) {
    let Some(start) = s.find("<!DOCTYPE") else { return };
    let mut depth = 0usize;
    let end = s[start..].char_indices().find(|(_, c)| match c {
        '[' => {
            depth += 1;
            false
        }
        ']' => {
            depth = depth.saturating_sub(1);
            false
        }
        '>' if depth == 0 => true,
        _ => false,
    });
    if let Some((len, _)) = end {
        s.replace_range(start..=start + len, "");
    }
}

pub fn import_dat<P: AsRef<Utf8Path>>(conn: &Connection, file_path: P) -> Result<db::DatRecord> {
    let mut df_buffer = std::fs::read_to_string(file_path.as_ref()).context("Unable to read reference dat file")?;
    //so we can turn off dtd-processing, we need to remove any declaration, in most files its unused and is a security issue.
    strip_doctype(&mut df_buffer);
    parse_dat(conn, &df_buffer)
}

pub fn parse_dat(conn: &Connection, df_buffer: &str) -> Result<db::DatRecord> {
    let df_xml = Document::parse(df_buffer).context("Unable to parse reference dat file")?;
    let new_dat = parse_dat_info(&df_xml)?;
    let dat = db::DatRecord::insert(conn, new_dat)?;

    for game_node in df_xml
        .root_element()
        .children()
        .filter(|node| node.tag_name().name() == TAG_GAME)
    {
        let game_name = game_node
            .attribute(ATTR_GAME_NAME)
            .context("Unable to read game name in reference dat file")?;

        let set = db::SetRecord::insert(conn, db::NewSet::new(dat.id, game_name))?;

        for rom_node in game_node.descendants().filter(|node| node.tag_name().name() == TAG_ROM) {
            let rom_name = rom_node.attribute(ATTR_ROM_NAME).context("Unable to read game name")?;
            let rom_size = rom_node.attribute(ATTR_ROM_SIZE).context("Unable to read game size")?;
            let rom_hash = rom_node.attribute(ATTR_ROM_HASH).context("Unable to read game hash")?;
            let rom_crc = rom_node.attribute(ATTR_ROM_CRC).map(normalize_crc).transpose()?;
            db::RomRecord::insert(
                conn,
                db::NewRom::new(
                    dat.id,
                    set.id,
                    rom_name,
                    rom_size.parse().context("should be a valid number")?,
                    rom_hash,
                    rom_crc,
                ),
            )?;
        }
    }
    Ok(dat)
}

pub fn normalize_crc(raw_crc: &str) -> Result<String> {
    let crc = raw_crc.trim();
    let crc = crc.strip_prefix("0x").or_else(|| crc.strip_prefix("0X")).unwrap_or(crc);
    ensure!(!crc.is_empty(), "crc should not be empty");
    let parsed = u32::from_str_radix(crc, 16).with_context(|| format!("invalid crc value '{raw_crc}'"))?;
    Ok(format!("{parsed:08x}"))
}

fn parse_dat_info(df_xml: &Document<'_>) -> Result<db::NewDat> {
    let mut name = None;
    let mut description = None;
    let mut version = None;
    let mut author = None;
    for header_node in df_xml
        .root_element()
        .children()
        .find(|node| node.tag_name().name() == TAG_HEADER)
        .map(|header| header.children())
        .context("Could not find header in reference dat file")?
    {
        match header_node.tag_name().name() {
            ATTR_HEADER_NAME => name = header_node.text(),
            ATTR_HEADER_DESC => description = header_node.text(),
            ATTR_HEADER_VERSION => version = header_node.text(),
            ATTR_HEADER_AUTHOR => author = header_node.text(),
            _ => {}
        };
    }
    let new_dat = db::NewDat::new(
        name.context("unable to find name attribute in header")?,
        description.context("unable to find description attribute in header")?,
        version.context("unable to find version attribute in header")?,
        author.context("unable to find author attribute in header")?,
        "sha1",
    );
    Ok(new_dat)
}

pub fn delete_dat(conn: &Connection, dat_id: db::DatId) -> Result<()> {
    use crate::db::{Deletable, DeletableByDat};

    //remove all scanned files and directories
    db::MatchRecord::delete_by_dat(conn, dat_id)?;
    db::FileRecord::delete_by_dat(conn, dat_id)?;
    db::DirRecord::delete_by_dat(conn, dat_id)?;

    //remove all roms and sets before removing the dat
    db::RomRecord::delete_by_dat(conn, dat_id)?;
    db::SetRecord::delete_by_dat(conn, dat_id)?;

    //remove the dat itself
    db::DatRecord::delete_by_id(conn, dat_id)?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::QueryableByDat;
    use std::collections::BTreeMap;

    fn stripped(input: &str) -> String {
        let mut s = input.to_string();
        strip_doctype(&mut s);
        s
    }

    #[test]
    fn strip_doctype_no_doctype() {
        let xml = r#"<?xml version="1.0"?><root/>"#;
        assert_eq!(stripped(xml), xml);
    }

    #[test]
    fn strip_doctype_simple() {
        let xml = r#"<?xml version="1.0"?><!DOCTYPE foo SYSTEM "foo.dtd"><root/>"#;
        assert_eq!(stripped(xml), r#"<?xml version="1.0"?><root/>"#);
    }

    #[test]
    fn strip_doctype_with_internal_subset() {
        let xml = r#"<!DOCTYPE foo [<!ELEMENT bar (baz)><!ELEMENT baz (#PCDATA)>]><root/>"#;
        assert_eq!(stripped(xml), "<root/>");
    }

    #[test]
    fn strip_doctype_internal_subset_gt_not_confused() {
        // The > inside the internal subset must not terminate the search early
        let xml = r#"<!DOCTYPE foo [<!ENTITY gt ">">]><root/>"#;
        assert_eq!(stripped(xml), "<root/>");
    }

    // --- parse_dat_info ---

    fn parse_info(xml: &str) -> Result<db::NewDat> {
        let doc = Document::parse(xml).unwrap();
        parse_dat_info(&doc)
    }

    #[test]
    fn parse_dat_info_valid() {
        let xml = r#"<datafile>
            <header>
                <name>My DAT</name>
                <description>A test dat</description>
                <version>1.0</version>
                <author>tester</author>
            </header>
        </datafile>"#;
        let dat = parse_info(xml).unwrap();
        assert_eq!(dat.name(), "My DAT");
        assert_eq!(dat.description(), "A test dat");
        assert_eq!(dat.version(), "1.0");
        assert_eq!(dat.author(), "tester");
        assert_eq!(dat.hash_type(), "sha1");
    }

    #[test]
    fn parse_dat_info_missing_header() {
        let xml = r#"<datafile><name>My DAT</name></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_name() {
        let xml = r#"<datafile><header>
            <description>desc</description><version>1.0</version><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_description() {
        let xml = r#"<datafile><header>
            <name>n</name><version>1.0</version><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_version() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_author() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><version>1.0</version>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_ignores_unknown_elements() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><version>1.0</version>
            <author>auth</author><unknown>ignored</unknown>
        </header></datafile>"#;
        assert!(parse_info(xml).is_ok());
    }

    #[test]
    fn parse_dat_reads_optional_crc_and_normalizes_it() {
        let conn = db::tests::mem_db();
        let xml = r#"<datafile>
            <header>
                <name>My DAT</name>
                <description>A test dat</description>
                <version>1.0</version>
                <author>tester</author>
            </header>
            <game name="Set One">
                <rom name="a.rom" size="1" sha1="aaa" crc="0x1A2b3C"/>
                <rom name="b.rom" size="2" sha1="bbb"/>
            </game>
        </datafile>"#;

        let dat = parse_dat(&conn, xml).unwrap();
        let roms = db::RomRecord::get_by_dat(&conn, dat.id).unwrap();
        let roms_by_name: BTreeMap<_, _> = roms.iter().map(|rom| (rom.name.as_str(), rom)).collect();

        assert_eq!(roms.len(), 2);
        assert_eq!(roms_by_name["a.rom"].crc.as_deref(), Some("001a2b3c"));
        assert_eq!(roms_by_name["b.rom"].crc, None);
    }

    #[test]
    fn normalize_crc_rejects_invalid_hex() {
        assert!(normalize_crc("not-hex").is_err());
    }
}
