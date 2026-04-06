use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use anyhow::{Result, anyhow};
use rusqlite::Connection;

use crate::cli::{SelectMode, TermInfo};
use crate::db;
use crate::db::{FindableByName, Queryable, QueryableByDat};
use crate::util::{self, OptionIf};

pub(crate) struct ListContext<'a> {
    pub(crate) conn: &'a Connection,
    pub(crate) dat_id: db::DatId,
    pub(crate) term: &'a TermInfo,
    pub(crate) partial_name: Option<&'a str>,
}

impl<'a> ListContext<'a> {
    pub(crate) fn new(
        conn: &'a Connection,
        dat_id: db::DatId,
        term: &'a TermInfo,
        partial_name: Option<&'a str>,
    ) -> Self {
        Self {
            conn,
            dat_id,
            term,
            partial_name,
        }
    }
}

macro_rules! println_if {
    ($cond:expr, $($arg:tt)*) => {
        if $cond {
            println!($($arg)*);
        }
    };
}

pub(crate) fn list_dat_files(conn: &Connection) -> Result<()> {
    let dats = db::DatRecord::get_all(conn)?;
    if dats.is_empty() {
        eprintln!("No installed dat files.")
    } else {
        println!("Installed dat files:");
        for (i, dat) in dats.iter().enumerate() {
            println!("[{i}] {} version: {}", dat.name, dat.version);
        }
    }
    Ok(())
}

pub(crate) fn list_dat_records(conn: &Connection, dat_id: db::DatId) -> Result<()> {
    let dat_record = db::DatRecord::get_by_id(conn, dat_id)?;
    println!("Name:        {}", dat_record.name);
    println!("Description: {}", dat_record.description);
    println!("Version:     {}", dat_record.version);
    println!("Author:      {}", dat_record.author);

    println!("--- SETS ---");
    for set in db::SetRecord::get_by_dat(conn, dat_id)? {
        println!("{}", set.name);
        for rom in set.get_roms(conn)? {
            println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size));
        }
    }
    Ok(())
}

pub(crate) fn list_sets(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
    let sets = if let Some(name) = name {
        db::SetRecord::find_by_name(conn, dat_id, name, false)
    } else {
        db::SetRecord::get_by_dat(conn, dat_id)
    }?;
    if sets.is_empty() {
        println!("No sets found.");
    } else {
        for set in sets {
            println!("{}", set.name);
        }
    }
    Ok(())
}

pub(crate) fn list_roms(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
    let roms = if let Some(name) = name {
        db::RomRecord::find_by_name(conn, dat_id, name, false)
    } else {
        db::RomRecord::get_by_dat(conn, dat_id)
    }?;
    if roms.is_empty() {
        println!("No roms found.");
    } else {
        let mut roms_by_set: BTreeMap<_, Vec<_>> = BTreeMap::new();
        roms.iter()
            .for_each(|rom| roms_by_set.entry(&rom.set_id).or_default().push(rom));

        let all_sets = db::SetRecord::get_by_dat(conn, dat_id)?;
        let sets_by_id: BTreeMap<_, _> = all_sets.iter().map(|s| (&s.id, s)).collect();

        for (set_id, roms) in roms_by_set {
            sets_by_id.get(&set_id).if_some(|set| {
                println!("{}", set.name);
                roms.iter()
                    .for_each(|rom| println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size)));
            });
        }
    }
    Ok(())
}

fn should_display_file_status(status: Option<db::MatchStatus>, mode: SelectMode) -> bool {
    matches!(
        (status, mode),
        (None, SelectMode::Unmatched | SelectMode::All)
            | (Some(db::MatchStatus::Hash), SelectMode::Warning | SelectMode::All)
            | (Some(db::MatchStatus::Name), SelectMode::Warning | SelectMode::All)
            | (Some(db::MatchStatus::Match), SelectMode::Matched | SelectMode::All)
    )
}

#[rustfmt::skip] //single line match arms are more readable
pub(crate) fn format_file_indicator(status: Option<db::MatchStatus>, is_tty: bool) -> &'static str {
    match status {
        None => if is_tty { "❌" } else { "NONE" },
        Some(db::MatchStatus::Hash) | Some(db::MatchStatus::Name) => if is_tty { "⚠️" } else { "WARN" },
        Some(db::MatchStatus::Match) => if is_tty { "✅" } else { " OK " },
    }
}

pub(crate) fn format_match_status(
    file: &db::FileRecord,
    matched: Option<(&db::MatchRecord, &db::RomRecord)>,
    is_tty: bool,
) -> String {
    let indicator = format_file_indicator(matched.map(|(m, _)| m.status), is_tty);
    match matched {
        None => format!("[{indicator}] {} {} - unknown file", file.hash, file.name),
        Some((m, rom)) => match m.status {
            db::MatchStatus::Hash => {
                format!("[{indicator}] {} {} - incorrect name, should be named {}", file.hash, file.name, rom.name)
            }
            db::MatchStatus::Name => {
                format!("[{indicator}] {} {} - incorrect hash, should have hash {}", file.hash, file.name, rom.hash)
            }
            db::MatchStatus::Match => {
                format!("[{indicator}] {} {}", file.hash, file.name)
            }
        },
    }
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

pub(crate) fn list_scanned_files(ctx: &ListContext<'_>, mode: SelectMode) -> Result<()> {
    //get these in bulk to avoid doing a query per file when we display them
    let matches = db::MatchRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    let matches_by_file: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.file_id).or_default().push(m);
        acc
    });

    // Bulk-load all rom records referenced by those matches
    let rom_ids: BTreeSet<_> = matches.iter().map(|m| m.rom_id).collect();
    let roms_by_id: BTreeMap<_, _> = db::RomRecord::get_by_ids(ctx.conn, &rom_ids)?
        .into_iter()
        .map(|r| (r.id, r))
        .collect();

    let dirs = db::DirRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    for dir in dirs {
        let files = if let Some(partial_name) = ctx.partial_name {
            dir.find_files(ctx.conn, partial_name, false)?
        } else {
            dir.get_files(ctx.conn)?
        };

        if files.is_empty() {
            continue;
        }

        let mut lines = Vec::new();
        for file in files {
            if let Some(file_matches) = matches_by_file.get(&file.id) {
                for fm in file_matches {
                    if should_display_file_status(Some(fm.status), mode) {
                        let rom = roms_by_id
                            .get(&fm.rom_id)
                            .ok_or_else(|| anyhow!("match references missing rom id {:?}", fm.rom_id))?;
                        lines.push(format_match_status(&file, Some((fm, rom)), ctx.term.tty_out));
                    }
                }
            } else if should_display_file_status(None, mode) {
                lines.push(format_match_status(&file, None, ctx.term.tty_out));
            }
        }

        if lines.is_empty() {
            continue;
        }

        println!("--- FILES IN '{}' ---", dir.path);
        let mut lock = std::io::stdout().lock();
        for line in lines {
            writeln!(lock, "{}", &line)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SetStatus {
    Missing,
    Partial,
    Complete,
}

pub(crate) fn calculate_set_status(set_roms: &[db::RomRecord], matched_rom_ids: &BTreeSet<db::RomId>) -> SetStatus {
    if matched_rom_ids.is_empty() {
        return SetStatus::Missing;
    }
    if set_roms.iter().all(|rom| matched_rom_ids.contains(&rom.id)) {
        SetStatus::Complete
    } else {
        SetStatus::Partial
    }
}

#[rustfmt::skip] //single line match arms are more readable
fn format_set_indicator(status: SetStatus, is_tty: bool) -> &'static str {
    match status {
        SetStatus::Missing => if is_tty { "❌" } else { "NONE" },
        SetStatus::Partial => if is_tty { "⚠️" } else { "WARN" },
        SetStatus::Complete => if is_tty { "✅" } else { " OK " },
    }
}

pub(crate) fn list_missing_sets(ctx: &ListContext<'_>) -> Result<()> {
    let matches = db::MatchRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    let all_sets = db::SetRecord::get_by_dat(ctx.conn, ctx.dat_id)?;

    let matches_by_set: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.set_id).or_default().push(m);
        acc
    });

    println!("--- MISSING SETS ---");
    let status = format_set_indicator(SetStatus::Missing, ctx.term.tty_out);
    for set in &all_sets {
        if let Some(partial_name) = ctx.partial_name
            && !contains_ascii_case_insensitive(&set.name, partial_name)
        {
            continue;
        }
        println_if!(!matches_by_set.contains_key(&set.id), "[{status}] {}", set.name);
    }
    println!("{} / {} sets missing.", all_sets.len() - matches_by_set.len(), all_sets.len());
    Ok(())
}

pub(crate) fn list_found_sets(ctx: &ListContext<'_>) -> Result<()> {
    //get all the matches so we know what sets we found
    let matches = db::MatchRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    let set_ids: BTreeSet<_> = matches.iter().map(|m| m.set_id).collect();
    let found_sets = db::SetRecord::get_by_ids(ctx.conn, &set_ids)?;
    let roms_by_set = db::RomRecord::get_by_sets(ctx.conn, &set_ids)?;
    let file_ids: BTreeSet<_> = matches.iter().map(|m| m.file_id).collect();
    let files_by_id: BTreeMap<_, _> = db::FileRecord::get_by_ids(ctx.conn, &file_ids)?
        .into_iter()
        .map(|f| (f.id, f))
        .collect();
    let matches_by_set: BTreeMap<_, Vec<_>> = matches.into_iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(m.set_id).or_default().push(m);
        acc
    });
    let all_set_count = db::SetRecord::get_num_by_dat(ctx.conn, ctx.dat_id)?;

    println!("--- FOUND SETS ---");
    let partial_status = format_set_indicator(SetStatus::Partial, ctx.term.tty_out);
    let complete_status = format_set_indicator(SetStatus::Complete, ctx.term.tty_out);
    for set in &found_sets {
        if let Some(partial_name) = ctx.partial_name
            && !contains_ascii_case_insensitive(&set.name, partial_name)
        {
            continue;
        }

        if let Some(set_roms) = roms_by_set.get(&set.id)
            && let Some(set_matches) = matches_by_set.get(&set.id)
        {
            let matched_rom_ids: BTreeSet<_> = set_matches.iter().map(|m| m.rom_id).collect();
            let roms_by_romid: BTreeMap<_, _> = set_roms.iter().map(|rom| (&rom.id, rom)).collect();

            if calculate_set_status(set_roms, &matched_rom_ids) == SetStatus::Complete {
                println!("[{complete_status}] {}", set.name);
            } else {
                println!("[{partial_status}] {}, set has missing roms", set.name);
            }

            for matched in set_matches {
                let file = files_by_id
                    .get(&matched.file_id)
                    .ok_or_else(|| anyhow!("set match references missing file id {:?}", matched.file_id))?;
                let rom = roms_by_romid
                    .get(&matched.rom_id)
                    .ok_or_else(|| anyhow!("set match references missing rom id {:?}", matched.rom_id))?;
                let status = format_match_status(file, Some((matched, rom)), ctx.term.tty_out);
                println!(" {status}");
            }

            let missing_indicator = format_file_indicator(None, ctx.term.tty_out);
            for rom in set_roms {
                println_if!(
                    !matched_rom_ids.contains(&rom.id),
                    " {missing_indicator}  {} {} - missing file",
                    rom.hash,
                    rom.name
                );
            }
        }
    }
    println!("{} / {} sets found.", found_sets.len(), all_set_count);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- helpers ---

    #[test]
    fn contains_ascii_case_insensitive_empty_needle() {
        assert!(contains_ascii_case_insensitive("anything", ""));
    }

    #[test]
    fn contains_ascii_case_insensitive_matches_mixed_case_substring() {
        assert!(contains_ascii_case_insensitive("Metal Slug", "sLuG"));
    }

    #[test]
    fn contains_ascii_case_insensitive_no_match() {
        assert!(!contains_ascii_case_insensitive("Metal Slug", "slugx"));
    }

    #[test]
    fn contains_ascii_case_insensitive_needle_longer_than_haystack() {
        assert!(!contains_ascii_case_insensitive("abc", "abcd"));
    }

    fn make_rom(id: i64, dat_id: i64, set_id: i64, size: u64, hash: &str) -> db::RomRecord {
        db::RomRecord {
            id: id.into(),
            dat_id: dat_id.into(),
            set_id: set_id.into(),
            name: format!("rom_{id}"),
            size,
            hash: hash.to_string(),
            crc: None,
        }
    }

    // --- should_display_file_status ---

    #[test]
    fn display_status_unmatched_file() {
        assert!(should_display_file_status(None, SelectMode::Unmatched));
        assert!(should_display_file_status(None, SelectMode::All));
        assert!(!should_display_file_status(None, SelectMode::Warning));
        assert!(!should_display_file_status(None, SelectMode::Matched));
    }

    #[test]
    fn display_status_hash_match() {
        assert!(should_display_file_status(Some(db::MatchStatus::Hash), SelectMode::Warning));
        assert!(should_display_file_status(Some(db::MatchStatus::Hash), SelectMode::All));
        assert!(!should_display_file_status(Some(db::MatchStatus::Hash), SelectMode::Matched));
        assert!(!should_display_file_status(Some(db::MatchStatus::Hash), SelectMode::Unmatched));
    }

    #[test]
    fn display_status_name_match() {
        assert!(should_display_file_status(Some(db::MatchStatus::Name), SelectMode::Warning));
        assert!(should_display_file_status(Some(db::MatchStatus::Name), SelectMode::All));
        assert!(!should_display_file_status(Some(db::MatchStatus::Name), SelectMode::Matched));
        assert!(!should_display_file_status(Some(db::MatchStatus::Name), SelectMode::Unmatched));
    }

    #[test]
    fn display_status_full_match() {
        assert!(should_display_file_status(Some(db::MatchStatus::Match), SelectMode::Matched));
        assert!(should_display_file_status(Some(db::MatchStatus::Match), SelectMode::All));
        assert!(!should_display_file_status(Some(db::MatchStatus::Match), SelectMode::Warning));
        assert!(!should_display_file_status(Some(db::MatchStatus::Match), SelectMode::Unmatched));
    }

    // --- calculate_set_status ---

    #[test]
    fn set_status_complete_when_all_roms_matched() {
        let roms = [make_rom(1, 1, 1, 100, "a"), make_rom(2, 1, 1, 200, "b")];
        let matched_ids = BTreeSet::from([1i64.into(), 2i64.into()]);
        assert_eq!(calculate_set_status(&roms, &matched_ids), SetStatus::Complete);
    }

    #[test]
    fn set_status_partial_when_some_roms_unmatched() {
        let roms = [make_rom(1, 1, 1, 100, "a"), make_rom(2, 1, 1, 200, "b")];
        let matched_ids = BTreeSet::from([1i64.into()]); // only rom 1 matched
        assert_eq!(calculate_set_status(&roms, &matched_ids), SetStatus::Partial);
    }

    #[test]
    fn set_status_missing_when_no_matches() {
        let roms = [make_rom(1, 1, 1, 100, "a")];
        assert_eq!(calculate_set_status(&roms, &BTreeSet::new()), SetStatus::Missing);
    }

    #[test]
    fn set_status_complete_with_single_rom() {
        let roms = [make_rom(1, 1, 1, 100, "a")];
        let matched_ids = BTreeSet::from([1i64.into()]);
        assert_eq!(calculate_set_status(&roms, &matched_ids), SetStatus::Complete);
    }
}
