use std::collections::BTreeSet;

use anyhow::{Context, Result};
use camino::Utf8Path;
use rusqlite::Connection;

use crate::db::{self, FindableByName, Insertable};

/// Bundles a database connection with the currently selected dat file ID.
/// Passed to any function that operates on a specific dat, avoiding the
/// repetition of carrying both as separate arguments everywhere.
pub(crate) struct DatContext<'a> {
    pub conn: &'a Connection,
    pub dat_id: db::DatId,
}

impl<'a> DatContext<'a> {
    pub fn new(conn: &'a Connection, dat_id: db::DatId) -> Self {
        Self { conn, dat_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FileMatch {
    pub status: db::MatchStatus,
    pub set_id: db::SetId,
    pub rom_id: db::RomId,
}

pub(crate) fn match_sets<P: AsRef<Utf8Path>>(ctx: &DatContext<'_>, path: P) -> Result<BTreeSet<db::SetId>> {
    let name = path.as_ref().file_prefix().context("should have a file name")?;
    let sets = db::SetRecord::find_by_name(ctx.conn, ctx.dat_id, name, true)?;
    let matched: BTreeSet<db::SetId> = sets.iter().map(|record| record.id).collect();
    Ok(matched)
}

fn resolve_match(
    file_size: u64,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
    named_roms: &[db::RomRecord],
    hash_roms: &[db::RomRecord],
) -> Option<Vec<FileMatch>> {
    // Step 1: if something is named the same, check for exact matches and return if so.
    if !named_roms.is_empty() {
        let exact_matches = match_exact(file_size, hash, matched_sets, named_roms);
        if exact_matches.is_some() {
            return exact_matches;
        }
    }
    // Step 2: if something is named the same, but the hash doesn't match,
    // check whether we got hash only matches if we ignore the filename.
    // If so, then treat it as a hash match, otherwise return the name only matches,
    // if there are any.
    if hash_roms.is_empty() {
        match_names(matched_sets, named_roms)
    } else {
        match_hashes(matched_sets, hash_roms)
    }
}

fn match_roms(
    ctx: &DatContext<'_>,
    filename: &str,
    file_size: u64,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<Option<Vec<FileMatch>>> {
    let named_roms = db::RomRecord::find_by_name(ctx.conn, ctx.dat_id, filename, true)?;
    let hash_roms = db::RomRecord::find_by_hash_in_dat(ctx.conn, ctx.dat_id, hash)?;
    Ok(resolve_match(file_size, hash, matched_sets, &named_roms, &hash_roms))
}

fn match_exact(
    file_size: u64,
    hash: &str,
    matched_sets: &BTreeSet<db::Id<db::SetRecord>>,
    named_roms: &[db::RomRecord],
) -> Option<Vec<FileMatch>> {
    let matches: Vec<_> = named_roms
        .iter()
        .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
        .filter(|rom| file_size == rom.size && hash == rom.hash)
        .map(|rom| FileMatch {
            status: db::MatchStatus::Match,
            set_id: rom.set_id,
            rom_id: rom.id,
        })
        .collect();
    if matches.is_empty() { None } else { Some(matches) }
}

fn match_names(matched_sets: &BTreeSet<db::Id<db::SetRecord>>, named_roms: &[db::RomRecord]) -> Option<Vec<FileMatch>> {
    let matches: Vec<_> = named_roms
        .iter()
        .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
        .map(|rom| FileMatch {
            status: db::MatchStatus::Name,
            set_id: rom.set_id,
            rom_id: rom.id,
        })
        .collect();
    if matches.is_empty() { None } else { Some(matches) }
}

fn match_hashes(matched_sets: &BTreeSet<db::Id<db::SetRecord>>, hash_roms: &[db::RomRecord]) -> Option<Vec<FileMatch>> {
    let matches: Vec<_> = hash_roms
        .iter()
        .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
        .map(|rom| FileMatch {
            status: db::MatchStatus::Hash,
            set_id: rom.set_id,
            rom_id: rom.id,
        })
        .collect();
    if matches.is_empty() { None } else { Some(matches) }
}

pub(crate) fn insert_files_and_matches(
    ctx: &DatContext<'_>,
    dir_id: &db::DirId,
    file_name: &str,
    file_size: u64,
    hash: Option<&str>,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<()> {
    let file = db::FileRecord::insert(
        ctx.conn,
        db::NewFile::new(ctx.dat_id, *dir_id, file_name, file_size, hash.unwrap_or("")),
    )?;

    if hash.is_some() {
        match_roms_and_insert(ctx, &file, matched_sets)?;
    }
    Ok(())
}

pub(crate) fn match_roms_and_insert(
    ctx: &DatContext<'_>,
    file: &db::FileRecord,
    matched_sets: &BTreeSet<db::Id<db::SetRecord>>,
) -> Result<()> {
    let matched = match_roms(ctx, &file.name, file.size, &file.hash, matched_sets)?;
    if let Some(items) = matched {
        for item in items {
            db::MatchRecord::insert(
                ctx.conn,
                db::NewMatch::new(ctx.dat_id, file.id, item.status, item.set_id, item.rom_id),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::{Insertable, QueryableByDat};

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

    // --- insert_files_and_matches ---

    #[test]
    fn insert_files_and_matches_allows_missing_hash_when_no_matched_sets() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/roms")).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        insert_files_and_matches(&ctx, &dir.id, "unknown.rom", 123, None, &BTreeSet::new()).unwrap();

        let files = dir.get_files(&conn).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].hash, "");

        let matches = db::MatchRecord::get_by_dat(&conn, dat.id).unwrap();
        assert!(matches.is_empty());
    }

    #[test]
    fn insert_files_and_matches_skips_matching_when_hash_missing() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "Matched Set")).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/roms")).unwrap();
        let ctx = DatContext::new(&conn, dat.id);
        let matched_sets = BTreeSet::from([set.id]);

        insert_files_and_matches(&ctx, &dir.id, "candidate.rom", 123, None, &matched_sets).unwrap();

        let files = dir.get_files(&conn).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "candidate.rom");
        assert_eq!(files[0].hash, "");
    }

    #[test]
    fn insert_files_and_matches_creates_match_when_hash_present() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "TestSet")).unwrap();
        let rom =
            db::RomRecord::insert(&conn, db::NewRom::new(dat.id, set.id, "test.rom", 100, "abc123", None)).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/roms")).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        insert_files_and_matches(&ctx, &dir.id, "test.rom", 100, Some("abc123"), &BTreeSet::new()).unwrap();

        let matches = db::MatchRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Match);
        assert_eq!(matches[0].rom_id, rom.id);
    }

    // --- match_roms_and_insert ---

    #[test]
    fn match_roms_and_insert_inserts_match_records() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "TestSet")).unwrap();
        let rom =
            db::RomRecord::insert(&conn, db::NewRom::new(dat.id, set.id, "game.rom", 256, "deadbeef", None)).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/roms")).unwrap();
        let file =
            db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "game.rom", 256, "deadbeef")).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        match_roms_and_insert(&ctx, &file, &BTreeSet::new()).unwrap();

        let matches = db::MatchRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Match);
        assert_eq!(matches[0].rom_id, rom.id);
        assert_eq!(matches[0].file_id, file.id);
    }

    #[test]
    fn match_roms_and_insert_no_match_inserts_nothing() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/roms")).unwrap();
        let file = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "unknown.rom", 999, "no_such_hash"))
            .unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        match_roms_and_insert(&ctx, &file, &BTreeSet::new()).unwrap();

        let matches = db::MatchRecord::get_by_dat(&conn, dat.id).unwrap();
        assert!(matches.is_empty());
    }

    // --- match_exact ---

    #[test]
    fn match_exact_empty_roms() {
        assert!(match_exact(100, "abc", &BTreeSet::new(), &[]).is_none());
    }

    #[test]
    fn match_exact_no_match_wrong_size() {
        let roms = [make_rom(1, 1, 1, 200, "abc")];
        assert!(match_exact(100, "abc", &BTreeSet::new(), &roms).is_none());
    }

    #[test]
    fn match_exact_no_match_wrong_hash() {
        let roms = [make_rom(1, 1, 1, 100, "xyz")];
        assert!(match_exact(100, "abc", &BTreeSet::new(), &roms).is_none());
    }

    #[test]
    fn match_exact_single_match() {
        let roms = [make_rom(1, 1, 1, 100, "abc")];
        let result = match_exact(100, "abc", &BTreeSet::new(), &roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Match);
        assert_eq!(result[0].set_id, 1i64.into());
        assert_eq!(result[0].rom_id, 1i64.into());
    }

    #[test]
    fn match_exact_multiple_matches() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 100, "abc")];
        let result = match_exact(100, "abc", &BTreeSet::new(), &roms).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn match_exact_set_filter_includes() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 100, "abc")];
        let filter = BTreeSet::from([1i64.into()]);
        let result = match_exact(100, "abc", &filter, &roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].set_id, 1i64.into());
    }

    #[test]
    fn match_exact_set_filter_excludes_all() {
        let roms = [make_rom(1, 1, 1, 100, "abc")];
        let filter = BTreeSet::from([99i64.into()]);
        assert!(match_exact(100, "abc", &filter, &roms).is_none());
    }

    // --- match_names ---

    #[test]
    fn match_names_empty_roms() {
        assert!(match_names(&BTreeSet::new(), &[]).is_none());
    }

    #[test]
    fn match_names_returns_all_unfiltered() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 200, "def")];
        let result = match_names(&BTreeSet::new(), &roms).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|m| m.status == db::MatchStatus::Name));
    }

    #[test]
    fn match_names_set_filter() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 200, "def")];
        let filter = BTreeSet::from([2i64.into()]);
        let result = match_names(&filter, &roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].set_id, 2i64.into());
    }

    #[test]
    fn match_names_set_filter_excludes_all() {
        let roms = [make_rom(1, 1, 1, 100, "abc")];
        let filter = BTreeSet::from([99i64.into()]);
        assert!(match_names(&filter, &roms).is_none());
    }

    // --- match_hashes ---

    #[test]
    fn match_hashes_empty_roms() {
        assert!(match_hashes(&BTreeSet::new(), &[]).is_none());
    }

    #[test]
    fn match_hashes_returns_all_unfiltered() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 200, "def")];
        let result = match_hashes(&BTreeSet::new(), &roms).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|m| m.status == db::MatchStatus::Hash));
    }

    #[test]
    fn match_hashes_set_filter() {
        let roms = [make_rom(1, 1, 1, 100, "abc"), make_rom(2, 1, 2, 200, "def")];
        let filter = BTreeSet::from([1i64.into()]);
        let result = match_hashes(&filter, &roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].set_id, 1i64.into());
    }

    #[test]
    fn match_hashes_set_filter_excludes_all() {
        let roms = [make_rom(1, 1, 1, 100, "abc")];
        let filter = BTreeSet::from([99i64.into()]);
        assert!(match_hashes(&filter, &roms).is_none());
    }

    // --- resolve_match ---

    #[test]
    fn resolve_match_exact_when_name_and_hash_match() {
        let named = [make_rom(1, 1, 1, 100, "abc")];
        let hash = [make_rom(1, 1, 1, 100, "abc")];
        let result = resolve_match(100, "abc", &BTreeSet::new(), &named, &hash).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Match);
    }

    #[test]
    fn resolve_match_hash_match_beats_name_only() {
        // name matches but hash is wrong; a different rom matches by hash — expect Hash status
        let named = [make_rom(1, 1, 1, 100, "xyz")];
        let hash_roms = [make_rom(2, 1, 2, 999, "abc")];
        let result = resolve_match(100, "abc", &BTreeSet::new(), &named, &hash_roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Hash);
        assert_eq!(result[0].rom_id, 2i64.into());
    }

    #[test]
    fn resolve_match_name_only_when_no_hash_match() {
        // name matches but hash is wrong, and no hash-only match exists — expect Name status
        let named = [make_rom(1, 1, 1, 100, "xyz")];
        let result = resolve_match(100, "abc", &BTreeSet::new(), &named, &[]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Name);
    }

    #[test]
    fn resolve_match_none_when_no_named_and_no_hash() {
        assert!(resolve_match(100, "abc", &BTreeSet::new(), &[], &[]).is_none());
    }

    #[test]
    fn resolve_match_none_when_no_named_and_hash_filtered_out() {
        let hash_roms = [make_rom(2, 1, 2, 999, "abc")];
        let filter = BTreeSet::from([99i64.into()]);
        assert!(resolve_match(100, "abc", &filter, &[], &hash_roms).is_none());
    }

    #[test]
    fn resolve_match_hash_only_no_named_roms() {
        // no name match at all, but hash matches — expect Hash status
        let hash_roms = [make_rom(3, 1, 3, 100, "abc")];
        let result = resolve_match(100, "abc", &BTreeSet::new(), &[], &hash_roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Hash);
    }

    #[test]
    fn resolve_match_exact_preferred_over_hash_only() {
        // named rom is an exact match; hash_roms also present — exact should win
        let named = [make_rom(1, 1, 1, 100, "abc")];
        let hash_roms = [make_rom(2, 1, 2, 100, "abc")];
        let result = resolve_match(100, "abc", &BTreeSet::new(), &named, &hash_roms).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].status, db::MatchStatus::Match);
        assert_eq!(result[0].rom_id, 1i64.into());
    }

    // --- match_sets ---

    #[test]
    fn match_sets_returns_matching_set_ids() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "MyGame")).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        let matched = match_sets(&ctx, "/roms/MyGame.zip").unwrap();
        assert_eq!(matched.len(), 1);
        assert!(matched.contains(&set.id));
    }

    #[test]
    fn match_sets_returns_empty_when_no_match() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        let matched = match_sets(&ctx, "/roms/NoSuchGame.zip").unwrap();
        assert!(matched.is_empty());
    }

    // --- match_roms (integration through match_roms_and_insert) ---

    #[test]
    fn match_roms_exact_match_by_name_size_and_hash() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "TestSet")).unwrap();
        let _rom =
            db::RomRecord::insert(&conn, db::NewRom::new(dat.id, set.id, "game.rom", 512, "aabbccdd", None)).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        let result = match_roms(&ctx, "game.rom", 512, "aabbccdd", &BTreeSet::new()).unwrap();
        assert!(result.is_some());
        let matches = result.unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Match);
    }

    #[test]
    fn match_roms_hash_only_match() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "TestSet")).unwrap();
        let _rom = db::RomRecord::insert(&conn, db::NewRom::new(dat.id, set.id, "original.rom", 512, "aabbccdd", None))
            .unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        // Different filename, same hash
        let result = match_roms(&ctx, "renamed.rom", 512, "aabbccdd", &BTreeSet::new()).unwrap();
        assert!(result.is_some());
        let matches = result.unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Hash);
    }

    #[test]
    fn match_roms_no_match() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let ctx = DatContext::new(&conn, dat.id);

        let result = match_roms(&ctx, "unknown.rom", 999, "no_match", &BTreeSet::new()).unwrap();
        assert!(result.is_none());
    }
}
