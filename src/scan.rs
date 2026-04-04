use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, Write};

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use fallible_iterator::FallibleIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rusqlite::{Connection, Transaction};

use crate::cli::TermInfo;
use crate::db::{self, Deletable, DeletableByDat, FindableByName, Insertable};
use crate::util::{self, ResultIf};

const ANSI_CURSOR_START: &str = "\x1B[1000D";
const ANSI_ERASE_TO_END: &str = "\x1B[K";

/// Options that are fixed for the duration of a scan and passed through every recursive call.
pub(crate) struct ScanOptions<'a> {
    pub exclude: &'a [String],
    pub rom_crcs: &'a BTreeSet<String>,
    pub recursive: bool,
    pub full_scan: bool,
    pub pool: &'a rayon::ThreadPool,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ZipEntryMeta {
    pub index: usize,
    pub name: String,
    pub size: u64,
    pub crc: String,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct FileMatch {
    pub status: db::MatchStatus,
    pub set_id: db::SetId,
    pub rom_id: db::RomId,
}

pub(crate) fn scan_files(
    tx: &mut Transaction,
    dat_id: db::DatId,
    term: &TermInfo,
    scan_path: &Utf8Path, //expect this to be canonicalized
    options: &ScanOptions<'_>,
) -> Result<()> {
    if options.full_scan {
        //delete all records associated with the files
        db::MatchRecord::delete_by_dat(tx, dat_id)?;
        db::FileRecord::delete_by_dat(tx, dat_id)?;
        db::DirRecord::delete_by_dat(tx, dat_id)?;
    }

    let file_count = scan_directory(tx, dat_id, scan_path, options, &|count| {
        report_progress(term, count);
    })?;

    if term.tty_out {
        println!("{ANSI_CURSOR_START}{} new files scanned.{ANSI_ERASE_TO_END}", file_count);
    } else {
        println!("{} new files scanned.", file_count);
    }
    Ok(())
}

fn report_progress(term: &TermInfo, count: u64) {
    if term.tty_out {
        print!("{ANSI_CURSOR_START}{count} new files scanned.{ANSI_ERASE_TO_END}");
        std::io::stdout().flush().ok();
    }
}

fn scan_directory(
    tx: &mut Transaction,
    dat_id: db::DatId,
    scan_path: &Utf8Path,
    options: &ScanOptions<'_>,
    progress_fn: &dyn Fn(u64),
) -> Result<u64> {
    let (dir, incremental) = match db::DirRecord::find_by_path_in_dat(tx, dat_id, scan_path.as_str())? {
        Some(dir) => (dir, true),
        None => {
            //no existing records, do a full scan
            let dir = db::DirRecord::insert(tx, db::NewDir::new(dat_id, scan_path.as_str()))?;
            (dir, false)
        }
    };

    //these will be empty if not incremental, but its cheap enough to call them that its not worth optimising them out
    let existing_subdirs = dir.get_children(tx)?;
    let mut subdirs_by_path: BTreeSet<_> = existing_subdirs.iter().map(|dir| dir.path.as_str()).collect();

    let existing_files = dir.get_files(tx)?;
    let mut files_by_name: BTreeMap<_, _> = existing_files.iter().map(|file| (file.name.as_str(), file)).collect();

    let mut file_count = 0u64;
    let mut files_to_hash: Vec<(Utf8PathBuf, String)> = Vec::new();
    let mut zips_to_hash: Vec<Utf8PathBuf> = Vec::new();

    let mut iter = fallible_iterator::convert(scan_path.read_dir_utf8()?);
    while let Some(entry) = iter.next()? {
        let path = entry.path();
        if util::is_hidden_file(path) {
            continue;
        }

        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if !options.recursive {
                continue;
            }
            subdirs_by_path.remove(path.as_str());
            let subdir_count = scan_directory(tx, dat_id, path, options, &|count| progress_fn(file_count + count))?;
            file_count += subdir_count;
        } else if file_type.is_file() {
            if util::has_extension(path, options.exclude) {
                continue;
            }

            if util::is_zip_file(path) {
                let was_scanned = subdirs_by_path.remove(path.as_str());
                if !was_scanned {
                    zips_to_hash.push(path.to_path_buf());
                }
            } else if let Some(filename) = path.file_name() {
                let exists = files_by_name.remove(filename).is_some();
                if exists && incremental {
                    //there was an existing scanned file, so skip it
                    continue;
                }
                files_to_hash.push((path.to_path_buf(), filename.to_string()));
            }
        }
        progress_fn(file_count);
    }

    // Hash loose files and zip entries in parallel, then insert results serially
    let exclude = options.exclude;
    let (hashed_files, hashed_zips) = options.pool.install(|| {
        rayon::join(
            || {
                files_to_hash
                    .par_iter()
                    .filter_map(|(path, filename)| {
                        let result = File::open(path).map_err(anyhow::Error::from).and_then(|file| {
                            let mut reader = BufReader::new(file);
                            util::calc_hash(&mut reader)
                        });
                        match result {
                            Ok((hash, file_size)) => Some((filename.as_str(), hash, file_size)),
                            Err(e) => {
                                eprintln!("Failed to scan {}. Error: {e}", path);
                                None
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            },
            || {
                let rom_crcs = options.rom_crcs;
                zips_to_hash
                    .par_iter()
                    .filter_map(|path| {
                        let file = match File::open(path) {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("Failed to open {}. Error: {e}", path);
                                return None;
                            }
                        };
                        let mut zip = match zip::ZipArchive::new(file) {
                            Ok(z) => z,
                            Err(e) => {
                                eprintln!("Failed to scan {}. Error: {e}", path);
                                return None;
                            }
                        };
                        let mut entries = match read_zip_entries(&mut zip, exclude) {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!("Failed to scan {}. Error: {e}", path);
                                return None;
                            }
                        };
                        for entry in &mut entries {
                            if rom_crcs.is_empty() || rom_crcs.contains(&entry.crc) {
                                match zip.by_index(entry.index) {
                                    Ok(mut inner) => match util::calc_hash(&mut inner) {
                                        Ok((hash, _)) => entry.hash = Some(hash),
                                        Err(e) => {
                                            eprintln!("Failed to hash {} in {}. Error: {e}", entry.name, path);
                                        }
                                    },
                                    Err(e) => {
                                        eprintln!("Failed to read entry {} in {}. Error: {e}", entry.index, path);
                                    }
                                }
                            }
                        }
                        Some((path, entries))
                    })
                    .collect::<Vec<_>>()
            },
        )
    });

    // Insert hashed loose files
    let matched_sets = BTreeSet::new();
    for (filename, hash, file_size) in &hashed_files {
        match insert_files_and_matches(
            &DatContext::new(tx, dat_id),
            &dir.id,
            filename,
            *file_size,
            Some(hash),
            &matched_sets,
        ) {
            Ok(_) => file_count += 1,
            Err(e) => eprintln!("Failed to insert {}. Error: {e}", filename),
        }
        progress_fn(file_count);
    }

    // Insert hashed zip entries, each zip in its own savepoint
    for (path, entries) in hashed_zips {
        match db::with_savepoint(tx, |sp| {
            let ctx = DatContext::new(sp, dat_id);
            let zip_dir = db::DirRecord::insert(ctx.conn, db::NewDir::new(dat_id, path.as_str()))?;
            let matched = match_sets(&ctx, path)?;
            for entry in &entries {
                insert_files_and_matches(&ctx, &zip_dir.id, &entry.name, entry.size, entry.hash.as_deref(), &matched)?;
            }
            Ok(entries.len() as u64)
        }) {
            Ok(count) => {
                file_count += count;
                progress_fn(file_count);
            }
            Err(e) => eprintln!("Failed to scan {}. Error: {e}", path),
        }
    }

    if incremental {
        remove_stale_entries(&DatContext::new(tx, dat_id), subdirs_by_path, files_by_name);
    }

    Ok(file_count)
}

/// Removes DB records for directories and files that were present in a previous scan
/// but were not encountered in the current one. Directories that still exist on disk are
/// left alone — the user may have simply omitted the `--recursive` flag.
fn remove_stale_entries<'a>(
    ctx: &DatContext<'_>,
    stale_subdirs: BTreeSet<&'a str>,
    stale_files: BTreeMap<&'a str, &'a db::FileRecord>,
) {
    for existing_path in stale_subdirs {
        if Utf8Path::new(existing_path).is_dir() {
            // Directory still exists on disk — skip so we don't lose data when the user
            // forgets the --recursive flag.
            continue;
        }
        match db::DirRecord::find_by_path_in_dat(ctx.conn, ctx.dat_id, existing_path) {
            Ok(Some(dir)) => {
                dir.delete_matches(ctx.conn)
                    .and_then(|_| dir.delete_files(ctx.conn))
                    .and_then(|_| db::DirRecord::delete_by_id(ctx.conn, dir.id))
                    .if_err(|e| eprintln!("Failed to delete directory {}. Error: {e}", existing_path));
            }
            Ok(None) => eprintln!("Failed to find directory entry {}.", existing_path),
            Err(e) => eprintln!("Failed to get directory entry {}. Error: {e}", existing_path),
        }
    }
    for (_, existing_file) in stale_files {
        existing_file
            .delete_matches(ctx.conn)
            .and_then(|_| db::FileRecord::delete_by_id(ctx.conn, existing_file.id))
            .if_err(|e| eprintln!("Failed to remove {}. Error: {e}", existing_file.name));
    }
}

/// Read zip entry metadata without hashing file contents.
pub(crate) fn read_zip_entries(zip: &mut zip::ZipArchive<File>, exclude: &[String]) -> Result<Vec<ZipEntryMeta>> {
    let mut entries = Vec::new();
    for i in 0..zip.len() {
        let inner_file = zip.by_index(i)?;
        if !inner_file.is_file() {
            continue;
        }
        if util::has_extension(inner_file.name(), exclude) {
            continue;
        }
        entries.push(ZipEntryMeta {
            index: i,
            name: inner_file.name().to_string(),
            size: inner_file.size(),
            crc: format!("{:08x}", inner_file.crc32()),
            hash: None,
        });
    }
    Ok(entries)
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

fn insert_files_and_matches(
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

    // --- read_zip_entries ---

    fn create_test_zip(entries: &[(&str, &[u8])]) -> tempfile::NamedTempFile {
        use zip::write::SimpleFileOptions;
        let tmp = tempfile::Builder::new().suffix(".zip").tempfile().unwrap();
        let mut writer = zip::ZipWriter::new(tmp.as_file());
        for (name, content) in entries {
            writer.start_file(*name, SimpleFileOptions::default()).unwrap();
            std::io::Write::write_all(&mut writer, content).unwrap();
        }
        writer.finish().unwrap();
        tmp
    }

    fn open_test_zip(tmp: &tempfile::NamedTempFile) -> zip::ZipArchive<File> {
        let file = File::open(tmp.path()).unwrap();
        zip::ZipArchive::new(file).unwrap()
    }

    #[test]
    fn read_zip_entries_returns_all_files() {
        let tmp = create_test_zip(&[("a.rom", b"hello"), ("b.rom", b"world")]);
        let mut zip = open_test_zip(&tmp);
        let entries = read_zip_entries(&mut zip, &[]).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "a.rom");
        assert_eq!(entries[1].name, "b.rom");
        assert!(entries[0].size > 0);
        assert!(entries[1].size > 0);
    }

    #[test]
    fn read_zip_entries_excludes_extensions() {
        let tmp = create_test_zip(&[("a.rom", b"data"), ("readme.txt", b"info")]);
        let mut zip = open_test_zip(&tmp);
        let exclude = vec!["txt".to_string()];
        let entries = read_zip_entries(&mut zip, &exclude).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "a.rom");
    }

    #[test]
    fn read_zip_entries_produces_consistent_crc_and_size() {
        let content = b"deterministic content";
        let tmp1 = create_test_zip(&[("file.rom", content)]);
        let tmp2 = create_test_zip(&[("file.rom", content)]);
        let mut zip1 = open_test_zip(&tmp1);
        let mut zip2 = open_test_zip(&tmp2);
        let entries1 = read_zip_entries(&mut zip1, &[]).unwrap();
        let entries2 = read_zip_entries(&mut zip2, &[]).unwrap();
        assert_eq!(entries1[0].crc, entries2[0].crc);
        assert_eq!(entries1[0].size, entries2[0].size);
    }

    #[test]
    fn read_zip_entries_empty_zip() {
        let tmp = create_test_zip(&[]);
        let mut zip = open_test_zip(&tmp);
        let entries = read_zip_entries(&mut zip, &[]).unwrap();
        assert!(entries.is_empty());
    }
}
