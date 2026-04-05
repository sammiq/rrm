use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, Write};

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use fallible_iterator::FallibleIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rusqlite::Transaction;

use crate::cli::TermInfo;
use crate::db::{self, Deletable, DeletableByDat, Insertable};
use crate::matching::{ScannedFile, insert_files_and_matches, match_sets};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ZipEntryMeta {
    pub index: usize,
    pub name: String,
    pub size: u64,
    pub crc: String,
    pub hash: Option<String>,
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
                            Ok((hash, file_size)) => Some(ScannedFile {
                                name: filename.to_string(),
                                size: file_size,
                                hash: Some(hash),
                            }),
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
    for scanned_file in &hashed_files {
        match insert_files_and_matches(tx, dat_id, dir.id, scanned_file, &matched_sets) {
            Ok(_) => file_count += 1,
            Err(e) => eprintln!("Failed to insert {}. Error: {e}", scanned_file.name),
        }
        progress_fn(file_count);
    }

    // Insert hashed zip entries, each zip in its own savepoint
    for (path, entries) in hashed_zips {
        match db::with_savepoint(tx, |sp| {
            let zip_dir = db::DirRecord::insert(sp, db::NewDir::new(dat_id, path.as_str()))?;
            let matched = match_sets(sp, dat_id, path)?;
            let entry_count = entries.len() as u64;
            for entry in entries {
                let scanned_file = ScannedFile {
                    name: entry.name,
                    size: entry.size,
                    hash: entry.hash,
                };
                insert_files_and_matches(sp, dat_id, zip_dir.id, &scanned_file, &matched)?;
            }
            Ok(entry_count)
        }) {
            Ok(count) => {
                file_count += count;
                progress_fn(file_count);
            }
            Err(e) => eprintln!("Failed to scan {}. Error: {e}", path),
        }
    }

    if incremental {
        remove_stale_entries(tx, dat_id, subdirs_by_path, files_by_name);
    }

    Ok(file_count)
}

/// Removes DB records for directories and files that were present in a previous scan
/// but were not encountered in the current one. Directories that still exist on disk are
/// left alone — the user may have simply omitted the `--recursive` flag.
fn remove_stale_entries<'a>(
    conn: &rusqlite::Connection,
    dat_id: db::DatId,
    stale_subdirs: BTreeSet<&'a str>,
    stale_files: BTreeMap<&'a str, &'a db::FileRecord>,
) {
    for existing_path in stale_subdirs {
        if Utf8Path::new(existing_path).is_dir() {
            // Directory still exists on disk — skip so we don't lose data when the user
            // forgets the --recursive flag.
            continue;
        }
        match db::DirRecord::find_by_path_in_dat(conn, dat_id, existing_path) {
            Ok(Some(dir)) => {
                dir.delete_matches(conn)
                    .and_then(|_| dir.delete_files(conn))
                    .and_then(|_| db::DirRecord::delete_by_id(conn, dir.id))
                    .if_err(|e| eprintln!("Failed to delete directory {}. Error: {e}", existing_path));
            }
            Ok(None) => eprintln!("Failed to find directory entry {}.", existing_path),
            Err(e) => eprintln!("Failed to get directory entry {}. Error: {e}", existing_path),
        }
    }
    for (_, existing_file) in stale_files {
        existing_file
            .delete_matches(conn)
            .and_then(|_| db::FileRecord::delete_by_id(conn, existing_file.id))
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
