#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod cli;
mod completion;
mod dat;
mod db;
mod display;
mod matching;
mod scan;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{IsTerminal, Write};

use anyhow::{Context, Result, anyhow, bail, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{CommandFactory, Parser};
use rusqlite::{Connection, Transaction};
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};

use crate::cli::{Args, Cli, Commands, CompletionHelper, DataCommands, FileCommands, SelectMode, TermInfo};
use crate::completion::build_completions;
use crate::db::{Deletable, DeletableByDat, Insertable, Queryable, QueryableByDat};
use crate::display::{ListContext, format_file_indicator};
use crate::display::{
    list_dat_files, list_dat_records, list_found_sets, list_missing_sets, list_roms, list_scanned_files, list_sets,
};
use crate::matching::{match_roms_and_insert, match_sets};
use crate::scan::{ScanOptions, read_zip_entries, scan_files};
use crate::util::ResultIf;

const APP_NAME: &str = "rrm";

fn main() -> Result<()> {
    let data_path = util::data_dir()
        .context("could not resolve data directory for platform")?
        .join(APP_NAME);
    std::fs::create_dir_all(&data_path)?;
    let db_path = data_path.join("rrm.db");

    if db_path.exists() {
        let bak = data_path.join("rrm.bak");
        std::fs::copy(&db_path, &bak)?;
    }
    let mut conn = db::open_or_create(&db_path)?;
    let mut dat_id = None;

    let args = Args::parse_from(wild::args_os());

    let term = TermInfo {
        tty_in: std::io::stdin().is_terminal(),
        tty_out: std::io::stdout().is_terminal(),
        interactive: args.command.is_none() || args.interactive,
    };

    if let Some(index) = args.select {
        select_dat(&conn, &mut dat_id, index)?;
    } else {
        dat_id = select_dat_from_path(&conn);
    }

    if let Some(command) = args.command {
        do_command(&mut conn, &mut dat_id, &term, &command)?;
    }

    if term.interactive && term.tty_in {
        run_repl(&mut conn, &mut dat_id, &term)?;
    }
    Ok(())
}

fn run_repl(conn: &mut Connection, dat_id: &mut Option<db::DatId>, term: &TermInfo) -> Result<()> {
    let command = Cli::command();
    let base_node = build_completions(&command);

    let helper = CompletionHelper { node: base_node };
    let mut rl = Editor::with_config(
        Config::builder()
            .completion_type(rustyline::CompletionType::List)
            .build(),
    )?;
    rl.set_helper(Some(helper));
    loop {
        match rl.readline(">> ") {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line)?;

                if let Some(args) = shlex::split(line) {
                    if args.is_empty() {
                        continue;
                    }
                    //if the first argument is exit or quit, exit the REPL
                    if args[0].eq_ignore_ascii_case("exit") || args[0].eq_ignore_ascii_case("quit") {
                        break;
                    }
                    match Cli::try_parse_from(args) {
                        Ok(cli) => do_command(conn, dat_id, term, &cli.command)
                            .if_err(|e| eprintln!("Error: Unable to perform command, {e}")),
                        Err(e) => e.print()?,
                    }
                } else {
                    eprintln!("Error: Invalid quoting");
                };
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(err) => eprintln!("Error: {}", err),
        }
    }
    Ok(())
}

fn select_dat_from_path(conn: &Connection) -> Option<db::DatId> {
    // as this method is best effort, don't bother complaining loudly about errors.
    let current_path = std::env::current_dir().ok().and_then(util::canonical_path)?;
    let paths = db::DirRecord::get_by_path(conn, current_path.as_str()).ok()?;

    if paths.is_empty() {
        // Only warn if dats are actually installed; silence the message on a fresh install.
        if db::DatRecord::get_all(conn).is_ok_and(|dats| !dats.is_empty()) {
            eprintln!("No default dat file for current path.");
        }
        return None;
    }

    if paths.len() > 1 {
        eprintln!("Warning: current path matches {} dat files, selecting the first.", paths.len());
    }

    db::DatRecord::get_by_id(conn, paths[0].dat_id).ok().map(|dat| {
        println!("dat file `{}` selected.", dat.name);
        dat.id
    })
}

fn do_command(
    conn: &mut Connection,
    dat_id: &mut Option<db::DatId>,
    term: &TermInfo,
    command: &Commands,
) -> Result<()> {
    match command {
        Commands::Data { data } => handle_data_commands(conn, dat_id, term, data),
        Commands::Files { files } => handle_file_commands(conn, *dat_id, term, files),
        Commands::Select { index } => select_dat(conn, dat_id, *index),
    }
}

fn handle_data_commands(
    conn: &mut Connection,
    dat_id: &mut Option<db::DatId>,
    term: &TermInfo,
    data: &DataCommands,
) -> Result<()> {
    match data {
        DataCommands::Import { dat_file } => {
            ensure!(dat_file.is_file(), "`{}` is not a valid file", dat_file);

            db::with_transaction(conn, |tx| {
                dat::import_dat(tx, dat_file).map(|imported| {
                    if term.interactive {
                        println!("dat file `{}` imported and selected.", imported.name);
                        *dat_id = Some(imported.id);
                    } else {
                        println!("dat file `{}` imported.", imported.name);
                    }
                })
            })
        }
        DataCommands::Update { dat_file, yes } => {
            if ask_for_confirmation(term, "Are you sure you want to update the current dat file? (y/N): ", *yes)? {
                let old_dat_id = dat_id
                    .as_ref()
                    .copied()
                    .ok_or_else(|| anyhow!("No dat file selected"))?;
                db::with_transaction(conn, |tx| {
                    update_dat(tx, dat_file, old_dat_id).map(|imported| {
                        println!("dat file `{}` imported and updated.", imported.name);
                        *dat_id = Some(imported.id);
                    })
                })?;
            }
            Ok(())
        }
        DataCommands::Remove { yes, index } => {
            let (old_dat_id, prompt) = if let Some(index) = index {
                let dat = get_dat_by_index(conn, *index)?;
                (dat.id, format!("dat file `{}`", dat.name))
            } else if let Some(select_id) = *dat_id {
                (select_id, "the current dat file".to_string())
            } else {
                bail!("No dat file selected");
            };

            if ask_for_confirmation(term, &format!("Are you sure you want to remove {}? (y/N): ", prompt), *yes)? {
                db::with_transaction(conn, |tx| {
                    dat::delete_dat(tx, old_dat_id).map(|_| {
                        println!("dat file removed.");
                        *dat_id = None;
                    })
                })?;
            }
            Ok(())
        }
        DataCommands::List => list_dat_files(conn),
        DataCommands::Select { index } => select_dat(conn, dat_id, *index),
        DataCommands::Records => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_dat_records(conn, *dat_id)
        }
        DataCommands::Sets { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_sets(conn, *dat_id, partial_name.as_deref())
        }
        DataCommands::Roms { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_roms(conn, *dat_id, partial_name.as_deref())
        }
    }
}

fn get_dat_by_index(conn: &Connection, index: usize) -> Result<db::DatRecord> {
    db::DatRecord::get_all(conn).and_then(|mut dats| {
        if index < dats.len() {
            let dat = dats.swap_remove(index);
            Ok(dat)
        } else {
            bail!("Invalid dat file selection.")
        }
    })
}

fn select_dat(conn: &Connection, dat_id: &mut Option<db::DatId>, index: usize) -> Result<()> {
    get_dat_by_index(conn, index).map(|dat| {
        println!("dat file `{}` selected.", dat.name);
        *dat_id = Some(dat.id);
    })
}

fn ask_for_confirmation(term: &TermInfo, prompt: &str, force: bool) -> Result<bool> {
    if !force {
        if term.tty_in {
            print!("{prompt}");
            std::io::stdout().flush()?;
            let mut buffer = String::new();
            std::io::stdin().read_line(&mut buffer)?;
            let buffer = buffer.trim();
            return Ok(buffer.eq_ignore_ascii_case("y"));
        } else {
            eprintln!("Cannot execute command without confirmation (pass `--yes` to override)")
        }
    }
    Ok(force)
}

fn handle_file_commands(
    conn: &mut Connection,
    dat_id: Option<db::DatId>,
    term: &TermInfo,
    files: &FileCommands,
) -> Result<()> {
    let dat_id = dat_id.ok_or_else(|| anyhow!("No dat file selected"))?;

    match files {
        FileCommands::Scan {
            exclude,
            recursive,
            full,
            parallel,
            path,
        } => {
            //make sure path is resolved to something absolute and proper before scanning
            let scan_path = path.canonicalize_utf8()?;
            ensure!(scan_path.is_dir(), "`{}` is not a valid directory", scan_path);

            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(*parallel)
                .build()
                .context("Failed to create thread pool")?;
            let rom_crcs = db::RomRecord::get_crcs_by_dat(conn, dat_id)?;

            let options = ScanOptions {
                exclude,
                rom_crcs: &rom_crcs,
                recursive: *recursive,
                full_scan: *full,
                pool: &pool,
            };

            db::with_transaction_mut(conn, |tx| scan_files(tx, dat_id, term, &scan_path, &options))
        }
        FileCommands::List { mode, partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            list_scanned_files(&ctx, *mode)
        }
        FileCommands::Sets { missing, partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            if *missing { list_missing_sets(&ctx) } else { list_found_sets(&ctx) }
        }
        FileCommands::Sort { mode, path, keep } => {
            db::with_transaction_mut(conn, |tx| sort_files(tx, dat_id, *mode, path, *keep))
        }
        FileCommands::Rename => db::with_transaction_mut(conn, |tx| rename_files(tx, dat_id, term)),
        FileCommands::Matched { partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            list_scanned_files(&ctx, SelectMode::Matched)
        }
        FileCommands::Missing { partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            list_missing_sets(&ctx)
        }
        FileCommands::Unmatched { partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            list_scanned_files(&ctx, SelectMode::Unmatched)
        }
        FileCommands::Warning { partial_name } => {
            let ctx = ListContext::new(conn, dat_id, term, partial_name.as_deref());
            list_scanned_files(&ctx, SelectMode::Warning)
        }
    }
}

/// Classify a file into a SelectMode based on its match records.
fn classify_file(file: &db::FileRecord, file_modes: &BTreeMap<db::FileId, SelectMode>) -> SelectMode {
    file_modes
        .get(&file.id)
        .copied()
        .unwrap_or(SelectMode::Unmatched)
}

/// Classify a zip directory based on the statuses of all files inside it.
/// Returns Unmatched if no files have any matches, Matched if all files are Match, Warning otherwise.
fn classify_zip(files: &[db::FileRecord], file_modes: &BTreeMap<db::FileId, SelectMode>) -> SelectMode {
    if files.is_empty() {
        return SelectMode::Unmatched;
    }
    let mut all_matched = true;
    let mut any_matched = false;
    for file in files {
        match classify_file(file, file_modes) {
            SelectMode::Matched => any_matched = true,
            SelectMode::Unmatched => all_matched = false,
            _ => {
                any_matched = true;
                all_matched = false;
            }
        }
    }
    if !any_matched {
        SelectMode::Unmatched
    } else if all_matched {
        SelectMode::Matched
    } else {
        SelectMode::Warning
    }
}

/// Build per-file sort mode in one pass over match records for the dat.
fn get_file_modes(conn: &Connection, dat_id: db::DatId) -> Result<BTreeMap<db::FileId, SelectMode>> {
    let mut file_modes = BTreeMap::new();
    for matched in db::MatchRecord::get_by_dat(conn, dat_id)? {
        let mode = file_modes.entry(matched.file_id).or_insert(SelectMode::Warning);
        if matched.status == db::MatchStatus::Match {
            *mode = SelectMode::Matched;
        }
    }
    Ok(file_modes)
}

fn subdir_name(mode: SelectMode) -> Option<&'static str> {
    match mode {
        SelectMode::Matched => Some("matched"),
        SelectMode::Warning => Some("warning"),
        SelectMode::Unmatched => Some("unmatched"),
        SelectMode::All => None,
    }
}

/// Get or create a DirRecord for the given path, caching in the provided map.
fn get_or_create_dest_dir(
    conn: &Connection,
    dat_id: db::DatId,
    dest_dirs: &mut BTreeMap<String, db::DirId>,
    dest_path: &str,
) -> Result<db::DirId> {
    if let Some(id) = dest_dirs.get(dest_path) {
        return Ok(*id);
    }
    let dir = if let Some(existing) = db::DirRecord::find_by_path_in_dat(conn, dat_id, dest_path)? {
        existing
    } else {
        db::DirRecord::insert(conn, db::NewDir::new(dat_id, dest_path))?
    };
    dest_dirs.insert(dest_path.to_string(), dir.id);
    Ok(dir.id)
}

fn sort_files(tx: &mut Transaction, dat_id: db::DatId, mode: SelectMode, path: &Utf8Path, keep: bool) -> Result<()> {
    // Determine which modes we need subdirectories for
    let modes: Vec<SelectMode> = match mode {
        SelectMode::All => vec![SelectMode::Matched, SelectMode::Warning, SelectMode::Unmatched],
        other => vec![other],
    };

    // Create destination subdirectories on disk
    for m in &modes {
        let mode_subdir = subdir_name(*m).ok_or_else(|| anyhow!("cannot sort into subdirectory for mode {:?}", m))?;
        let dest = path.join(mode_subdir);
        if !dest.exists() {
            std::fs::create_dir_all(&dest)?;
        }
    }

    // Cache of destination DirRecords (path -> DirId) for keep mode
    let mut dest_dirs: BTreeMap<String, db::DirId> = BTreeMap::new();
    let file_modes = get_file_modes(tx, dat_id)?;

    for (dir, files) in db::DirRecord::get_by_dat_with_files(tx, dat_id)? {
        if util::is_zip_file(&dir.path) {
            // Classify the zip as a whole
            let zip_mode = classify_zip(&files, &file_modes);
            if mode != SelectMode::All && mode != zip_mode {
                continue;
            }

            let zip_path = Utf8PathBuf::from(&dir.path);
            let file_name = zip_path.file_name().context("zip should have a file name")?;
            let mode_subdir = subdir_name(zip_mode)
                .ok_or_else(|| anyhow!("cannot sort zip '{}' with unsupported mode {:?}", dir.path, zip_mode))?;
            let dest = path.join(mode_subdir).join(file_name);

            match db::with_savepoint(tx, |sp| {
                std::fs::rename(&zip_path, &dest)?;
                if keep {
                    // Update the dir record path to the new location
                    dir.update_path(sp, dest.as_str())?;
                } else {
                    // Delete matches, files, and the dir record
                    dir.delete_matches(sp)?;
                    dir.delete_files(sp)?;
                    db::DirRecord::delete_by_id(sp, dir.id)?;
                }
                Ok(())
            }) {
                Ok(()) => println!("[zip] {} -> {}", dir.path, dest),
                Err(e) => eprintln!("Failed to sort zip {}. Error was {e}", dir.path),
            }
        } else {
            // Loose files: handle each file individually
            for file in &files {
                let file_mode = classify_file(file, &file_modes);
                if mode != SelectMode::All && mode != file_mode {
                    continue;
                }

                let src = Utf8Path::new(&dir.path).join(&file.name);
                let mode_subdir = subdir_name(file_mode)
                    .ok_or_else(|| anyhow!("cannot sort file '{}' with unsupported mode {:?}", file.name, file_mode))?;
                let dest = path.join(mode_subdir).join(&file.name);

                match db::with_savepoint(tx, |sp| {
                    std::fs::rename(&src, &dest)?;
                    if keep {
                        let dest_path_str = path.join(mode_subdir).as_str().to_string();
                        let dest_dir_id = get_or_create_dest_dir(sp, dat_id, &mut dest_dirs, &dest_path_str)?;
                        file.update_dir_id(sp, dest_dir_id)?;
                    } else {
                        file.delete_matches(sp)?;
                        db::FileRecord::delete_by_id(sp, file.id)?;
                    }
                    Ok(())
                }) {
                    Ok(()) => println!("{} -> {}", src, dest),
                    Err(e) => eprintln!("Failed to sort {}. Error was {e}", file.name),
                }
            }
        }
    }

    Ok(())
}

fn update_dat(conn: &Connection, dat_file: &Utf8PathBuf, old_dat_id: db::DatId) -> Result<db::DatRecord> {
    let imported = dat::import_dat(conn, dat_file)?;
    let new_rom_crcs = db::RomRecord::get_crcs_by_dat(conn, imported.id)?;

    //delete all existing matches for the old dat, we'll re-match them as we relink directories and files to the new dat
    db::MatchRecord::delete_by_dat(conn, old_dat_id)?;

    for (directory, files) in db::DirRecord::get_by_dat_with_files(conn, old_dat_id)? {
        if util::is_zip_file(&directory.path) {
            let matched_sets = match_sets(conn, imported.id, &directory.path)?;
            let zip_file = File::open(&directory.path)?;
            let mut zip = zip::ZipArchive::new(zip_file)
                .with_context(|| format!("could not open '{}' as a zip file", directory.path))?;
            let zip_entries: BTreeMap<_, _> = read_zip_entries(&mut zip, &[])?
                .into_iter()
                .map(|entry| (entry.name.clone(), entry))
                .collect();
            for file in files {
                // Zip entries may have been left unhashed during the original scan,
                // so recover the hash only when the new DAT makes it relevant.
                let file = ensure_hash_for_update(conn, &mut zip, &directory.path, &zip_entries, &file, &new_rom_crcs)?;
                match_roms_and_insert(conn, imported.id, &file, &matched_sets)?;
            }
        } else {
            let matched_sets = BTreeSet::new();
            for file in files {
                // Loose files are always hashed during scan, so no recovery path is needed.
                match_roms_and_insert(conn, imported.id, &file, &matched_sets)?;
            }
        }
    }

    //relink all directories to the new dat
    db::DirRecord::relink_dirs(conn, old_dat_id, imported.id)?;

    //relink all files to the new dat
    db::FileRecord::relink_files(conn, old_dat_id, imported.id)?;

    //if we successfully updated everything and relinked and the transaction completed, we can now delete the old dat
    dat::delete_dat(conn, old_dat_id)?;

    Ok(imported)
}

fn ensure_hash_for_update(
    conn: &Connection,
    zip: &mut zip::ZipArchive<File>,
    zip_path: &str,
    zip_entries: &BTreeMap<String, scan::ZipEntryMeta>,
    file: &db::FileRecord,
    rom_crcs: &BTreeSet<String>,
) -> Result<db::FileRecord> {
    if !file.hash.is_empty() {
        return Ok(file.clone());
    }

    let entry = zip_entries
        .get(&file.name)
        .with_context(|| format!("zip entry '{}' missing from '{}'", file.name, zip_path))?;

    let should_hash = rom_crcs.is_empty() || rom_crcs.contains(&entry.crc);
    if !should_hash {
        return Ok(file.clone());
    }

    let hash = util::calc_hash(&mut zip.by_index(entry.index)?)?.0;
    file.update_hash(conn, &hash)
}

fn rename_files(tx: &mut Transaction, dat_id: db::DatId, term: &TermInfo) -> Result<()> {
    // Bulk-load all Hash-status matches and group by file_id
    let hash_matches = db::MatchRecord::find_by_status_for_dat(tx, dat_id, db::MatchStatus::Hash)?;
    let mut matches_by_file: BTreeMap<db::FileId, Vec<db::MatchRecord>> = BTreeMap::new();
    for m in hash_matches {
        matches_by_file.entry(m.file_id).or_default().push(m);
    }

    // Bulk-load all rom records referenced by those matches
    let rom_ids: BTreeSet<_> = matches_by_file.values().flatten().map(|m| m.rom_id).collect();
    let roms_by_id: BTreeMap<_, _> = db::RomRecord::get_by_ids(tx, &rom_ids)?
        .into_iter()
        .map(|r| (r.id, r))
        .collect();

    for (directory, files) in db::DirRecord::get_by_dat_with_files(tx, dat_id)? {
        if util::is_zip_file(&directory.path) {
            continue;
        }

        let mut matches_by_name = BTreeMap::new();
        for file in &files {
            if let Some(file_matches) = matches_by_file.get(&file.id) {
                if file_matches.len() != 1 {
                    continue;
                }
                matches_by_name
                    .entry(&file.name)
                    .or_insert(Vec::new())
                    .push((file, &file_matches[0]));
            }
        }

        let path = Utf8PathBuf::from(directory.path);
        for (name, records) in matches_by_name {
            if records.len() == 1 {
                let (file, file_match) = &records[0];
                let rom = roms_by_id
                    .get(&file_match.rom_id)
                    .ok_or_else(|| anyhow!("rename match references missing rom id {:?}", file_match.rom_id))?;

                match db::with_savepoint(tx, |sp| {
                    let new_file = file.update_name(sp, &rom.name)?;
                    let new_match = file_match.update(sp, db::MatchStatus::Match)?;
                    let old_path = path.join(name);
                    let new_path = path.join(&new_file.name);
                    std::fs::rename(&old_path, &new_path)?;
                    Ok((new_file, new_match))
                }) {
                    Ok((new_file, new_match)) => {
                        let indicator = format_file_indicator(Some(new_match.status), term.tty_out);
                        println!("[{indicator}] {} {} -> {}", file.hash, file.name, new_file.name);
                    }
                    Err(e) => eprintln!("Failed to rename {name}. Error was {e}"),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn get_file_modes_marks_match_vs_warning_and_omits_unmatched() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let set = db::SetRecord::insert(&conn, db::NewSet::new(dat.id, "Set")).unwrap();
        let rom = db::RomRecord::insert(&conn, db::NewRom::new(dat.id, set.id, "rom.bin", 1, "abc", None)).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/tmp")).unwrap();

        let file_match = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "match.bin", 1, "abc")).unwrap();
        let file_warn = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "warn.bin", 1, "def")).unwrap();
        let file_unmatched =
            db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "none.bin", 1, "ghi")).unwrap();

        db::MatchRecord::insert(&conn, db::NewMatch::new(dat.id, file_match.id, db::MatchStatus::Match, set.id, rom.id))
            .unwrap();
        db::MatchRecord::insert(&conn, db::NewMatch::new(dat.id, file_warn.id, db::MatchStatus::Hash, set.id, rom.id))
            .unwrap();

        let modes = get_file_modes(&conn, dat.id).unwrap();
        assert_eq!(modes.get(&file_match.id), Some(&SelectMode::Matched));
        assert_eq!(modes.get(&file_warn.id), Some(&SelectMode::Warning));
        assert!(!modes.contains_key(&file_unmatched.id));
    }

    #[test]
    fn classify_helpers_use_precomputed_modes() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/tmp")).unwrap();

        let matched_file = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "a.bin", 1, "a")).unwrap();
        let warning_file = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "b.bin", 1, "b")).unwrap();
        let unmatched_file = db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir.id, "c.bin", 1, "c")).unwrap();

        let mut modes = BTreeMap::new();
        modes.insert(matched_file.id, SelectMode::Matched);
        modes.insert(warning_file.id, SelectMode::Warning);

        assert_eq!(classify_file(&matched_file, &modes), SelectMode::Matched);
        assert_eq!(classify_file(&unmatched_file, &modes), SelectMode::Unmatched);

        assert_eq!(classify_zip(&[matched_file.clone()], &modes), SelectMode::Matched);
        assert_eq!(classify_zip(&[warning_file.clone()], &modes), SelectMode::Warning);
        assert_eq!(classify_zip(&[unmatched_file.clone()], &modes), SelectMode::Unmatched);
        assert_eq!(
            classify_zip(&[matched_file, warning_file, unmatched_file], &modes),
            SelectMode::Warning
        );
    }

    #[test]
    fn get_dirs_with_files_groups_and_sorts_files_by_name() {
        let conn = db::tests::mem_db();
        let dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir1 = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/a")).unwrap();
        let dir2 = db::DirRecord::insert(&conn, db::NewDir::new(dat.id, "/b")).unwrap();

        db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir1.id, "z.bin", 1, "1")).unwrap();
        db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir1.id, "a.bin", 1, "2")).unwrap();
        db::FileRecord::insert(&conn, db::NewFile::new(dat.id, dir2.id, "m.bin", 1, "3")).unwrap();

        let grouped = db::DirRecord::get_by_dat_with_files(&conn, dat.id).unwrap();
        let names_by_dir: BTreeMap<_, Vec<_>> = grouped
            .into_iter()
            .map(|(dir, files)| (dir.id, files.into_iter().map(|f| f.name).collect::<Vec<_>>()))
            .collect();

        assert_eq!(names_by_dir.get(&dir1.id), Some(&vec!["a.bin".to_string(), "z.bin".to_string()]));
        assert_eq!(names_by_dir.get(&dir2.id), Some(&vec!["m.bin".to_string()]));
    }

    #[test]
    fn update_dat_hashes_hashless_zip_entries_when_set_name_narrows_matches() {
        let conn = db::tests::mem_db();
        let temp = tempdir().unwrap();
        let zip_path = Utf8PathBuf::from_path_buf(temp.path().join("archive.zip")).unwrap();
        let dat_path = Utf8PathBuf::from_path_buf(temp.path().join("new.dat")).unwrap();
        let content = b"hello world";
        let expected_hash = {
            let mut cursor = std::io::Cursor::new(content);
            util::calc_hash(&mut cursor).unwrap().0
        };

        {
            let zip = create_test_zip(&[("game.rom", content)]);
            std::fs::copy(zip.path(), zip_path.as_std_path()).unwrap();
        }

        let old_dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(old_dat.id, zip_path.as_str())).unwrap();
        db::FileRecord::insert(&conn, db::NewFile::new(old_dat.id, dir.id, "game.rom", content.len() as u64, ""))
            .unwrap();

        std::fs::write(
            dat_path.as_std_path(),
            r#"<datafile>
                <header>
                    <name>Updated DAT</name>
                    <description>A test dat</description>
                    <version>1.0</version>
                    <author>tester</author>
                </header>
                <game name="archive">
                    <rom name="game.rom" size="11" sha1="2aae6c35c94fcfb415dbe95f408b9ce91ee846ed" />
                </game>
            </datafile>"#,
        )
        .unwrap();

        let imported = update_dat(&conn, &dat_path, old_dat.id).unwrap();
        let files = db::FileRecord::get_by_dat(&conn, imported.id).unwrap();
        let matches = db::MatchRecord::get_by_dat(&conn, imported.id).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].hash, expected_hash);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Match);
    }

    #[test]
    fn update_dat_hashes_hashless_zip_entries_when_new_dat_crc_matches() {
        let conn = db::tests::mem_db();
        let temp = tempdir().unwrap();
        let zip_path = Utf8PathBuf::from_path_buf(temp.path().join("bundle.zip")).unwrap();
        let dat_path = Utf8PathBuf::from_path_buf(temp.path().join("new.dat")).unwrap();
        let content = b"hash me from crc";

        {
            let zip = create_test_zip(&[("game.rom", content)]);
            std::fs::copy(zip.path(), zip_path.as_std_path()).unwrap();
        }

        let expected_hash = {
            let mut cursor = std::io::Cursor::new(content);
            util::calc_hash(&mut cursor).unwrap().0
        };
        let crc = {
            let file = File::open(&zip_path).unwrap();
            let mut zip = zip::ZipArchive::new(file).unwrap();
            read_zip_entries(&mut zip, &[]).unwrap()[0].crc.clone()
        };

        let old_dat = db::DatRecord::insert(&conn, db::tests::sample_dat()).unwrap();
        let dir = db::DirRecord::insert(&conn, db::NewDir::new(old_dat.id, zip_path.as_str())).unwrap();
        db::FileRecord::insert(&conn, db::NewFile::new(old_dat.id, dir.id, "game.rom", content.len() as u64, ""))
            .unwrap();

        std::fs::write(
            dat_path.as_std_path(),
            format!(
                r#"<datafile>
                    <header>
                        <name>Updated DAT</name>
                        <description>A test dat</description>
                        <version>1.0</version>
                        <author>tester</author>
                    </header>
                    <game name="Different Set">
                        <rom name="different-name.rom" size="{size}" sha1="{sha1}" crc="{crc}" />
                    </game>
                </datafile>"#,
                size = content.len(),
                sha1 = expected_hash,
                crc = crc,
            ),
        )
        .unwrap();

        let imported = update_dat(&conn, &dat_path, old_dat.id).unwrap();
        let files = db::FileRecord::get_by_dat(&conn, imported.id).unwrap();
        let matches = db::MatchRecord::get_by_dat(&conn, imported.id).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].hash, expected_hash);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].status, db::MatchStatus::Hash);
    }

    // --- test helpers ---

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
}
