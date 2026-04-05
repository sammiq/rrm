#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod cli;
mod completion;
mod dat;
mod db;
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
use crate::db::{Deletable, DeletableByDat, FindableByName, Insertable, Queryable, QueryableByDat};
use crate::matching::{match_roms_and_insert, match_sets};
use crate::scan::{ScanOptions, read_zip_entries, scan_files};
use crate::util::{OptionIf, ResultIf};

const APP_NAME: &str = "rrm";

struct ListContext<'a> {
    conn: &'a Connection,
    dat_id: db::DatId,
    term: &'a TermInfo,
    partial_name: Option<&'a str>,
}

impl<'a> ListContext<'a> {
    fn new(conn: &'a Connection, dat_id: db::DatId, term: &'a TermInfo, partial_name: Option<&'a str>) -> Self {
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
            if *missing {
                list_missing_sets(&ctx)
            } else {
                list_found_sets(&ctx)
            }
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

fn list_dat_files(conn: &Connection) -> Result<()> {
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

/// Classify a file into a SelectMode based on its match records.
fn classify_file(conn: &Connection, file: &db::FileRecord) -> Result<SelectMode> {
    let matches = db::MatchRecord::get_by_file_id(conn, file.id)?;
    if matches.is_empty() {
        return Ok(SelectMode::Unmatched);
    }
    if matches.iter().any(|m| m.status == db::MatchStatus::Match) {
        Ok(SelectMode::Matched)
    } else {
        Ok(SelectMode::Warning)
    }
}

/// Classify a zip directory based on the statuses of all files inside it.
/// Returns Unmatched if no files have any matches, Matched if all files are Match, Warning otherwise.
fn classify_zip(conn: &Connection, dir: &db::DirRecord) -> Result<SelectMode> {
    let files = dir.get_files(conn)?;
    if files.is_empty() {
        return Ok(SelectMode::Unmatched);
    }
    let mut all_matched = true;
    let mut any_matched = false;
    for file in &files {
        match classify_file(conn, file)? {
            SelectMode::Matched => any_matched = true,
            SelectMode::Unmatched => all_matched = false,
            _ => {
                any_matched = true;
                all_matched = false;
            }
        }
    }
    if !any_matched {
        Ok(SelectMode::Unmatched)
    } else if all_matched {
        Ok(SelectMode::Matched)
    } else {
        Ok(SelectMode::Warning)
    }
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

    for dir in db::DirRecord::get_by_dat(tx, dat_id)? {
        if util::is_zip_file(&dir.path) {
            // Classify the zip as a whole
            let zip_mode = classify_zip(tx, &dir)?;
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
            let files = dir.get_files(tx)?;
            for file in &files {
                let file_mode = classify_file(tx, file)?;
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

    for directory in db::DirRecord::get_by_dat(conn, old_dat_id)? {
        if util::is_zip_file(&directory.path) {
            let matched_sets = match_sets(conn, imported.id, &directory.path)?;
            let zip_file = File::open(&directory.path)?;
            let mut zip = zip::ZipArchive::new(zip_file)
                .with_context(|| format!("could not open '{}' as a zip file", directory.path))?;
            let zip_entries: BTreeMap<_, _> = read_zip_entries(&mut zip, &[])?
                .into_iter()
                .map(|entry| (entry.name.clone(), entry))
                .collect();
            for file in directory.get_files(conn)? {
                // Zip entries may have been left unhashed during the original scan,
                // so recover the hash only when the new DAT makes it relevant.
                let file = ensure_hash_for_update(conn, &mut zip, &directory.path, &zip_entries, &file, &new_rom_crcs)?;
                match_roms_and_insert(conn, imported.id, &file, &matched_sets)?;
            }
        } else {
            let matched_sets = BTreeSet::new();
            for file in directory.get_files(conn)? {
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

fn list_dat_records(conn: &Connection, dat_id: db::DatId) -> Result<()> {
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

fn list_sets(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
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

fn list_roms(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
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
fn format_file_indicator(status: Option<db::MatchStatus>, is_tty: bool) -> &'static str {
    match status {
        None => if is_tty { "❌" } else { "NONE" },
        Some(db::MatchStatus::Hash) | Some(db::MatchStatus::Name) => if is_tty { "⚠️" } else { "WARN" },
        Some(db::MatchStatus::Match) => if is_tty { "✅" } else { " OK " },
    }
}

fn format_match_status(
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

fn list_scanned_files(ctx: &ListContext<'_>, mode: SelectMode) -> Result<()> {
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
enum SetStatus {
    Missing,
    Partial,
    Complete,
}

fn calculate_set_status(set_roms: &[db::RomRecord], matched_rom_ids: &BTreeSet<db::RomId>) -> SetStatus {
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

fn list_missing_sets(ctx: &ListContext<'_>) -> Result<()> {
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

fn list_found_sets(ctx: &ListContext<'_>) -> Result<()> {
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

    for directory in db::DirRecord::get_by_dat(tx, dat_id)? {
        if util::is_zip_file(&directory.path) {
            continue;
        }

        let files = directory.get_files(tx)?;
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
