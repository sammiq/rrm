mod db;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufReader;
use std::io::Write;

use anyhow::{Context, Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand, ValueEnum};
use roxmltree::{Document, ParsingOptions};
use rusqlite::{Connection, Transaction, TransactionBehavior};

const APP_NAME: &str = "rrm";

macro_rules! println_if {
    ($cond:expr, $($arg:tt)*) => {
        if $cond {
            println!($($arg)*);
        }
    };
}

#[derive(Debug, Parser)]
#[command(multicall = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Data {
        #[command(subcommand)]
        data: DataCommands,
    },
    Files {
        #[command(subcommand)]
        files: FileCommands,
    },
    Exit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ListMode {
    All,
    Matched,
    Warning,
    Unmatched,
}

#[derive(Debug, Subcommand)]
enum FileCommands {
    /// scan a path and match files with the current dat file
    Scan {
        /// extensions to exclude when scanning files
        #[arg(long, value_delimiter = ',', default_value = "m3u,dat,txt")]
        exclude: Vec<String>,
        /// scan recursively each directory found
        #[arg(short('R'), long, default_value_t = false)]
        recursive: bool,
        /// re-scan existing files in the directory and not just new files
        #[arg(long, default_value_t = false)]
        full: bool,
        /// the path to use for scanning files
        #[arg(default_value=".", value_hint = clap::ValueHint::DirPath)]
        path: Utf8PathBuf,
    },
    /// list all files scanned and show their status
    List {
        /// show only files with this status
        #[arg(long, value_enum, default_value_t = ListMode::All)]
        mode: ListMode,
        /// show only files partially matching this name
        partial_name: Option<String>,
    },
    /// list all sets matched by scanned files
    Sets {
        /// show missing sets instead of matches
        #[arg(long, default_value_t = false)]
        missing: bool,
        /// show only sets partially matching this name
        partial_name: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum DataCommands {
    /// import a dat file into the system and make it the current dat file
    Import {
        #[arg(value_hint = clap::ValueHint::FilePath)]
        dat_file: Utf8PathBuf,
    },
    /// update the current dat file with a new version and re-match files
    Update {
        #[arg(value_hint = clap::ValueHint::FilePath)]
        dat_file: Utf8PathBuf,
    },
    /// List dat files in the system
    List,
    /// Select the current dat file
    Select { index: usize },
    /// Show all Set and Roms in the current dat file
    Records,
    /// Search for a Set in the current dat file
    Sets { name: String },
    /// Search for a Rom in the current dat file
    Roms { name: String },
}

fn readline() -> Result<String> {
    write!(std::io::stdout(), "$ ")?;
    std::io::stdout().flush()?;
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;
    Ok(buffer)
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

    //default the dat to the current directory if it exists
    if let Ok(current_path) = Utf8PathBuf::from(".").canonicalize_utf8() {
        let paths = db::get_directories_by_path(&conn, current_path.as_str())?;
        if !paths.is_empty()
            && let Some(dat) = db::get_dat(&conn, paths[0].dat_id)?
        {
            println!("dat file `{}` selected.", dat.name);
            dat_id = Some(dat.id);
        }
    }

    loop {
        let line = readline()?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let maybe_args = shlex::split(line);
        if maybe_args.is_none() {
            eprintln!("error: Invalid quoting");
            continue;
        }

        match Cli::try_parse_from(maybe_args.unwrap()) {
            Ok(cli) => {
                match cli.command {
                    Commands::Data { data } => match data {
                        DataCommands::Import { dat_file } => {
                            if dat_file.is_file() {
                                match import_dat(&mut conn, &dat_file) {
                                    Ok(imported) => {
                                        dat_id = Some(imported.id);
                                        println!("dat file `{}` imported and selected.", imported.name);
                                    }
                                    Err(e) => println!("Failed to import dat file. {e}"),
                                }
                            } else {
                                println!("`{}` is not a valid file", dat_file);
                            }
                        }
                        DataCommands::Update { dat_file } => {
                            if let Some(old_dat_id) = dat_id {
                                //ideally we would ask the user to confirm
                                match update_dat(&mut conn, dat_file, old_dat_id) {
                                    Ok(imported) => {
                                        dat_id = Some(imported.id);
                                        println!("dat file `{}` imported and updated.", imported.name);
                                    }
                                    Err(e) => println!("Failed to import dat file. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        DataCommands::List => match db::get_dats(&conn) {
                            Ok(dats) => {
                                println!("Installed dat files:");
                                for (i, dat) in dats.iter().enumerate() {
                                    println!("[{i}] {} version: {} author: {}", dat.name, dat.version, dat.author);
                                }
                            }
                            Err(e) => println!("Failed to list dat files. {e}"),
                        },
                        DataCommands::Select { index } => match db::get_dats(&conn) {
                            Ok(dats) => {
                                if let Some(dat) = dats.get(index) {
                                    println!("dat file `{}` selected.", dat.name);
                                    dat_id = Some(dat.id);
                                } else {
                                    println!("Invalid selection; Installed dat files:");
                                    for (i, dat) in dats.iter().enumerate() {
                                        println!("[{i}] {} version: {} author: {}", dat.name, dat.version, dat.author);
                                    }
                                }
                            }
                            Err(e) => println!("Failed to select dat file. {e}"),
                        },
                        DataCommands::Records => {
                            if let Some(dat_id) = dat_id {
                                match list_dat_records(&conn, dat_id) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to list dat file. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        DataCommands::Sets { name } => {
                            if let Some(dat_id) = dat_id {
                                match find_sets_by_name(&conn, dat_id, &name) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to find sets. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        DataCommands::Roms { name } => {
                            if let Some(dat_id) = dat_id {
                                match find_roms(&conn, dat_id, &name) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to find roms. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                    },
                    Commands::Files { files } => {
                        if let Some(dat_id) = dat_id {
                            match files {
                                FileCommands::Scan {
                                    exclude,
                                    recursive,
                                    full,
                                    path,
                                } => {
                                    //make sure path is resolved to something absolte and proper before scanning
                                    let scan_path = path.canonicalize_utf8()?;
                                    if scan_path.is_dir() {
                                        match scan_files(&mut conn, dat_id, &scan_path, &exclude, recursive, !full) {
                                            Ok(_) => {
                                                println!("Directory `{}` scanned.", scan_path)
                                            }
                                            Err(e) => println!("Failed to scan directory. {e}"),
                                        }
                                    } else {
                                        println!("`{}` is not a valid directory", scan_path);
                                    }
                                }
                                FileCommands::List { mode, partial_name } => {
                                    match list_files(&mut conn, dat_id, mode, partial_name.as_deref()) {
                                        Ok(_) => {}
                                        Err(e) => println!("Failed to list files. {e}"),
                                    }
                                }
                                FileCommands::Sets { missing, partial_name } => {
                                    match list_sets(&mut conn, dat_id, missing, partial_name.as_deref()) {
                                        Ok(_) => {}
                                        Err(e) => println!("Failed to list files. {e}"),
                                    }
                                }
                            }
                        } else {
                            println!("No dat file selected");
                        }
                    }
                    Commands::Exit => return Ok(()),
                };
            }
            Err(e) => {
                e.print()?;
            }
        }
    }
}

fn update_dat(conn: &mut Connection, dat_file: Utf8PathBuf, old_dat_id: db::DatId) -> Result<db::DatRecord> {
    let imported = import_dat(conn, &dat_file)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    for directory in db::get_directories(&tx, old_dat_id, None)? {
        //check if its a zip file, if so, restrict matches to set name if matched
        let matched_sets = if directory.path.ends_with("zip") {
            match_sets(&tx, imported.id, &directory.path)?
        } else {
            BTreeSet::new()
        };
        let dir_files = db::get_files(&tx, directory.id, None)?;
        let unique_files: BTreeMap<String, &db::FileRecord> =
            dir_files.iter().map(|file| (file.name.clone(), file)).collect();
        //delete all the old files in the directory
        db::delete_files(&tx, directory.id)?;
        for (_, file) in unique_files {
            //rematch using existing information, but link to the new dat
            process_file_matches(&tx, imported.id, directory.id, file.size, &file.name, &file.hash, &matched_sets)?;
        }
    }
    //relink all directories to the new dat
    db::update_directories(&tx, old_dat_id, imported.id)?;

    tx.commit()?;
    Ok(imported)
}

fn import_dat<P: AsRef<Utf8Path>>(conn: &mut Connection, file_path: P) -> Result<db::DatRecord> {
    //TODO check whether name named item exists
    let df_buffer = std::fs::read_to_string(file_path.as_ref()).context("Unable to read reference dat file")?;
    let df_xml = Document::parse_with_options(
        df_buffer.as_str(),
        ParsingOptions {
            allow_dtd: true,
            ..Default::default()
        },
    )
    .context("Unable to parse reference dat file")?;

    //find header get the mandatory fields, according to https://github.com/Logiqx/logiqx-dev/blob/master/DatLib/datafile.dtd
    let mut name = None;
    let mut description = None;
    let mut version = None;
    let mut author = None;
    for header_node in df_xml
        .root_element()
        .children()
        .find(|node| node.tag_name().name() == "header")
        .map(|header| header.children())
        .context("Could not find header in reference dat file")?
    {
        match header_node.tag_name().name() {
            "name" => name = header_node.text(),
            "description" => description = header_node.text(),
            "version" => version = header_node.text(),
            "author" => author = header_node.text(),
            _ => {}
        };
    }

    if (name.is_none()) || (description.is_none()) || (version.is_none()) || (author.is_none()) {
        bail!("Required fields in reference dat header not found");
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let dat = db::insert_dat(&tx, name.unwrap(), description.unwrap(), version.unwrap(), author.unwrap(), "sha1")?;

    let dat_id = dat.id;

    for game_node in df_xml
        .root_element()
        .children()
        .filter(|node| node.tag_name().name() == "game")
    {
        let game_name = game_node
            .attribute("name")
            .context("Unable to read game name in reference dat file")?;

        let set = db::insert_set(&tx, dat_id, game_name)?;

        let set_id = set.id;

        for rom_node in game_node.descendants().filter(|node| node.tag_name().name() == "rom") {
            let rom_name = rom_node.attribute("name").context("Unable to read game name")?;
            let rom_size = rom_node.attribute("size").context("Unable to read game size")?;
            let rom_hash = rom_node.attribute("sha1").context("Unable to read game hash")?;
            db::insert_rom(&tx, dat_id, set_id, rom_name, rom_size.parse().unwrap(), rom_hash)?;
        }
    }

    tx.commit()?;

    Ok(dat)
}

fn list_dat_records(conn: &Connection, dat_id: db::DatId) -> Result<()> {
    let dat_record = db::get_dat(conn, dat_id)?.unwrap();
    println!("Name:        {}", dat_record.name);
    println!("Description: {}", dat_record.description);
    println!("Version:     {}", dat_record.version);
    println!("Author:      {}", dat_record.author);

    println!("--- SETS ---");
    for set in db::get_sets(conn, dat_id)? {
        println!("{}", set.name);
        for rom in db::get_roms_by_set(conn, set.id)? {
            println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size));
        }
    }
    Ok(())
}

fn find_sets_by_name(conn: &Connection, dat_id: db::DatId, name: &str) -> Result<()> {
    let sets = db::get_sets_by_name(conn, dat_id, name, false)?;
    if sets.is_empty() {
        println!("No sets found matching `{name}`");
    } else {
        for set in sets {
            println!("{}", set.name);
            for rom in db::get_roms_by_set(conn, set.id)? {
                println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size));
            }
        }
    }
    Ok(())
}

fn find_roms(conn: &Connection, dat_id: db::DatId, name: &str) -> Result<()> {
    let roms = db::get_roms_by_name(conn, dat_id, name, false)?;
    if roms.is_empty() {
        println!("No roms found matching `{name}`");
    } else {
        let mut roms_by_set: BTreeMap<db::SetId, Vec<db::RomRecord>> = BTreeMap::new();
        for rom in roms {
            roms_by_set.entry(rom.set_id).or_default().push(rom);
        }

        for (set_id, roms) in roms_by_set {
            let set = db::get_set(conn, set_id)?;
            println!("{}", set.name);
            for roms in roms {
                println!("    {} {} - {}", roms.hash, roms.name, util::human_size(roms.size));
            }
        }
    }
    Ok(())
}

fn scan_files(
    conn: &mut Connection,
    dat_id: db::DatId,
    scan_path: &Utf8Path, //expect this to be canonicalized
    exclude: &[String],
    recursive: bool,
    incremental: bool,
) -> Result<()> {
    let mut tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    scan_directory(&mut tx, dat_id, scan_path, exclude, recursive, incremental, None)?;
    tx.commit()?;
    Ok(())
}

fn scan_directory(
    tx: &mut Transaction,
    dat_id: db::DatId,
    scan_path: &Utf8Path,
    exclude: &[String],
    recursive: bool,
    incremental: bool,
    parent_id: Option<db::DirId>,
) -> Result<(), anyhow::Error> {
    let (dir_id, incremental) = match db::get_directory(tx, dat_id, scan_path.as_str())? {
        Some(dir) => {
            if incremental {
                // add on to existing records
                (dir.id, true)
            } else {
                //wipe existing file records and do full scan
                let _ = db::delete_files(tx, dir.id)?;
                (dir.id, false)
            }
        }
        None => {
            //no existing records, do a full scan
            let dir = db::insert_directory(tx, dat_id, scan_path.as_str(), parent_id)?;
            (dir.id, false)
        }
    };

    let existing_dirs = db::get_directories(tx, dat_id, Some(dir_id))?;
    let mut existing_paths: BTreeSet<&str> = existing_dirs.iter().map(|dir| dir.path.as_str()).collect();
    let existing_files = db::get_files(tx, dir_id, None)?;
    let mut existing_names: BTreeSet<&str> = existing_files.iter().map(|file| file.name.as_str()).collect();
    for entry in scan_path.read_dir_utf8()? {
        let entry = entry?;
        let path = entry.path();
        if util::is_hidden_file(path) {
            //skip
        } else if recursive && path.is_dir() {
            scan_directory(tx, dat_id, path, exclude, recursive, incremental, Some(dir_id))?;
            existing_paths.remove(path.as_str());
        } else if path.is_file() {
            if path
                .extension()
                .map(|ext| exclude.iter().any(|e| ext.eq_ignore_ascii_case(e)))
                .unwrap_or_default()
            {
                continue;
            }
            if path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("zip")) {
                //for zip files we need to rollback the entire directory and files if it failed to scan properly
                let mut sp = tx.savepoint()?;
                if let Err(e) = scan_zip_file(&sp, dat_id, path, incremental, dir_id) {
                    eprintln!("Failed to scan {}. Error: {e}", path);
                    sp.rollback()?;
                } else {
                    sp.commit()?;

                    existing_paths.remove(path.as_str());
                }
            } else {
                match path.file_name().context("Could not get filename") {
                    Ok(filename) => {
                        if existing_names.remove(filename) && incremental {
                            //there was an existing scanned file, so skip it
                            continue;
                        }
                        if let Err(e) = scan_file(tx, dat_id, dir_id, path, filename) {
                            eprintln!("Failed to scan {}. Error: {e}", path);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to scan {}. Error: {e}", path);
                    }
                }
            }
        }
    }
    for existing_path in existing_paths {
        match db::get_directory(tx, dat_id, existing_path) {
            Ok(dir) => {
                if let Some(dir) = dir {
                    if let Err(e) = db::delete_files(tx, dir.id) {
                        eprintln!("Failed to delete files in {}. Error: {e}", existing_path);
                    }
                    if let Err(e) = db::delete_directory(tx, dir.id) {
                        eprintln!("Failed to delete directory {}. Error: {e}", existing_path);
                    }
                } else {
                    eprintln!("Failed to find directory entry {}.", existing_path);
                }
            }
            Err(e) => {
                eprintln!("Failed to get directory entry {}. Error: {e}", existing_path);
            }
        }
    }
    for existing_name in existing_names {
        if let Err(e) = db::delete_file(tx, dir_id, existing_name) {
            eprintln!("Failed to remove {}. Error: {e}", existing_name);
        }
    }
    Ok(())
}

fn scan_zip_file(
    conn: &Connection,
    dat_id: db::DatId,
    path: &Utf8Path,
    incremental: bool,
    parent_id: db::DirId,
) -> Result<()> {
    let maybe_dir = db::get_directory(conn, dat_id, path.as_str())?;
    if incremental && maybe_dir.is_some() {
        //if incremental and we have scanned this zip file before, skip it
        return Ok(());
    }

    let dir_id = match maybe_dir {
        Some(dir) => {
            //wipe existing file records and do full scan
            let _ = db::delete_files(conn, dir.id)?;
            dir.id
        }
        None => {
            //no existing records, do a full scan
            let dir = db::insert_directory(conn, dat_id, path.as_str(), Some(parent_id))?;
            dir.id
        }
    };

    let matched = match_sets(conn, dat_id, path)?;

    let file = File::open(path)?;
    let mut zip = zip::ZipArchive::new(file).with_context(|| format!("could not open '{}' as a zip file", path))?;
    for i in 0..zip.len() {
        match zip.by_index(i) {
            Ok(mut inner_file) => {
                if inner_file.is_file() {
                    let (hash, size) = util::calc_hash(&mut inner_file)?;
                    let name = inner_file.name();
                    process_file_matches(conn, dat_id, dir_id, size, name, &hash, &matched)?;
                }
            }
            Err(error) => bail!("{}", error),
        }
    }
    Ok(())
}

fn match_sets<P: AsRef<Utf8Path>>(conn: &Connection, dat_id: db::DatId, path: P) -> Result<BTreeSet<db::SetId>> {
    let name = path.as_ref().file_prefix().unwrap();
    let sets = db::get_sets_by_name(conn, dat_id, name, true)?;
    let matched: BTreeSet<db::SetId> = sets.iter().map(|record| record.id).collect();
    Ok(matched)
}

fn scan_file(conn: &Connection, dat_id: db::DatId, dir_id: db::DirId, path: &Utf8Path, filename: &str) -> Result<()> {
    //scan the file,find a match and insert
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    let mut reader = BufReader::new(&file);
    let (hash, _) = util::calc_hash(&mut reader)?;

    process_file_matches(conn, dat_id, dir_id, file_size, filename, &hash, &BTreeSet::new())?;
    Ok(())
}

enum Match {
    None,
    Partial(Vec<db::MatchStatus>),
    Exact(Vec<db::MatchStatus>),
}

fn match_roms(
    conn: &Connection,
    dat_id: db::DatId,
    file_size: u64,
    filename: &str,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<Match> {
    // Step 1: is there any roms called the same as the filename?
    let named_roms = db::get_roms_by_name(conn, dat_id, filename, true)?;
    if named_roms.is_empty() {
        // Step 2a: if nothing named the same, check for hash matches, and match accordingly
        let hash_roms = db::get_roms_by_hash(conn, dat_id, hash)?;
        let matched = if hash_roms.is_empty() {
            Match::None
        } else {
            let matches = hash_roms
                .iter()
                .map(|rom| db::MatchStatus::Hash {
                    set_id: rom.set_id,
                    rom_id: rom.id,
                })
                .collect();
            Match::Partial(matches)
        };
        Ok(matched)
    } else {
        //Step 2b: if something is named the same, check for exact matches with those items
        let exact_matches: Vec<db::MatchStatus> = named_roms
            .iter()
            .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
            .filter(|rom| file_size == rom.size && hash == rom.hash)
            .map(|rom| db::MatchStatus::Match {
                set_id: rom.set_id,
                rom_id: rom.id,
            })
            .collect();

        if !exact_matches.is_empty() {
            return Ok(Match::Exact(exact_matches));
        }

        //Step 3b: if something is named the same, check whether we got hash only
        //matches, if so, then treat it as a hash match, otherwise its name only matches
        let hash_roms = db::get_roms_by_hash(conn, dat_id, hash)?;
        let matches: Vec<db::MatchStatus> = if hash_roms.is_empty() {
            named_roms
                .iter()
                .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
                .map(|rom| db::MatchStatus::Name {
                    set_id: rom.set_id,
                    rom_id: rom.id,
                })
                .collect()
        } else {
            hash_roms
                .iter()
                .map(|rom| db::MatchStatus::Hash {
                    set_id: rom.set_id,
                    rom_id: rom.id,
                })
                .collect()
        };
        Ok(Match::Partial(matches))
    }
}

fn process_file_matches(
    conn: &Connection,
    dat_id: db::DatId,
    dir_id: db::DirId,
    file_size: u64,
    filename: &str,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<()> {
    match match_roms(conn, dat_id, file_size, filename, hash, matched_sets)? {
        Match::None => {
            db::insert_file(conn, dir_id, filename, file_size, hash, db::MatchStatus::None)?;
        }
        Match::Partial(items) => {
            for item in items {
                db::insert_file(conn, dir_id, filename, file_size, hash, item)?;
            }
        }
        Match::Exact(items) => {
            for item in items {
                db::insert_file(conn, dir_id, filename, file_size, hash, item)?;
            }
        }
    }

    Ok(())
}

fn list_files(conn: &mut Connection, dat_id: db::DatId, mode: ListMode, partial_name: Option<&str>) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id, None)?;

    for dir in dirs {
        let files = db::get_files(conn, dir.id, partial_name)?;

        if files.is_empty() {
            continue;
        }

        let mut lines = Vec::new();
        for file in files {
            match file.status {
                db::MatchStatus::None => {
                    if mode == ListMode::Unmatched || mode == ListMode::All {
                        lines.push(format!("[❌] {} {} - unknown file", file.hash, file.name));
                    }
                }
                db::MatchStatus::Hash { set_id: _, rom_id: _ } => {
                    if mode == ListMode::Warning || mode == ListMode::All {
                        lines.push(format!("[⚠️] {} {} - incorrect name", file.hash, file.name));
                    }
                }
                db::MatchStatus::Name { set_id: _, rom_id: _ } => {
                    if mode == ListMode::Warning || mode == ListMode::All {
                        lines.push(format!("[⚠️] {} {} - incorrect hash", file.hash, file.name));
                    }
                }
                db::MatchStatus::Match { set_id: _, rom_id: _ } => {
                    if mode == ListMode::Matched || mode == ListMode::All {
                        lines.push(format!("[✅] {} {}", file.hash, file.name));
                    }
                }
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

fn list_sets(conn: &mut Connection, dat_id: db::DatId, missing: bool, partial_name: Option<&str>) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id, None)?;

    let mut sets_to_files: BTreeMap<db::SetId, Vec<db::FileRecord>> = BTreeMap::new();
    let mut found_roms: BTreeSet<db::RomId> = BTreeSet::new();

    for dir in dirs {
        let files = db::get_files(conn, dir.id, None)?;
        for file in files {
            match file.status {
                db::MatchStatus::None => {}
                db::MatchStatus::Hash { set_id, rom_id } => {
                    sets_to_files.entry(set_id).or_default().push(file);
                    found_roms.insert(rom_id);
                }
                db::MatchStatus::Name { set_id, rom_id } => {
                    sets_to_files.entry(set_id).or_default().push(file);
                    found_roms.insert(rom_id);
                }
                db::MatchStatus::Match { set_id, rom_id } => {
                    sets_to_files.entry(set_id).or_default().push(file);
                    found_roms.insert(rom_id);
                }
            }
        }
    }

    let sets = db::get_sets(conn, dat_id)?;
    if missing {
        println!("--- MISSING SETS ---");
        for set in &sets {
            if let Some(partial_name) = partial_name
                && !set
                    .name
                    .to_ascii_lowercase()
                    .contains(&partial_name.to_ascii_lowercase())
            {
                continue;
            }
            println_if!(!sets_to_files.contains_key(&set.id), "[❌] {}", set.name);
        }
        println!("{} / {} sets missing.", sets.len() - sets_to_files.len(), sets.len());
    } else {
        println!("--- FOUND SETS ---");
        for set in &sets {
            if let Some(partial_name) = partial_name
                && !set
                    .name
                    .to_ascii_lowercase()
                    .contains(&partial_name.to_ascii_lowercase())
            {
                continue;
            }

            if let Some(files) = sets_to_files.get(&set.id) {
                let roms = db::get_roms_by_set(conn, set.id)?;
                let roms_by_id: BTreeMap<db::RomId, &db::RomRecord> = roms.iter().map(|rom| (rom.id, rom)).collect();
                if files.len() == roms.len() {
                    println!("[✅] {}", set.name);
                } else {
                    println!("[⚠️] {}, set has missing roms", set.name);
                }
                for file in files {
                    match file.status {
                        db::MatchStatus::Hash { set_id: _, rom_id } => {
                            println!(" ⚠️  {} {}, should be named {}", file.hash, file.name, roms_by_id[&rom_id].name);
                        }
                        db::MatchStatus::Name { set_id: _, rom_id } => {
                            println!(
                                "  ⚠️  {} {}, should have hash {}",
                                file.hash, file.name, roms_by_id[&rom_id].hash
                            );
                        }
                        db::MatchStatus::Match { set_id: _, rom_id: _ } => {
                            println!(" ✅  {} {}", file.hash, file.name);
                        }
                        db::MatchStatus::None => unreachable!(),
                    }
                }
                for rom in roms {
                    println_if!(!found_roms.contains(&rom.id), " ❌  {} {} missing", rom.hash, rom.name);
                }
            }
        }
        println!("{} / {} sets found.", sets_to_files.len(), sets.len());
    }
    Ok(())
}
