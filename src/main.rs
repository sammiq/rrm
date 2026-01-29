mod db;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, IsTerminal, Write};

use anyhow::{Context, Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand, ValueEnum};
use roxmltree::{Document, ParsingOptions};
use rusqlite::{Connection, Transaction, TransactionBehavior};

const APP_NAME: &str = "rrm";

// constants for XML dat file
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

macro_rules! println_if {
    ($cond:expr, $($arg:tt)*) => {
        if $cond {
            println!($($arg)*);
        }
    };
}

#[derive(Debug, Parser)]
#[clap(version, about, long_about = None)]
struct Args {
    /// select the dat file to use
    #[arg(short, long)]
    select: Option<usize>,

    /// command to execute, if none given
    /// will enter interactive mode
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Parser)]
#[command(multicall = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// execute commands on dat file
    Data {
        #[command(subcommand)]
        data: DataCommands,
    },
    /// execute commands on files
    Files {
        #[command(subcommand)]
        files: FileCommands,
    },
    /// exit from interactive mode
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
    //rename files to the correct name (loose files only)
    Rename,
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
    Remove,
    /// List dat files in the system
    List,
    /// Select the current dat file
    Select {
        index: usize,
    },
    /// Show all Set and Roms in the current dat file
    Records,
    /// Search for a Set in the current dat file
    Sets {
        partial_name: Option<String>,
    },
    /// Search for a Rom in the current dat file
    Roms {
        partial_name: Option<String>,
    },
}

fn readline() -> Result<String> {
    write!(std::io::stdout(), "$ ")?;
    std::io::stdout().flush()?;
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;
    Ok(buffer)
}

struct TermInfo {
    tty_in: bool,
    tty_out: bool,
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

    let term = TermInfo {
        tty_in: std::io::stdin().is_terminal(),
        tty_out: std::io::stdout().is_terminal(),
    };

    let args = Args::parse();
    if let Some(index) = args.select {
        do_command(
            &mut conn,
            &mut dat_id,
            &Commands::Data {
                data: DataCommands::Select { index },
            },
            &term,
        )?;
    } else {
        //default the dat to the current directory if it exists
        if let Some(current_path) = std::env::current_dir()
            .ok()
            .and_then(|path| Utf8PathBuf::from_path_buf(path).ok())
            .and_then(|path| path.canonicalize_utf8().ok())
        {
            let paths = db::get_directories_by_path(&conn, current_path.as_str())?;
            if !paths.is_empty() {
                let dat = db::get_dat(&conn, paths[0].dat_id)?;
                println!("dat file `{}` selected.", dat.name);
                dat_id = Some(dat.id);
            } else {
                eprintln!("No default dat file for current path.");
            }
        } else {
            eprintln!("Invalid current path, no default dat file for current path.");
        }
    }

    if let Some(command) = args.command {
        do_command(&mut conn, &mut dat_id, &command, &term)?;
    } else if term.tty_in {
        loop {
            let line = readline()?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(args) = shlex::split(line) {
                match Cli::try_parse_from(args) {
                    Ok(cli) => {
                        let exit = do_command(&mut conn, &mut dat_id, &cli.command, &term)?;
                        if exit {
                            break;
                        }
                    }
                    Err(e) => e.print()?,
                };
            } else {
                eprintln!("error: Invalid quoting");
            }
        }
    }
    Ok(())
}

fn do_command(
    conn: &mut Connection,
    dat_id: &mut Option<db::DatId>,
    command: &Commands,
    term: &TermInfo,
) -> Result<bool> {
    match command {
        Commands::Data { data } => handle_data_commands(conn, dat_id, term, data),
        Commands::Files { files } => handle_file_commands(conn, dat_id, term, files),
        Commands::Exit => return Ok(true),
    };
    Ok(false)
}

fn handle_data_commands(conn: &mut Connection, dat_id: &mut Option<db::DatId>, term: &TermInfo, data: &DataCommands) {
    match data {
        DataCommands::Import { dat_file } => {
            if dat_file.is_file() {
                match import_dat(conn, dat_file) {
                    Ok(imported) => {
                        println!("dat file `{}` imported and selected.", imported.name);
                        *dat_id = Some(imported.id);
                    }
                    Err(e) => eprintln!("Failed to import dat file. {e}"),
                }
            } else {
                eprintln!("`{}` is not a valid file", dat_file);
            }
        }
        DataCommands::Update { dat_file } => {
            if let Some(old_dat_id) = *dat_id {
                let confirmed = if term.tty_in {
                    print!("Are you sure you want to update the current dat file? (y/N): ");
                    ask_for_confirmation()
                } else {
                    Ok(true)
                };
                match confirmed {
                    Ok(true) => match update_dat(conn, dat_file, old_dat_id) {
                        Ok(imported) => {
                            println!("dat file `{}` imported and updated.", imported.name);
                            *dat_id = Some(imported.id);
                        }
                        Err(e) => eprintln!("Failed to import dat file. {e}"),
                    },
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("Failed to read confirmation. {e}");
                    }
                }
            } else {
                eprintln!("No dat file selected");
            }
        }
        DataCommands::Remove => {
            if let Some(old_dat_id) = *dat_id {
                //ask the user to confirm
                let confirmed = if term.tty_in {
                    print!("Are you sure you want to remove the current dat file? (y/N): ");
                    ask_for_confirmation()
                } else {
                    Ok(true)
                };
                match confirmed {
                    Ok(true) => match delete_dat(conn, old_dat_id) {
                        Ok(_) => {
                            println!("dat file removed.");
                            *dat_id = None;
                        }
                        Err(e) => eprintln!("Failed to remove dat file. {e}"),
                    },
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("Failed to read confirmation. {e}");
                    }
                }
            } else {
                eprintln!("No dat file selected");
            }
        }
        DataCommands::List => {
            if let Err(e) = list_dat_files(conn) {
                eprintln!("Failed to list dat files. {e}");
            }
        }
        DataCommands::Select { index } => match db::get_dats(conn) {
            Ok(dats) => {
                if let Some(dat) = dats.get(*index) {
                    println!("dat file `{}` selected.", dat.name);
                    *dat_id = Some(dat.id);
                } else {
                    eprintln!("Invalid dat file selection.");
                }
            }
            Err(e) => eprintln!("Failed to select dat file. {e}"),
        },
        DataCommands::Records => {
            if let Some(dat_id) = *dat_id {
                if let Err(e) = list_dat_records(conn, dat_id) {
                    eprintln!("Failed to list dat file. {e}");
                }
            } else {
                eprintln!("No dat file selected");
            }
        }
        DataCommands::Sets { partial_name } => {
            if let Some(dat_id) = *dat_id {
                if let Err(e) = find_sets_by_name(conn, dat_id, partial_name.as_deref()) {
                    eprintln!("Failed to find sets. {e}");
                }
            } else {
                eprintln!("No dat file selected");
            }
        }
        DataCommands::Roms { partial_name } => {
            if let Some(dat_id) = *dat_id {
                if let Err(e) = find_roms(conn, dat_id, partial_name.as_deref()) {
                    eprintln!("Failed to find roms. {e}");
                }
            } else {
                eprintln!("No dat file selected");
            }
        }
    }
}

fn ask_for_confirmation() -> Result<bool> {
    std::io::stdout().flush()?;
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;
    let buffer = buffer.trim();
    Ok(buffer.eq_ignore_ascii_case("y"))
}

fn handle_file_commands(conn: &mut Connection, dat_id: &mut Option<db::DatId>, term: &TermInfo, files: &FileCommands) {
    if let Some(dat_id) = *dat_id {
        match files {
            FileCommands::Scan {
                exclude,
                recursive,
                full,
                path,
            } => {
                //make sure path is resolved to something absolte and proper before scanning
                if let Some(scan_path) = path.canonicalize_utf8().ok()
                    && scan_path.is_dir()
                {
                    match scan_files(conn, dat_id, term, &scan_path, exclude, *recursive, !full) {
                        Ok(_) => println!("Directory `{}` scanned.", scan_path),
                        Err(e) => eprintln!("Failed to scan directory. {e}"),
                    }
                } else {
                    eprintln!("`{}` is not a valid directory", path);
                }
            }
            FileCommands::List { mode, partial_name } => {
                if let Err(e) = list_files(conn, dat_id, term, *mode, partial_name.as_deref()) {
                    eprintln!("Failed to list files. {e}");
                }
            }
            FileCommands::Sets { missing, partial_name } => {
                if let Err(e) = list_sets(conn, dat_id, term, *missing, partial_name.as_deref()) {
                    eprintln!("Failed to list files. {e}");
                }
            }
            FileCommands::Rename => {
                if let Err(e) = rename_files(conn, dat_id, term) {
                    eprintln!("Failed to rename files. {e}");
                }
            }
        }
    } else {
        eprintln!("No dat file selected");
    };
}

fn list_dat_files(conn: &Connection) -> Result<()> {
    let dats = db::get_dats(conn)?;
    if dats.is_empty() {
        eprintln!("No installed dat files.")
    } else {
        println!("Installed dat files:");
        for (i, dat) in dats.iter().enumerate() {
            println!("[{i}] {} version: {} author: {}", dat.name, dat.version, dat.author);
        }
    }
    Ok(())
}

fn update_dat(conn: &mut Connection, dat_file: &Utf8PathBuf, old_dat_id: db::DatId) -> Result<db::DatRecord> {
    let imported = import_dat(conn, dat_file)?;

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    for directory in db::get_directories(&tx, old_dat_id, None)? {
        //check if its a zip file, if so, restrict matches to set name if matched
        let matched_sets = if Utf8Path::new(&directory.path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
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

    //find header get the mandatory fields, according to
    // https://github.com/Logiqx/logiqx-dev/blob/master/DatLib/datafile.dtd
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

    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let dat = db::insert_dat(
        &tx,
        name.context("unable to find name attribute in header")?,
        description.context("unable to find description attribute in header")?,
        version.context("unable to find version attribute in header")?,
        author.context("unable to find author attribute in header")?,
        "sha1",
    )?;

    let dat_id = dat.id;

    for game_node in df_xml
        .root_element()
        .children()
        .filter(|node| node.tag_name().name() == TAG_GAME)
    {
        let game_name = game_node
            .attribute(ATTR_GAME_NAME)
            .context("Unable to read game name in reference dat file")?;

        let set = db::insert_set(&tx, dat_id, game_name)?;

        let set_id = set.id;

        for rom_node in game_node.descendants().filter(|node| node.tag_name().name() == TAG_ROM) {
            let rom_name = rom_node.attribute(ATTR_ROM_NAME).context("Unable to read game name")?;
            let rom_size = rom_node.attribute(ATTR_ROM_SIZE).context("Unable to read game size")?;
            let rom_hash = rom_node.attribute(ATTR_ROM_HASH).context("Unable to read game hash")?;
            db::insert_rom(
                &tx,
                dat_id,
                set_id,
                rom_name,
                rom_size.parse().context("should be a valid number")?,
                rom_hash,
            )?;
        }
    }

    tx.commit()?;

    Ok(dat)
}

fn delete_dat(conn: &mut Connection, dat_id: db::DatId) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    //remove all scanned files and directories
    for dir in db::get_directories(&tx, dat_id, None)? {
        db::delete_files(&tx, dir.id)?;
    }
    db::delete_directories(&tx, dat_id)?;

    //remove all roms and sets before removing the dat
    db::delete_roms(&tx, dat_id)?;
    db::delete_sets(&tx, dat_id)?;

    db::delete_dat(&tx, dat_id)?;

    tx.commit()?;
    Ok(())
}

fn list_dat_records(conn: &Connection, dat_id: db::DatId) -> Result<()> {
    let dat_record = db::get_dat(conn, dat_id)?;
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

fn find_sets_by_name(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
    let sets = if let Some(name) = name {
        db::get_sets_by_name(conn, dat_id, name, false)
    } else {
        db::get_sets(conn, dat_id)
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

fn find_roms(conn: &Connection, dat_id: db::DatId, name: Option<&str>) -> Result<()> {
    let roms = if let Some(name) = name {
        db::get_roms_by_name(conn, dat_id, name, false)
    } else {
        db::get_roms(conn, dat_id)
    }?;
    if roms.is_empty() {
        println!("No roms found.");
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
    term: &TermInfo,
    scan_path: &Utf8Path, //expect this to be canonicalized
    exclude: &[String],
    recursive: bool,
    incremental: bool,
) -> Result<()> {
    let mut tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let mut file_count = 0;
    scan_directory(&mut tx, dat_id, term, scan_path, exclude, recursive, incremental, None, &mut file_count)?;

    tx.commit()?;

    if term.tty_out {
        println!("{ANSI_CURSOR_START}{} files scanned.{ANSI_ERASE_TO_END}", file_count);
    } else {
        println!("{} files scanned.", file_count);
    }
    Ok(())
}

const ANSI_CURSOR_START: &str = "\x1B[1000D";
const ANSI_ERASE_TO_END: &str = "\x1B[K";

fn scan_directory(
    tx: &mut Transaction,
    dat_id: db::DatId,
    term: &TermInfo,
    scan_path: &Utf8Path,
    exclude: &[String],
    recursive: bool,
    incremental: bool,
    parent_id: Option<db::DirId>,
    file_count: &mut u64,
) -> Result<()> {
    let (dir_id, incremental) = match db::get_directory_by_path(tx, dat_id, scan_path.as_str())? {
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
            scan_directory(tx, dat_id, term, path, exclude, recursive, incremental, Some(dir_id), file_count)?;
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
                match scan_zip_file(&sp, dat_id, path, incremental, dir_id) {
                    Ok(files_scanned) => {
                        sp.commit()?;

                        *file_count += files_scanned;
                        existing_paths.remove(path.as_str());
                    }
                    Err(e) => {
                        sp.rollback()?;

                        eprintln!("Failed to scan {}. Error: {e}", path);
                    }
                }
            } else {
                match path.file_name().context("Could not get filename") {
                    Ok(filename) => {
                        *file_count += 1;
                        let exists = existing_names.remove(filename);
                        if exists && incremental {
                            //there was an existing scanned file, so skip it
                        } else if let Err(e) = scan_file(tx, dat_id, dir_id, path, filename) {
                            eprintln!("Failed to scan {}. Error: {e}", path);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to scan {}. Error: {e}", path);
                    }
                }
            }
        }
        if term.tty_out {
            print!("{ANSI_CURSOR_START}{} files scanned.{ANSI_ERASE_TO_END}", file_count);
            std::io::stdout().flush()?;
        }
    }

    for existing_path in existing_paths {
        match db::get_directory_by_path(tx, dat_id, existing_path) {
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
) -> Result<u64> {
    let maybe_dir = db::get_directory_by_path(conn, dat_id, path.as_str())?;
    if incremental && maybe_dir.is_some() {
        //if incremental and we have scanned this zip file before, skip it
        return Ok(0);
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
    Ok(zip.len() as u64)
}

fn match_sets<P: AsRef<Utf8Path>>(conn: &Connection, dat_id: db::DatId, path: P) -> Result<BTreeSet<db::SetId>> {
    let name = path.as_ref().file_prefix().context("should have a file name")?;
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
        // Step 2a: if nothing named the same, check for hash matches,
        // and match accordingly, otherwise return no match
        match_hashes(conn, dat_id, hash, || Match::None)
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

        // Step 3b: if something is named the same, but the hash doesn't match,
        // check whether we got hash only matches, and if so, then treat it as a hash match,
        // otherwise return the name only matches
        match_hashes(conn, dat_id, hash, || {
            let matches = named_roms
                .iter()
                .filter(|rom| matched_sets.is_empty() || matched_sets.contains(&rom.set_id))
                .map(|rom| db::MatchStatus::Name {
                    set_id: rom.set_id,
                    rom_id: rom.id,
                })
                .collect();
            Match::Partial(matches)
        })
    }
}

fn match_hashes<F>(conn: &Connection, dat_id: db::DatId, hash: &str, no_match_fn: F) -> Result<Match>
where
    F: FnOnce() -> Match,
{
    let hash_roms = db::get_roms_by_hash(conn, dat_id, hash)?;
    let matched = if hash_roms.is_empty() {
        no_match_fn()
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

fn should_display_file_status(status: &db::MatchStatus, mode: ListMode) -> bool {
    matches!(
        (status, mode),
        (db::MatchStatus::None, ListMode::Unmatched | ListMode::All)
            | (db::MatchStatus::Hash { .. }, ListMode::Warning | ListMode::All)
            | (db::MatchStatus::Name { .. }, ListMode::Warning | ListMode::All)
            | (db::MatchStatus::Match { .. }, ListMode::Matched | ListMode::All)
    )
}

fn format_file_indicator(status: &db::MatchStatus, is_tty: bool) -> &str {
    match status {
        db::MatchStatus::None => {
            if is_tty {
                "❌"
            } else {
                "NONE"
            }
        }
        db::MatchStatus::Hash { .. } | db::MatchStatus::Name { .. } => {
            if is_tty {
                "⚠️"
            } else {
                "WARN"
            }
        }
        db::MatchStatus::Match { .. } => {
            if is_tty {
                "✅"
            } else {
                " OK "
            }
        }
    }
}

fn format_file_status(conn: &Connection, file: &db::FileRecord, is_tty: bool) -> Result<String> {
    let indicator = format_file_indicator(&file.status, is_tty);
    let result = match file.status {
        db::MatchStatus::None => {
            format!("[{indicator}] {} {} - unknown file", file.hash, file.name)
        }
        db::MatchStatus::Hash { rom_id, .. } => {
            let rom = db::get_rom(conn, rom_id)?;
            format!("[{indicator}] {} {} - incorrect name, should be named {}", file.hash, file.name, rom.name)
        }
        db::MatchStatus::Name { rom_id, .. } => {
            let rom = db::get_rom(conn, rom_id)?;
            format!(
                "[{indicator}] {} {} - incorrect hash, should have hash {}",
                file.hash, file.name, rom.hash
            )
        }
        db::MatchStatus::Match { .. } => {
            format!("[{indicator}] {} {}", file.hash, file.name)
        }
    };
    Ok(result)
}

fn list_files(
    conn: &mut Connection,
    dat_id: db::DatId,
    term: &TermInfo,
    mode: ListMode,
    partial_name: Option<&str>,
) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id, None)?;
    for dir in dirs {
        let files = db::get_files(conn, dir.id, partial_name)?;

        if files.is_empty() {
            continue;
        }

        let mut lines = Vec::new();
        for file in files {
            if should_display_file_status(&file.status, mode) {
                lines.push(format_file_status(conn, &file, term.tty_out)?);
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

enum SetStatus {
    Missing,
    Partial,
    Complete,
}

fn format_set_indicator(status: &SetStatus, is_tty: bool) -> &str {
    match status {
        SetStatus::Missing => {
            if is_tty {
                "❌"
            } else {
                "NONE"
            }
        }
        SetStatus::Partial => {
            if is_tty {
                "⚠️"
            } else {
                "WARN"
            }
        }
        SetStatus::Complete => {
            if is_tty {
                "✅"
            } else {
                " OK "
            }
        }
    }
}

fn list_sets(
    conn: &mut Connection,
    dat_id: db::DatId,
    term: &TermInfo,
    missing: bool,
    partial_name: Option<&str>,
) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id, None)?;

    let mut sets_to_files: BTreeMap<db::SetId, Vec<db::FileRecord>> = BTreeMap::new();
    let mut found_roms: BTreeSet<db::RomId> = BTreeSet::new();

    for dir in dirs {
        let files = db::get_files(conn, dir.id, None)?;
        for file in files {
            if let Some((set_id, rom_id)) = file.status.ids() {
                sets_to_files.entry(set_id).or_default().push(file);
                found_roms.insert(rom_id);
            }
        }
    }

    let sets = db::get_sets(conn, dat_id)?;
    if missing {
        println!("--- MISSING SETS ---");
        let status = format_set_indicator(&SetStatus::Missing, term.tty_out);
        for set in &sets {
            if let Some(partial_name) = partial_name
                && !set
                    .name
                    .to_ascii_lowercase()
                    .contains(&partial_name.to_ascii_lowercase())
            {
                continue;
            }
            println_if!(!sets_to_files.contains_key(&set.id), "[{status}] {}", set.name);
        }
        println!("{} / {} sets missing.", sets.len() - sets_to_files.len(), sets.len());
    } else {
        println!("--- FOUND SETS ---");
        let partial_status = format_set_indicator(&SetStatus::Partial, term.tty_out);
        let complete_status = format_set_indicator(&SetStatus::Complete, term.tty_out);
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
                    println!("[{complete_status}] {}", set.name);
                } else {
                    println!("[{partial_status}] {}, set has missing roms", set.name);
                }
                for file in files {
                    let indicator = format_file_indicator(&file.status, term.tty_out);
                    match file.status {
                        db::MatchStatus::Hash { set_id: _, rom_id } => {
                            println!(
                                " {indicator}  {} {}, should be named {}",
                                file.hash, file.name, roms_by_id[&rom_id].name
                            );
                        }
                        db::MatchStatus::Name { set_id: _, rom_id } => {
                            println!(
                                "  {indicator}  {} {}, should have hash {}",
                                file.hash, file.name, roms_by_id[&rom_id].hash
                            );
                        }
                        db::MatchStatus::Match { set_id: _, rom_id: _ } => {
                            println!(" {indicator}  {} {}", file.hash, file.name);
                        }
                        db::MatchStatus::None => unreachable!(),
                    }
                }
                let indicator = format_file_indicator(&db::MatchStatus::None, term.tty_out);
                for rom in roms {
                    println_if!(!found_roms.contains(&rom.id), " {indicator}  {} {} missing", rom.hash, rom.name);
                }
            }
        }
        println!("{} / {} sets found.", sets_to_files.len(), sets.len());
    }
    Ok(())
}

fn rename_files(conn: &mut Connection, dat_id: db::DatId, term: &TermInfo) -> Result<()> {
    let mut tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    for directory in db::get_directories(&tx, dat_id, None)? {
        if Utf8Path::new(&directory.path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
            continue;
        }

        let dir_files = db::get_files(&tx, directory.id, None)?;

        let mut matches_by_name = BTreeMap::new();
        for file in &dir_files {
            matches_by_name.entry(&file.name).or_insert(Vec::new()).push(file);
        }

        let path = Utf8PathBuf::from(directory.path);
        for (name, records) in matches_by_name {
            if records.len() == 1 {
                let file = records[0];

                if let db::MatchStatus::Hash { set_id, rom_id } = file.status {
                    //single match and its a hash, so attempt to rename
                    let rom = db::get_rom(&tx, rom_id)?;

                    let old_path = path.join(name);
                    let new_path = path.join(&rom.name);
                    let mut sp = tx.savepoint()?;
                    let new_status = db::MatchStatus::Match { set_id, rom_id };
                    if let Err(e) = db::update_file(&sp, file.id, &rom.name, new_status) {
                        eprintln!("Failed to rename {old_path}. Error was {e}");
                        sp.rollback()?;
                    } else {
                        match std::fs::rename(&old_path, &new_path) {
                            Ok(_) => {
                                let indicator = format_file_indicator(&new_status, term.tty_out);
                                println!("[{indicator}] {} {} -> {}", file.hash, file.name, rom.name);
                                sp.commit()?;
                            }
                            Err(e) => {
                                eprintln!("Failed to rename {old_path}. Error was {e}");
                                sp.rollback()?;
                            }
                        }
                    }
                }
            }
        }
    }

    tx.commit()?;
    Ok(())
}
