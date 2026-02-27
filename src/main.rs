mod db;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, IsTerminal, Write};

use anyhow::{Context, Result, anyhow, bail, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand, ValueEnum};
use roxmltree::{Document, ParsingOptions};
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::db::{Deletable, DeletableByDat, FindableByName, Insertable, Queryable, QueryableByDat};

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

    /// command to execute, if none given will enter interactive mode
    #[command(subcommand)]
    command: Option<Commands>,

    /// force enter interactive mode, if command is given
    #[arg(short, long)]
    interactive: bool,
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
    /// Alias for `data select`
    Select {
        /// the index of the dat file to select, as seen in list
        index: usize,
    },
    /// exit from interactive mode
    Exit,
}

#[derive(Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ListMode {
    /// list all files
    All,
    /// list only matched files
    Matched,
    /// list only misnamed or bad dumps
    Warning,
    /// list only unmatched files
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
    /// alias for `list --mode matched`
    Matched {
        /// show only files partially matching this name
        partial_name: Option<String>,
    },
    /// alias for `sets --missing`
    Missing {
        /// show only sets partially matching this name
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
    /// alias for `list --mode unmatched`
    Unmatched {
        /// show only files partially matching this name
        partial_name: Option<String>,
    },
    /// alias for `list --mode warning`
    Warning {
        /// show only files partially matching this name
        partial_name: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum DataCommands {
    /// import a dat file into the system and make it the current dat file
    Import {
        /// the path and filename of the dat file to import
        #[arg(value_hint = clap::ValueHint::FilePath)]
        dat_file: Utf8PathBuf,
    },
    /// update the current dat file with a new version and re-match files
    Update {
        /// the path and filename of the dat file to import
        #[arg(value_hint = clap::ValueHint::FilePath)]
        dat_file: Utf8PathBuf,

        /// don't ask for confirmation, and perform the action
        #[arg(long)]
        yes: bool,
    },
    /// remove the current dat file and all matched files
    Remove {
        /// don't ask for confirmation, and perform the action
        #[arg(long)]
        yes: bool,
    },
    /// List dat files in the system
    List,
    /// Select the current dat file
    Select {
        /// the index of the dat file to select, as seen in list
        index: usize,
    },
    /// Show all Set and Roms in the current dat file
    Records,
    /// Search for a Set in the current dat file
    Sets {
        /// an optional partial name to match
        partial_name: Option<String>,
    },
    /// Search for a Rom in the current dat file
    Roms {
        /// an optional partial name to match
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
            .and_then(|path| path.canonicalize())
            .ok()
            .and_then(|path| Utf8PathBuf::try_from(path).ok())
        {
            let paths = db::DirRecord::get_by_path(&conn, current_path.as_str())?;
            if !paths.is_empty() {
                let dat = db::DatRecord::get_by_id(&conn, &paths[0].dat_id)?;
                println!("dat file `{}` selected.", dat.name);
                dat_id = Some(dat.id);
            } else {
                eprintln!("No default dat file for current path.");
            }
        } else {
            eprintln!("Invalid current path, no default dat file for current path.");
        }
    }

    let interactive = if let Some(command) = args.command {
        do_command(&mut conn, &mut dat_id, &command, &term)?;
        args.interactive
    } else {
        true
    };

    if interactive && term.tty_in {
        loop {
            let line = readline()?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(args) = shlex::split(line) {
                match Cli::try_parse_from(args) {
                    Ok(cli) => match do_command(&mut conn, &mut dat_id, &cli.command, &term) {
                        Ok(exit) => {
                            if exit {
                                break;
                            }
                        }
                        Err(e) => eprintln!("Unable to perform command, {e}"),
                    },
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
        Commands::Data { data } => {
            handle_data_commands(conn, dat_id, term, data)?;
            Ok(false)
        }
        Commands::Files { files } => {
            handle_file_commands(conn, dat_id.as_ref(), term, files)?;
            Ok(false)
        }
        Commands::Select { index } => {
            handle_data_commands(conn, dat_id, term, &DataCommands::Select { index: *index })?;
            Ok(false)
        }
        Commands::Exit => Ok(true),
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

            import_dat(conn, dat_file).map(|imported| {
                println!("dat file `{}` imported and selected.", imported.name);
                *dat_id = Some(imported.id);
            })
        }
        DataCommands::Update { dat_file, yes } => {
            ensure!(dat_id.is_some(), "No dat file selected");

            if ask_for_confirmation(term, "Are you sure you want to update the current dat file? (y/N): ", *yes)? {
                let old_dat_id = dat_id.take().expect("Option should contain data");
                update_dat(conn, dat_file, old_dat_id).map(|imported| {
                    println!("dat file `{}` imported and updated.", imported.name);
                    *dat_id = Some(imported.id);
                })?;
            }
            Ok(())
        }
        DataCommands::Remove { yes } => {
            ensure!(dat_id.is_some(), "No dat file selected");

            if ask_for_confirmation(term, "Are you sure you want to remove the current dat file? (y/N): ", *yes)? {
                let old_dat_id = dat_id.take().expect("Option should contain data");
                delete_dat(conn, old_dat_id).map(|_| {
                    println!("dat file removed.");
                    *dat_id = None;
                })?;
            }
            Ok(())
        }
        DataCommands::List => list_dat_files(conn),
        DataCommands::Select { index } => db::DatRecord::get_all(conn).and_then(|dats| {
            let dat = dats.get(*index).ok_or_else(|| anyhow!("Invalid dat file selection."))?;
            println!("dat file `{}` selected.", dat.name);
            *dat_id = Some(dat.id.clone());
            Ok(())
        }),
        DataCommands::Records => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_dat_records(conn, dat_id)
        }
        DataCommands::Sets { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            find_sets_by_name(conn, dat_id, partial_name.as_deref())
        }
        DataCommands::Roms { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            find_roms(conn, dat_id, partial_name.as_deref())
        }
    }
}

fn ask_for_confirmation(term: &TermInfo, prompt: &str, skip: bool) -> Result<bool> {
    if !skip && term.tty_in {
        print!("{prompt}");
        std::io::stdout().flush()?;
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer)?;
        let buffer = buffer.trim();
        Ok(buffer.eq_ignore_ascii_case("y"))
    } else {
        Ok(skip)
    }
}

fn handle_file_commands(
    conn: &mut Connection,
    dat_id: Option<&db::DatId>,
    term: &TermInfo,
    files: &FileCommands,
) -> Result<()> {
    let dat_id = dat_id.ok_or_else(|| anyhow!("No dat file selected"))?;

    match files {
        FileCommands::Scan {
            exclude,
            recursive,
            full,
            path,
        } => {
            //make sure path is resolved to something absolute and proper before scanning
            let scan_path = path.canonicalize_utf8()?;
            ensure!(scan_path.is_dir(), "`{}` is not a valid directory", scan_path);
            scan_files(conn, dat_id, term, &scan_path, exclude, *recursive, !full)
        }
        FileCommands::List { mode, partial_name } => list_files(conn, dat_id, term, mode, partial_name.as_deref()),
        FileCommands::Sets { missing, partial_name } => {
            list_sets(conn, dat_id, term, *missing, partial_name.as_deref())
        }
        FileCommands::Rename => rename_files(conn, dat_id, term),
        FileCommands::Matched { partial_name } => {
            list_files(conn, dat_id, term, &ListMode::Matched, partial_name.as_deref())
        }
        FileCommands::Missing { partial_name } => list_sets(conn, dat_id, term, true, partial_name.as_deref()),
        FileCommands::Unmatched { partial_name } => {
            list_files(conn, dat_id, term, &ListMode::Unmatched, partial_name.as_deref())
        }
        FileCommands::Warning { partial_name } => {
            list_files(conn, dat_id, term, &ListMode::Warning, partial_name.as_deref())
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

fn update_dat(conn: &mut Connection, dat_file: &Utf8PathBuf, old_dat_id: db::DatId) -> Result<db::DatRecord> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    let imported = parse_dat_file(&tx, dat_file)?;

    //delete all existing matches for the old dat, we'll re-match them as we relink directories and files to the new dat
    db::MatchRecord::delete_by_dat(&tx, &old_dat_id)?;

    for directory in db::DirRecord::get_by_dat(&tx, &old_dat_id)? {
        //check if its a zip file, if so, restrict matches to set name if matched
        if Utf8Path::new(&directory.path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
            let matched_sets = match_sets(&tx, &imported.id, &directory.path)?;
            for file in directory.get_files(&tx)? {
                //rematch using existing information, but link to the new dat
                insert_matches(&tx, &imported.id, &file, &matched_sets)?;
            }
        }
    }

    //relink all directories to the new dat
    db::DirRecord::relink_dirs(&tx, &old_dat_id, &imported.id)?;

    tx.commit()?;

    //if we successfully updated everything and relinked and the transaction completed, we can now delete the old dat
    delete_dat(conn, old_dat_id)?;

    Ok(imported)
}

fn import_dat<P: AsRef<Utf8Path>>(conn: &mut Connection, file_path: P) -> Result<db::DatRecord> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let dat = parse_dat_file(&tx, file_path)?;
    tx.commit()?;
    Ok(dat)
}

fn parse_dat_file<P: AsRef<Utf8Path>>(conn: &Connection, file_path: P) -> Result<db::DatRecord> {
    let df_buffer = std::fs::read_to_string(file_path.as_ref()).context("Unable to read reference dat file")?;
    let df_xml = Document::parse_with_options(
        df_buffer.as_str(),
        ParsingOptions {
            allow_dtd: true,
            ..Default::default()
        },
    )
    .context("Unable to parse reference dat file")?;
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
    let new_dat = db::NewDat {
        name: name.context("unable to find name attribute in header")?.to_string(),
        description: description
            .context("unable to find description attribute in header")?
            .to_string(),
        version: version
            .context("unable to find version attribute in header")?
            .to_string(),
        author: author.context("unable to find author attribute in header")?.to_string(),
        hash_type: "sha1".to_string(),
    };
    let dat = db::DatRecord::insert(conn, &new_dat)?;
    for game_node in df_xml
        .root_element()
        .children()
        .filter(|node| node.tag_name().name() == TAG_GAME)
    {
        let game_name = game_node
            .attribute(ATTR_GAME_NAME)
            .context("Unable to read game name in reference dat file")?;

        let set = db::SetRecord::insert(
            conn,
            &db::NewSet {
                dat_id: dat.id.clone(),
                name: game_name.to_string(),
            },
        )?;

        for rom_node in game_node.descendants().filter(|node| node.tag_name().name() == TAG_ROM) {
            let rom_name = rom_node.attribute(ATTR_ROM_NAME).context("Unable to read game name")?;
            let rom_size = rom_node.attribute(ATTR_ROM_SIZE).context("Unable to read game size")?;
            let rom_hash = rom_node.attribute(ATTR_ROM_HASH).context("Unable to read game hash")?;
            db::RomRecord::insert(
                conn,
                &db::NewRom {
                    dat_id: dat.id.clone(),
                    set_id: set.id.clone(),
                    name: rom_name.to_string(),
                    size: db::SizeWrapper(rom_size.parse().context("should be a valid number")?),
                    hash: rom_hash.to_string(),
                },
            )?;
        }
    }
    Ok(dat)
}

fn delete_dat(conn: &mut Connection, dat_id: db::DatId) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;

    //remove all scanned files and directories
    for dir in db::DirRecord::get_by_dat(&tx, &dat_id)? {
        dir.delete_files(&tx)?;
    }
    db::DirRecord::delete_by_dat(&tx, &dat_id)?;

    //remove all roms and sets before removing the dat
    db::RomRecord::delete_by_dat(&tx, &dat_id)?;
    db::SetRecord::delete_by_dat(&tx, &dat_id)?;

    db::DatRecord::delete_by_id(&tx, &dat_id)?;

    tx.commit()?;
    Ok(())
}

fn list_dat_records(conn: &Connection, dat_id: &db::DatId) -> Result<()> {
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

fn find_sets_by_name(conn: &Connection, dat_id: &db::DatId, name: Option<&str>) -> Result<()> {
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

fn find_roms(conn: &Connection, dat_id: &db::DatId, name: Option<&str>) -> Result<()> {
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
            if let Some(set) = sets_by_id.get(&set_id) {
                println!("{}", set.name);
                for rom in roms {
                    println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size));
                }
            }
        }
    }
    Ok(())
}

fn scan_files(
    conn: &mut Connection,
    dat_id: &db::DatId,
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
        println!("{ANSI_CURSOR_START}{} new files scanned.{ANSI_ERASE_TO_END}", file_count);
    } else {
        println!("{} new files scanned.", file_count);
    }
    Ok(())
}

const ANSI_CURSOR_START: &str = "\x1B[1000D";
const ANSI_ERASE_TO_END: &str = "\x1B[K";

#[allow(clippy::too_many_arguments)]
fn scan_directory(
    tx: &mut Transaction,
    dat_id: &db::DatId,
    term: &TermInfo,
    scan_path: &Utf8Path,
    exclude: &[String],
    recursive: bool,
    incremental: bool,
    parent_id: Option<&db::DirId>,
    file_count: &mut u64,
) -> Result<()> {
    let (dir, incremental) = match db::DirRecord::get_by_dat_path(tx, dat_id, scan_path.as_str())? {
        Some(dir) => {
            if incremental {
                // add on to existing records
                (dir, true)
            } else {
                //wipe existing file records and do full scan
                let _ = dir.delete_files(tx)?;
                (dir, false)
            }
        }
        None => {
            //no existing records, do a full scan
            let dir = db::DirRecord::insert(
                tx,
                &db::NewDir {
                    dat_id: dat_id.clone(),
                    path: scan_path.to_string(),
                    parent_id: parent_id.cloned(),
                },
            )?;
            (dir, false)
        }
    };

    let existing_dirs = dir.get_children(tx)?;
    let mut existing_paths: BTreeSet<&str> = existing_dirs.iter().map(|dir| dir.path.as_str()).collect();
    let existing_files = dir.get_files(tx)?;
    //there may be multiple matches per filename as the hash might match multiple roms
    let mut existing_files_by_name: BTreeMap<_, Vec<_>> = BTreeMap::new();
    existing_files
        .iter()
        .for_each(|file| existing_files_by_name.entry(file.name.as_str()).or_default().push(file));

    for entry in scan_path.read_dir_utf8()? {
        let entry = entry?;
        let path = entry.path();
        if util::is_hidden_file(path) {
            //skip
        } else if recursive && path.is_dir() {
            scan_directory(tx, dat_id, term, path, exclude, recursive, incremental, Some(&dir.id), file_count)?;
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
                match scan_zip_file(&sp, dat_id, path, incremental, exclude, &dir.id) {
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
                        let exists = existing_files_by_name.remove(filename).is_some();
                        if exists && incremental {
                            //there was an existing scanned file, so skip it
                            continue;
                        }

                        if let Err(e) = scan_file(tx, dat_id, &dir.id, path, filename) {
                            eprintln!("Failed to scan {}. Error: {e}", path);
                        } else {
                            *file_count += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to scan {}. Error: {e}", path);
                    }
                }
            }
        }
        if term.tty_out {
            print!("{ANSI_CURSOR_START}{} new files scanned.{ANSI_ERASE_TO_END}", file_count);
            std::io::stdout().flush()?;
        }
    }

    for existing_path in existing_paths {
        if incremental && Utf8Path::new(&existing_path).is_dir() {
            //if its an incremental scan and the directory still exists, don't delete
            //the directory as they may have missed the recursive flag and we don't
            //want to delete data unnecessarily
            continue;
        }
        match db::DirRecord::get_by_dat_path(tx, dat_id, existing_path) {
            Ok(dir) => {
                if let Some(dir) = dir {
                    if let Err(e) = dir.delete_files(tx) {
                        eprintln!("Failed to delete files in {}. Error: {e}", existing_path);
                    }
                    if let Err(e) = db::DirRecord::delete_by_id(tx, &dir.id) {
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
    for (_, existing_files) in existing_files_by_name {
        for existing_file in existing_files {
            if let Err(e) = db::FileRecord::delete_by_id(tx, &existing_file.id) {
                eprintln!("Failed to remove {}. Error: {e}", existing_file.name);
            }
        }
    }
    Ok(())
}

fn scan_zip_file(
    conn: &Connection,
    dat_id: &db::DatId,
    path: &Utf8Path,
    incremental: bool,
    exclude: &[String],
    parent_id: &db::DirId,
) -> Result<u64> {
    let maybe_dir = db::DirRecord::get_by_dat_path(conn, dat_id, path.as_str())?;
    if incremental && maybe_dir.is_some() {
        //if incremental and we have scanned this zip file before, skip it
        return Ok(0);
    }

    let dir_id = match maybe_dir {
        Some(dir) => {
            //wipe existing file records and do full scan
            let _ = dir.delete_files(conn)?;
            dir.id
        }
        None => {
            //no existing records, do a full scan
            let dir = db::DirRecord::insert(
                conn,
                &db::NewDir {
                    dat_id: dat_id.clone(),
                    path: path.to_string(),
                    parent_id: Some(parent_id.clone()),
                },
            )?;
            dir.id
        }
    };

    let matched = match_sets(conn, dat_id, path)?;

    let file = File::open(path)?;
    let mut zip = zip::ZipArchive::new(file).with_context(|| format!("could not open '{}' as a zip file", path))?;
    let mut file_count = 0u64;
    for i in 0..zip.len() {
        match zip.by_index(i) {
            Ok(mut inner_file) => {
                if inner_file.is_file() {
                    if Utf8Path::new(inner_file.name())
                        .extension()
                        .map(|ext| exclude.iter().any(|e| ext.eq_ignore_ascii_case(e)))
                        .unwrap_or_default()
                    {
                        continue;
                    }

                    file_count += 1;
                    let (hash, file_size) = util::calc_hash(&mut inner_file)?;
                    insert_files_and_matches(conn, dat_id, &dir_id, inner_file.name(), file_size, &hash, &matched)?;
                }
            }
            Err(error) => bail!("{}", error),
        }
    }

    //we could be smarter here and try to infer the largest set matched
    //and assume that the set is supposed to be that if no set was matched.

    Ok(file_count)
}

fn match_sets<P: AsRef<Utf8Path>>(conn: &Connection, dat_id: &db::DatId, path: P) -> Result<BTreeSet<db::SetId>> {
    let name = path.as_ref().file_prefix().context("should have a file name")?;
    let sets = db::SetRecord::find_by_name(conn, dat_id, name, true)?;
    let matched: BTreeSet<db::SetId> = sets.iter().map(|record| record.id.clone()).collect();
    Ok(matched)
}

fn scan_file(conn: &Connection, dat_id: &db::DatId, dir_id: &db::DirId, path: &Utf8Path, filename: &str) -> Result<()> {
    //scan the file,find a match and insert
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    let mut reader = BufReader::new(&file);
    let (hash, _) = util::calc_hash(&mut reader)?;

    insert_files_and_matches(conn, dat_id, dir_id, filename, file_size, &hash, &BTreeSet::new())?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileMatch {
    pub status: db::MatchStatus,
    pub set_id: db::SetId,
    pub rom_id: db::RomId,
}

fn match_roms(
    conn: &Connection,
    dat_id: &db::DatId,
    filename: &str,
    file_size: u64,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<Option<Vec<FileMatch>>> {
    // Step 1: is there any roms called the same as the filename?
    let named_roms = db::RomRecord::find_by_name(conn, dat_id, filename, true)?;
    if !named_roms.is_empty() {
        //Step 2: if something is named the same, check for exact matches with those items, and return if so.
        let exact_matches = match_exact(file_size, hash, matched_sets, &named_roms);
        if exact_matches.is_some() {
            return Ok(exact_matches);
        }
    }
    // Step 3: if something is named the same, but the hash doesn't match,
    // check whether we got hash only matches if we ignore the filename.
    // If so, then treat it as a hash match, otherwise return the name only matches,
    // if there are any.
    let hash_roms = db::RomRecord::get_by_hash(conn, dat_id, hash)?;
    if hash_roms.is_empty() {
        Ok(match_names(matched_sets, &named_roms))
    } else {
        Ok(match_hashes(matched_sets, &hash_roms))
    }
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
            set_id: rom.set_id.clone(),
            rom_id: rom.id.clone(),
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
            set_id: rom.set_id.clone(),
            rom_id: rom.id.clone(),
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
            set_id: rom.set_id.clone(),
            rom_id: rom.id.clone(),
        })
        .collect();
    if matches.is_empty() { None } else { Some(matches) }
}

fn insert_files_and_matches(
    conn: &Connection,
    dat_id: &db::DatId,
    dir_id: &db::DirId,
    file_name: &str,
    file_size: u64,
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<()> {
    let file = db::FileRecord::insert(
        conn,
        &db::NewFile {
            dat_id: dat_id.clone(),
            dir_id: dir_id.clone(),
            name: file_name.to_string(),
            size: db::SizeWrapper(file_size),
            hash: hash.to_string(),
        },
    )?;

    insert_matches(conn, dat_id, &file, matched_sets)
}

fn insert_matches(
    conn: &Connection,
    dat_id: &db::DatId,
    file: &db::FileRecord,
    matched_sets: &BTreeSet<db::Id<db::SetRecord>>,
) -> std::result::Result<(), anyhow::Error> {
    let matched = match_roms(conn, dat_id, &file.name, file.size, &file.hash, matched_sets)?;
    if let Some(items) = matched {
        for item in items {
            db::MatchRecord::insert(
                conn,
                &db::NewMatch {
                    dat_id: dat_id.clone(),
                    file_id: file.id.clone(),
                    status: item.status,
                    set_id: item.set_id,
                    rom_id: item.rom_id,
                },
            )?;
        }
    }
    Ok(())
}

fn should_display_file_status(status: Option<&db::MatchStatus>, mode: &ListMode) -> bool {
    matches!(
        (status, mode),
        (None, ListMode::Unmatched | ListMode::All)
            | (Some(db::MatchStatus::Hash), ListMode::Warning | ListMode::All)
            | (Some(db::MatchStatus::Name), ListMode::Warning | ListMode::All)
            | (Some(db::MatchStatus::Match), ListMode::Matched | ListMode::All)
    )
}

fn format_file_indicator(status: Option<&db::MatchStatus>, is_tty: bool) -> &str {
    match status {
        None => {
            if is_tty {
                "❌"
            } else {
                "NONE"
            }
        }
        Some(db::MatchStatus::Hash) | Some(db::MatchStatus::Name) => {
            if is_tty {
                "⚠️"
            } else {
                "WARN"
            }
        }
        Some(db::MatchStatus::Match) => {
            if is_tty {
                "✅"
            } else {
                " OK "
            }
        }
    }
}

fn format_file_status(
    conn: &Connection,
    file: &db::FileRecord,
    matched: Option<&db::MatchRecord>,
    is_tty: bool,
) -> Result<String> {
    let indicator = format_file_indicator(matched.map(|m| &m.status), is_tty);
    let result = match matched {
        None => {
            format!("[{indicator}] {} {} - unknown file", file.hash, file.name)
        }
        Some(m) => match m.status {
            db::MatchStatus::Hash => {
                let rom = db::RomRecord::get_by_id(conn, &m.rom_id)?;
                format!("[{indicator}] {} {} - incorrect name, should be named {}", file.hash, file.name, rom.name)
            }
            db::MatchStatus::Name => {
                let rom = db::RomRecord::get_by_id(conn, &m.rom_id)?;
                format!("[{indicator}] {} {} - incorrect hash, should have hash {}", file.hash, file.name, rom.hash)
            }
            db::MatchStatus::Match => {
                format!("[{indicator}] {} {}", file.hash, file.name)
            }
        },
    };
    Ok(result)
}

fn list_files(
    conn: &mut Connection,
    dat_id: &db::DatId,
    term: &TermInfo,
    mode: &ListMode,
    partial_name: Option<&str>,
) -> Result<()> {
    //get these in bulk to avoid doing a query per file when we display them
    let matches = db::MatchRecord::get_by_dat(conn, dat_id)?;
    let matches_by_file: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.file_id).or_default().push(m);
        acc
    });

    let dirs = db::DirRecord::get_by_dat(conn, dat_id)?;
    for dir in dirs {
        let files = if let Some(partial_name) = partial_name {
            dir.find_files(conn, partial_name, false)?
        } else {
            dir.get_files(conn)?
        };

        if files.is_empty() {
            continue;
        }

        let mut lines = Vec::new();
        for file in files {
            if let Some(file_matches) = matches_by_file.get(&file.id) {
                for fm in file_matches {
                    if should_display_file_status(Some(&fm.status), mode) {
                        lines.push(format_file_status(conn, &file, Some(fm), term.tty_out)?);
                    }
                }
            } else if should_display_file_status(None, mode) {
                lines.push(format_file_status(conn, &file, None, term.tty_out)?);
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
    dat_id: &db::DatId,
    term: &TermInfo,
    missing: bool,
    partial_name: Option<&str>,
) -> Result<()> {
    //get these in bulk to avoid doing a query per file when we display them
    let matches = db::MatchRecord::get_by_dat(conn, dat_id)?;
    let matches_by_file: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.file_id).or_default().push(m);
        acc
    });

    let all_files = db::FileRecord::get_by_dat(conn, dat_id)?;

    let mut sets_to_files: BTreeMap<_, Vec<_>> = BTreeMap::new();
    let mut found_roms: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();

    for file in &all_files {
        if let Some(file_matches) = matches_by_file.get(&file.id) {
            for fm in file_matches {
                //FIXME: avoid cloning if possible
                sets_to_files.entry(fm.set_id.clone()).or_default().push((file, *fm));
                found_roms
                    .entry(fm.set_id.clone())
                    .or_default()
                    .insert(fm.rom_id.clone());
            }
        }
    }

    let all_sets = db::SetRecord::get_by_dat(conn, dat_id)?;
    if missing {
        println!("--- MISSING SETS ---");
        let status = format_set_indicator(&SetStatus::Missing, term.tty_out);
        for set in &all_sets {
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
        println!("{} / {} sets missing.", all_sets.len() - sets_to_files.len(), all_sets.len());
    } else {
        let all_roms = db::RomRecord::get_by_dat(conn, dat_id)?;
        let mut roms_by_set: BTreeMap<_, Vec<_>> = BTreeMap::new();
        all_roms
            .iter()
            .for_each(|rom| roms_by_set.entry(&rom.set_id).or_default().push(rom));

        println!("--- FOUND SETS ---");
        let partial_status = format_set_indicator(&SetStatus::Partial, term.tty_out);
        let complete_status = format_set_indicator(&SetStatus::Complete, term.tty_out);
        for set in &all_sets {
            if let Some(partial_name) = partial_name
                && !set
                    .name
                    .to_ascii_lowercase()
                    .contains(&partial_name.to_ascii_lowercase())
            {
                continue;
            }

            if let Some(files) = sets_to_files.get(&set.id)
                && let Some(roms) = roms_by_set.get(&set.id)
            {
                //iterate through the file matches and check if we have all the roms in the set matched, if so its complete, otherwise its partial
                let roms_by_id: BTreeMap<_, _> = roms.iter().map(|&rom| (&rom.id, rom)).collect();
                if found_roms.get(&set.id).is_some_and(|s| s.len() >= roms.len()) {
                    //we found the same number (or more) of unique roms that are in the set
                    println!("[{complete_status}] {}", set.name);
                } else {
                    println!("[{partial_status}] {}, set has missing roms", set.name);
                }

                for (file, fm) in files {
                    let indicator = format_file_indicator(Some(&fm.status), term.tty_out);
                    match fm.status {
                        db::MatchStatus::Hash => {
                            println!(
                                " {indicator}  {} {}, should be named {}",
                                file.hash, file.name, roms_by_id[&fm.rom_id].name
                            );
                        }
                        db::MatchStatus::Name => {
                            println!(
                                "  {indicator}  {} {}, should have hash {}",
                                file.hash, file.name, roms_by_id[&fm.rom_id].hash
                            );
                        }
                        db::MatchStatus::Match => {
                            println!(" {indicator}  {} {}", file.hash, file.name);
                        }
                    }
                }

                let missing_indicator = format_file_indicator(None, term.tty_out);
                for rom in roms {
                    println_if!(
                        !found_roms.get(&set.id).is_some_and(|s| s.contains(&rom.id)),
                        " {missing_indicator}  {} {} missing",
                        rom.hash,
                        rom.name
                    );
                }
            }
        }
        println!("{} / {} sets found.", sets_to_files.len(), all_sets.len());
    }
    Ok(())
}

fn rename_files(conn: &mut Connection, dat_id: &db::DatId, term: &TermInfo) -> Result<()> {
    let mut tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    for directory in db::DirRecord::get_by_dat(&tx, dat_id)? {
        if Utf8Path::new(&directory.path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
            continue;
        }

        let files = directory.get_files(&tx)?;
        let mut matches_by_name = BTreeMap::new();
        for file in &files {
            let file_matches = db::MatchRecord::get_by_file_status(&tx, &file.id, "hash")?;
            if file_matches.len() != 1 {
                continue;
            }
            matches_by_name
                .entry(&file.name)
                .or_insert(Vec::new())
                .push((file, file_matches[0].clone()));
        }

        let path = Utf8PathBuf::from(directory.path);
        for (name, records) in matches_by_name {
            if records.len() == 1 {
                let (file, file_match) = &records[0];
                let rom = db::RomRecord::get_by_id(&tx, &file_match.rom_id)?;

                let mut sp = tx.savepoint()?;
                match file_match.update(&sp, &db::MatchStatus::Match) {
                    Ok(new_match) => {
                        let old_path = path.join(name);
                        let new_path = path.join(&rom.name);

                        match std::fs::rename(&old_path, &new_path) {
                            Ok(_) => {
                                let indicator = format_file_indicator(Some(&new_match.status), term.tty_out);
                                println!("[{indicator}] {} {} -> {}", file.hash, file.name, &rom.name);
                                sp.commit()?;
                            }
                            Err(e) => {
                                eprintln!("Failed to rename {old_path} to {new_path}. Error was {e}");
                                sp.rollback()?;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to rename {name} in database. Error was {e}");
                        sp.rollback()?;
                    }
                }
            }
        }
    }

    tx.commit()?;
    Ok(())
}
