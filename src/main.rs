mod db;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::usize;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use digest::Digest;
use roxmltree::{Document, ParsingOptions};
use rusqlite::{Connection, TransactionBehavior};
use sha1::Sha1;

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
    Scan {
        #[arg(long, value_delimiter = ',', default_value = "m3u,dat,txt")]
        exclude: Vec<String>,
        #[arg(short('R'), long, default_value_t = false)]
        recursive: bool,
        #[arg(long, default_value_t = false)]
        incremental: bool,
        #[arg(default_value=".", value_hint = clap::ValueHint::DirPath)]
        path: PathBuf,
    },
    List {
        #[arg(long, value_enum, default_value_t = ListMode::All)]
        mode: ListMode,
        partial_name: Option<String>,
    },
    Sets {
        #[arg(long, default_value_t = false)]
        missing: bool,
    },
}

#[derive(Debug, Subcommand)]
enum DataCommands {
    /// import a reference dat file into the system
    Import {
        #[arg(value_hint = clap::ValueHint::FilePath)]
        dat_file: PathBuf,
    },
    /// List reference dat files in the system
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

    let mut conn = db::open_or_create(&db_path)?;

    let mut dat_id = None;
    loop {
        let line = readline()?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let args = shlex::split(line).ok_or(anyhow!("error: Invalid quoting"))?;
        match Cli::try_parse_from(args) {
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
                                println!("`{}` is not a valid file", dat_file.display());
                            }
                        }
                        DataCommands::List => match db::get_dats(&mut conn) {
                            Ok(dats) => {
                                println!("Installed dat files:");
                                for (i, dat) in dats.iter().enumerate() {
                                    println!("[{i}] {} version: {} author: {}", dat.name, dat.version, dat.author);
                                }
                            }
                            Err(e) => println!("Failed to list dat files. {e}"),
                        },
                        DataCommands::Select { index } => match db::get_dats(&mut conn) {
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
                                match list_dat_records(&mut conn, dat_id) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to list dat file. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        DataCommands::Sets { name } => {
                            if let Some(dat_id) = dat_id {
                                match find_sets_by_name(&mut conn, dat_id, &name) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to find sets. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        DataCommands::Roms { name } => {
                            if let Some(dat_id) = dat_id {
                                match find_roms(&mut conn, dat_id, &name) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to find roms. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                    },
                    Commands::Files { files } => match files {
                        FileCommands::Scan {
                            exclude,
                            recursive,
                            incremental,
                            path,
                        } => {
                            if path.is_dir() {
                                if let Some(dat_id) = dat_id {
                                    match scan_files(&mut conn, dat_id, &path, &exclude, recursive, incremental) {
                                        Ok(_) => {
                                            let full_path = path.canonicalize();
                                            println!("Directory {} scanned.", full_path.ok().unwrap_or(path).display())
                                        }
                                        Err(e) => println!("Failed to scan directory. {e}"),
                                    }
                                } else {
                                    println!("No dat file selected");
                                }
                            } else {
                                let full_path = path.canonicalize();
                                println!("`{}` is not a valid directory", full_path.ok().unwrap_or(path).display());
                            }
                        }
                        FileCommands::List { mode, partial_name } => {
                            if let Some(dat_id) = dat_id {
                                match list_files(&mut conn, dat_id, mode, partial_name.as_deref()) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to list files. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                        FileCommands::Sets { missing } => {
                            if let Some(dat_id) = dat_id {
                                match list_sets(&mut conn, dat_id, missing) {
                                    Ok(_) => {}
                                    Err(e) => println!("Failed to list files. {e}"),
                                }
                            } else {
                                println!("No dat file selected");
                            }
                        }
                    },
                    Commands::Exit => return Ok(()),
                };
            }
            Err(e) => {
                e.print()?;
            }
        }
    }
}

fn import_dat<P: AsRef<Path>>(conn: &mut Connection, file_path: P) -> Result<db::DatRecord> {
    //TODO check whether name named item exists
    let df_buffer = std::fs::read_to_string(file_path).context("Unable to read reference dat file")?;
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

    if (name == None) || (description == None) || (version == None) || (author == None) {
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

        let set = db::insert_set(&tx, dat_id, &game_name)?;

        let set_id = set.id;

        for rom_node in game_node.descendants().filter(|node| node.tag_name().name() == "rom") {
            let rom_name = rom_node.attribute("name").context("Unable to read game name")?;
            let rom_size = rom_node.attribute("size").context("Unable to read game size")?;
            let rom_hash = rom_node.attribute("sha1").context("Unable to read game hash")?;
            db::insert_rom(&tx, dat_id, set_id, &rom_name, rom_size.parse().unwrap(), &rom_hash)?;
        }
    }

    tx.commit()?;

    Ok(dat)
}

fn list_dat_records(conn: &mut Connection, dat_id: db::DatId) -> Result<()> {
    let dat_record = db::get_dat(conn, dat_id)?.unwrap();
    println!("Name:        {}", dat_record.name);
    println!("Description: {}", dat_record.description);
    println!("Version:     {}", dat_record.version);
    println!("Author:      {}", dat_record.author);

    println!("--- SETS ---");
    for set_record in db::get_sets(conn, dat_id)? {
        println!("{}", set_record.name);
        for rom_record in db::get_roms_by_set(conn, dat_id, set_record.id)? {
            println!("    {} {} - {}", rom_record.hash, rom_record.name, util::human_size(rom_record.size));
        }
    }
    Ok(())
}

fn find_sets_by_name(conn: &mut Connection, dat_id: db::DatId, name: &str) -> Result<()> {
    let set_records = db::get_sets_by_name(conn, dat_id, name, false)?;
    if set_records.is_empty() {
        println!("No sets found matching `{name}`");
    } else {
        for set_record in set_records {
            println!("{}", set_record.name);
            for rom_record in db::get_roms_by_set(conn, dat_id, set_record.id)? {
                println!("    {} {} - {}", rom_record.hash, rom_record.name, util::human_size(rom_record.size));
            }
        }
    }
    Ok(())
}

fn find_roms(conn: &mut Connection, dat_id: db::DatId, name: &str) -> Result<()> {
    let rom_records = db::get_roms_by_name(conn, dat_id, name, false)?;
    if rom_records.is_empty() {
        println!("No roms found matching `{name}`");
    } else {
        let mut roms_by_set: BTreeMap<db::SetId, Vec<db::RomRecord>> = BTreeMap::new();
        for rom_record in rom_records {
            roms_by_set.entry(rom_record.set_id).or_default().push(rom_record);
        }

        for (set_id, rom_records) in roms_by_set {
            let set_record = db::get_set(conn, set_id)?;
            println!("{}", set_record.name);
            for rom_record in rom_records {
                println!("    {} {} - {}", rom_record.hash, rom_record.name, util::human_size(rom_record.size));
            }
        }
    }
    Ok(())
}

fn scan_files<P: AsRef<Path>>(
    conn: &mut Connection,
    dat_id: db::DatId,
    scan_path: P,
    exclude: &[String],
    recursive: bool,
    incremental: bool,
) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    scan_directory(&tx, dat_id, scan_path, exclude, recursive, incremental)?;
    tx.commit()?;
    Ok(())
}

fn scan_directory<P: AsRef<Path>>(
    conn: &Connection,
    dat_id: db::DatId,
    scan_path: P,
    exclude: &[String],
    recursive: bool,
    incremental: bool,
) -> Result<(), anyhow::Error> {
    let scan_path = scan_path.as_ref().canonicalize()?;
    let (dir_id, incremental) = match db::get_directory(conn, dat_id, &scan_path)? {
        Some(record) => {
            if incremental {
                // add on to existing records
                (record.id, true)
            } else {
                //wipe existing file records and do full scan
                let _ = db::delete_files(conn, record.id)?;
                (record.id, false)
            }
        }
        None => {
            //no existing records, do a full scan
            let record = db::insert_directory(conn, dat_id, &scan_path)?;
            (record.id, false)
        }
    };
    Ok({
        for entry in std::fs::read_dir(&scan_path)? {
            let entry = entry?;
            let path = entry.path();
            if util::is_hidden_file(&path) {
                //skip
            } else if recursive && path.is_dir() {
                scan_directory(conn, dat_id, path, exclude, recursive, incremental)?;
            } else if path.is_file() {
                if path
                    .extension()
                    .map(|ext| exclude.iter().any(|e| ext.eq_ignore_ascii_case(e)))
                    .unwrap_or_default()
                {
                    continue;
                }
                if path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("zip")) {
                    scan_zip_file(conn, dat_id, path, incremental)?;
                } else {
                    scan_file(conn, dat_id, dir_id, path, incremental)?;
                }
            }
        }
    })
}

fn scan_zip_file<P: AsRef<Path>>(conn: &Connection, dat_id: db::DatId, path: P, incremental: bool) -> Result<()> {
    let scan_path = path.as_ref().canonicalize()?;
    let (dir_id, incremental) = match db::get_directory(conn, dat_id, &scan_path)? {
        Some(record) => {
            if incremental {
                // add on to existing records
                (record.id, true)
            } else {
                //wipe existing file records and do full scan
                let _ = db::delete_files(conn, record.id)?;
                (record.id, false)
            }
        }
        None => {
            //no existing records, do a full scan
            let record = db::insert_directory(conn, dat_id, &scan_path)?;
            (record.id, false)
        }
    };

    let name = scan_path.file_prefix().map(|name| name.to_string_lossy()).unwrap();
    let set_records = db::get_sets_by_name(conn, dat_id, &name, true)?;
    let matched = if set_records.is_empty() {
        None
    } else {
        let matched_sets: BTreeSet<db::SetId> = set_records.iter().map(|record| record.id).collect();
        Some(matched_sets)
    };

    let file = File::open(&path)?;
    let mut zip = zip::ZipArchive::new(file)
        .with_context(|| format!("could not open '{}' as a zip file", path.as_ref().display()))?;
    for i in 0..zip.len() {
        match zip.by_index(i) {
            Ok(mut inner_file) => {
                if inner_file.is_file() {
                    if incremental {
                        if let Some(_) = db::get_file(&conn, dir_id, inner_file.name())? {
                            continue;
                        }
                    }

                    let (hash, size) = calc_hash(&mut inner_file)?;
                    let name = inner_file.name();
                    match_roms(conn, dat_id, dir_id, size, name, hash, matched.as_ref())?;
                }
            }
            Err(error) => bail!("{}", error),
        }
    }
    Ok(())
}

fn scan_file<P: AsRef<Path>>(
    conn: &Connection,
    dat_id: db::DatId,
    dir_id: db::DirId,
    path: P,
    incremental: bool,
) -> Result<()> {
    let filename = path.as_ref().file_name().context("Could not get filename")?;

    if incremental {
        if let Some(_) = db::get_file(&conn, dir_id, filename)? {
            return Ok(());
        }
    }
    //scan the file,find a match and insert
    let file = File::open(&path)?;
    let file_size = file.metadata()?.len();
    let filename = &filename.to_string_lossy().to_string();

    let mut reader = BufReader::new(&file);
    let (hash, _) = calc_hash(&mut reader)?;

    match_roms(conn, dat_id, dir_id, file_size, filename, hash, None)?;
    Ok(())
}

fn match_roms(
    conn: &Connection,
    dat_id: db::DatId,
    dir_id: db::DirId,
    file_size: u64,
    filename: &str,
    hash: String,
    matched_sets: Option<&BTreeSet<db::SetId>>,
) -> Result<()> {
    let named_roms = db::get_roms_by_name(conn, dat_id, &filename, true)?;
    if named_roms.is_empty() {
        //nothing named the same, so see if anything matches the hash and if not, then mark as no match
        match_by_hash(conn, dat_id, dir_id, filename, file_size, &hash, db::MatchStatus::None)?;
    } else {
        let mut matches = false;
        for named_rom in &named_roms {
            if file_size == named_rom.size && hash == named_rom.hash {
                if let Some(matched_sets) = matched_sets
                    && !matched_sets.is_empty()
                    && !matched_sets.contains(&named_rom.set_id)
                {
                    //we only want to match if this is in the already matched sets
                    continue;
                }
                //found a match for filename and hash, mark as exact match
                db::insert_file(
                    conn,
                    dir_id,
                    filename,
                    named_rom.size,
                    &named_rom.hash,
                    db::MatchStatus::Match {
                        set_id: named_rom.set_id,
                        rom_id: named_rom.id,
                    },
                )?;
                matches = true;
            }
        }

        if !matches {
            //we found nothing that matches name and hash, so see if anything matches hash, and if not then mark is as name only match
            for named_rom in &named_roms {
                if let Some(matched_sets) = matched_sets
                    && !matched_sets.is_empty()
                    && !matched_sets.contains(&named_rom.set_id)
                {
                    //we only want to match if this is in the already matched sets
                    continue;
                }
                match_by_hash(
                    conn,
                    dat_id,
                    dir_id,
                    filename,
                    file_size,
                    &hash,
                    db::MatchStatus::Name {
                        set_id: named_rom.set_id,
                        rom_id: named_rom.id,
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn calc_hash<R: std::io::Read + ?Sized>(reader: &mut R) -> Result<(String, u64)> {
    let mut hasher = Sha1::new();
    let size = std::io::copy(reader, &mut hasher)?;
    let digest = hasher.finalize();
    let hash = base16ct::lower::encode_string(&digest);
    Ok((hash, size))
}

fn match_by_hash(
    conn: &Connection,
    dat_id: db::DatId,
    dir_id: db::DirId,
    filename: &str,
    size: u64,
    hash: &str,
    default_status: db::MatchStatus,
) -> Result<()> {
    let matches = db::get_roms_by_hash(conn, dat_id, hash)?;
    Ok(match matches.len() {
        0 => {
            db::insert_file(conn, dir_id, filename, size, hash, default_status)?;
        }
        _ => {
            for rom in matches {
                db::insert_file(
                    conn,
                    dir_id,
                    filename,
                    size,
                    hash,
                    db::MatchStatus::Hash {
                        set_id: rom.set_id,
                        rom_id: rom.id,
                    },
                )?;
            }
        }
    })
}

fn list_files(conn: &mut Connection, dat_id: db::DatId, mode: ListMode, partial_name: Option<&str>) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id)?;

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

fn list_sets(conn: &mut Connection, dat_id: db::DatId, missing: bool) -> Result<()> {
    let dirs = db::get_directories(conn, dat_id)?;

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

    println!("--- FOUND SETS ---");
    for (set_id, files) in &sets_to_files {
        let (set_record, roms) = db::get_set_with_roms(conn, *set_id)?;
        let roms_by_id: BTreeMap<db::RomId, &db::RomRecord> = roms.iter().map(|rom| (rom.id, rom)).collect();
        if files.len() == roms.len() {
            println!("[✅] {}", set_record.name);
        } else {
            println!("[⚠️] {}, set has missing roms", set_record.name);
        }
        for file in files {
            match file.status {
                db::MatchStatus::Hash { set_id: _, rom_id } => {
                    println!(" ⚠️  {} {}, should be named {}", file.hash, file.name, roms_by_id[&rom_id].name);
                }
                db::MatchStatus::Name { set_id: _, rom_id } => {
                    println!("  ⚠️  {} {}, should have hash {}", file.hash, file.name, roms_by_id[&rom_id].hash);
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

    if missing {
        println!("--- MISSING SETS ---");
        for set_record in db::get_sets(conn, dat_id)? {
            println_if!(!sets_to_files.contains_key(&set_record.id), "[❌] {}", set_record.name);
        }
    }
    Ok(())
}
