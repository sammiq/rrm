mod completion;
mod db;
mod util;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, IsTerminal, Write};

use anyhow::{Context, Result, anyhow, bail, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use fallible_iterator::FallibleIterator;
use roxmltree::Document;
use rusqlite::{Connection, Transaction};
use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Config, Editor, Helper};

use crate::completion::{TreeNode, build_completions, complete};
use crate::db::{Deletable, DeletableByDat, FindableByName, Insertable, Queryable, QueryableByDat};
use crate::util::{OptionIf, ResultIf};

const APP_NAME: &str = "rrm";
const ANSI_CURSOR_START: &str = "\x1B[1000D";
const ANSI_ERASE_TO_END: &str = "\x1B[K";

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
        #[arg(short('R'), long)]
        recursive: bool,
        /// re-scan existing files in the directory and not just new files
        #[arg(long)]
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
        #[arg(long)]
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

struct TermInfo {
    tty_in: bool,
    tty_out: bool,
    interactive: bool,
}

struct CompletionHelper<'a> {
    node: TreeNode<'a>,
}

impl Completer for CompletionHelper<'_> {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let line = &line[..pos];
        let (trailing, completions) = complete(&self.node, line);
        let offset = line.len() - trailing;
        Ok((offset, completions))
    }
}
impl Hinter for CompletionHelper<'_> {
    type Hint = String;
}
impl Highlighter for CompletionHelper<'_> {}
impl Validator for CompletionHelper<'_> {}
impl Helper for CompletionHelper<'_> {}

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

    let args = Args::parse();

    let term = TermInfo {
        tty_in: std::io::stdin().is_terminal(),
        tty_out: std::io::stdout().is_terminal(),
        interactive: args.command.is_none() || args.interactive,
    };

    if let Some(index) = args.select {
        do_command(
            &mut conn,
            &mut dat_id,
            &term,
            &Commands::Data {
                data: DataCommands::Select { index },
            },
        )?;
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

    db::DatRecord::get_by_id(conn, &paths[0].dat_id).ok().map(|dat| {
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
        Commands::Files { files } => handle_file_commands(conn, dat_id.as_ref(), term, files),
        Commands::Select { index } => handle_data_commands(conn, dat_id, term, &DataCommands::Select { index: *index }),
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
                import_dat(tx, dat_file).map(|imported| {
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
            ensure!(dat_id.is_some(), "No dat file selected");

            if ask_for_confirmation(term, "Are you sure you want to update the current dat file? (y/N): ", *yes)? {
                let old_dat_id = dat_id.take().expect("Option should contain data");
                db::with_transaction(conn, |tx| {
                    update_dat(tx, dat_file, old_dat_id).map(|imported| {
                        println!("dat file `{}` imported and updated.", imported.name);
                        *dat_id = Some(imported.id);
                    })
                })?;
            }
            Ok(())
        }
        DataCommands::Remove { yes } => {
            ensure!(dat_id.is_some(), "No dat file selected");

            if ask_for_confirmation(term, "Are you sure you want to remove the current dat file? (y/N): ", *yes)? {
                let old_dat_id = dat_id.take().expect("Option should contain data");
                db::with_transaction(conn, |tx| {
                    delete_dat(tx, old_dat_id).map(|_| {
                        println!("dat file removed.");
                        *dat_id = None;
                    })
                })?;
            }
            Ok(())
        }
        DataCommands::List => list_dat_files(conn),
        DataCommands::Select { index } => db::DatRecord::get_all(conn).and_then(|dats| {
            let dat = dats.get(*index).ok_or_else(|| anyhow!("Invalid dat file selection."))?;
            println!("dat file `{}` selected.", dat.name);
            *dat_id = Some(dat.id);
            Ok(())
        }),
        DataCommands::Records => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_dat_records(&DatContext::new(conn, dat_id))
        }
        DataCommands::Sets { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_sets(&DatContext::new(conn, dat_id), partial_name.as_deref())
        }
        DataCommands::Roms { partial_name } => {
            let dat_id = dat_id.as_ref().ok_or_else(|| anyhow!("No dat file selected"))?;
            list_roms(&DatContext::new(conn, dat_id), partial_name.as_deref())
        }
    }
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

            db::with_transaction_mut(conn, |tx| scan_files(tx, dat_id, term, &scan_path, exclude, *recursive, *full))
        }
        FileCommands::List { mode, partial_name } => {
            list_scanned_files(&DatContext::new(conn, dat_id), term, mode, partial_name.as_deref())
        }
        FileCommands::Sets { missing, partial_name } => {
            if *missing {
                list_missing_sets(&DatContext::new(conn, dat_id), term, partial_name.as_deref())
            } else {
                list_found_sets(&DatContext::new(conn, dat_id), term, partial_name.as_deref())
            }
        }
        FileCommands::Rename => db::with_transaction_mut(conn, |tx| rename_files(tx, dat_id, term)),
        FileCommands::Matched { partial_name } => {
            list_scanned_files(&DatContext::new(conn, dat_id), term, &ListMode::Matched, partial_name.as_deref())
        }
        FileCommands::Missing { partial_name } => {
            list_missing_sets(&DatContext::new(conn, dat_id), term, partial_name.as_deref())
        }
        FileCommands::Unmatched { partial_name } => {
            list_scanned_files(&DatContext::new(conn, dat_id), term, &ListMode::Unmatched, partial_name.as_deref())
        }
        FileCommands::Warning { partial_name } => {
            list_scanned_files(&DatContext::new(conn, dat_id), term, &ListMode::Warning, partial_name.as_deref())
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

fn update_dat(conn: &Connection, dat_file: &Utf8PathBuf, old_dat_id: db::DatId) -> Result<db::DatRecord> {
    let imported = import_dat(conn, dat_file)?;

    //delete all existing matches for the old dat, we'll re-match them as we relink directories and files to the new dat
    db::MatchRecord::delete_by_dat(conn, &old_dat_id)?;

    let new_context = DatContext::new(conn, &imported.id);

    for directory in db::DirRecord::get_by_dat(conn, &old_dat_id)? {
        //check if its a zip file, if so, restrict matches to set name if matched
        let matched_sets = if util::is_zip_file(&directory.path) {
            match_sets(&new_context, &directory.path)?
        } else {
            BTreeSet::new()
        };

        for file in directory.get_files(conn)? {
            //rematch using existing information, but link to the new dat
            insert_matches(&new_context, &file, &matched_sets)?;
        }
    }

    //relink all directories to the new dat
    db::DirRecord::relink_dirs(conn, &old_dat_id, &imported.id)?;

    //relink all files to the new dat
    db::FileRecord::relink_files(conn, &old_dat_id, &imported.id)?;

    //if we successfully updated everything and relinked and the transaction completed, we can now delete the old dat
    delete_dat(conn, old_dat_id)?;

    Ok(imported)
}

fn strip_doctype(s: &mut String) {
    let Some(start) = s.find("<!DOCTYPE") else { return };
    let mut depth = 0usize;
    let end = s[start..].char_indices().find(|(_, c)| match c {
        '[' => {
            depth += 1;
            false
        }
        ']' => {
            depth = depth.saturating_sub(1);
            false
        }
        '>' if depth == 0 => true,
        _ => false,
    });
    if let Some((len, _)) = end {
        s.replace_range(start..=start + len, "");
    }
}

fn import_dat<P: AsRef<Utf8Path>>(conn: &Connection, file_path: P) -> Result<db::DatRecord> {
    let mut df_buffer = std::fs::read_to_string(file_path.as_ref()).context("Unable to read reference dat file")?;
    //so we can turn off dtd-processing, we need to remove any declaration, in most files its unused and is a security issue.
    strip_doctype(&mut df_buffer);
    parse_dat(conn, &df_buffer)
}

fn parse_dat(conn: &Connection, df_buffer: &str) -> Result<db::DatRecord> {
    let df_xml = Document::parse(df_buffer).context("Unable to parse reference dat file")?;
    let new_dat = parse_dat_info(&df_xml)?;
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
                dat_id: dat.id,
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
                    dat_id: dat.id,
                    set_id: set.id,
                    name: rom_name.to_string(),
                    size: db::StoredU64(rom_size.parse().context("should be a valid number")?),
                    hash: rom_hash.to_string(),
                },
            )?;
        }
    }
    Ok(dat)
}

fn parse_dat_info(df_xml: &Document<'_>) -> Result<db::NewDat> {
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
    Ok(new_dat)
}

fn delete_dat(conn: &Connection, dat_id: db::DatId) -> Result<()> {
    //remove all scanned files and directories
    db::MatchRecord::delete_by_dat(conn, &dat_id)?;
    db::FileRecord::delete_by_dat(conn, &dat_id)?;
    db::DirRecord::delete_by_dat(conn, &dat_id)?;

    //remove all roms and sets before removing the dat
    db::RomRecord::delete_by_dat(conn, &dat_id)?;
    db::SetRecord::delete_by_dat(conn, &dat_id)?;

    //remove the dat itself
    db::DatRecord::delete_by_id(conn, &dat_id)?;

    Ok(())
}

fn list_dat_records(ctx: &DatContext<'_>) -> Result<()> {
    let dat_record = db::DatRecord::get_by_id(ctx.conn, ctx.dat_id)?;
    println!("Name:        {}", dat_record.name);
    println!("Description: {}", dat_record.description);
    println!("Version:     {}", dat_record.version);
    println!("Author:      {}", dat_record.author);

    println!("--- SETS ---");
    for set in db::SetRecord::get_by_dat(ctx.conn, ctx.dat_id)? {
        println!("{}", set.name);
        for rom in set.get_roms(ctx.conn)? {
            println!("    {} {} - {}", rom.hash, rom.name, util::human_size(rom.size));
        }
    }
    Ok(())
}

fn list_sets(ctx: &DatContext<'_>, name: Option<&str>) -> Result<()> {
    let sets = if let Some(name) = name {
        db::SetRecord::find_by_name(ctx.conn, ctx.dat_id, name, false)
    } else {
        db::SetRecord::get_by_dat(ctx.conn, ctx.dat_id)
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

fn list_roms(ctx: &DatContext<'_>, name: Option<&str>) -> Result<()> {
    let roms = if let Some(name) = name {
        db::RomRecord::find_by_name(ctx.conn, ctx.dat_id, name, false)
    } else {
        db::RomRecord::get_by_dat(ctx.conn, ctx.dat_id)
    }?;
    if roms.is_empty() {
        println!("No roms found.");
    } else {
        let mut roms_by_set: BTreeMap<_, Vec<_>> = BTreeMap::new();
        roms.iter()
            .for_each(|rom| roms_by_set.entry(&rom.set_id).or_default().push(rom));

        let all_sets = db::SetRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
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

/// Options that are fixed for the duration of a scan and passed through every recursive call.
struct ScanOptions<'a> {
    exclude: &'a [String],
    recursive: bool,
}

/// Bundles a database connection with the currently selected dat file ID.
/// Passed to any function that operates on a specific dat, avoiding the
/// repetition of carrying both as separate arguments everywhere.
struct DatContext<'a> {
    conn: &'a Connection,
    dat_id: &'a db::DatId,
}

impl<'a> DatContext<'a> {
    fn new(conn: &'a Connection, dat_id: &'a db::DatId) -> Self {
        Self { conn, dat_id }
    }
}

fn scan_files(
    tx: &mut Transaction,
    dat_id: &db::DatId,
    term: &TermInfo,
    scan_path: &Utf8Path, //expect this to be canonicalized
    exclude: &[String],
    recursive: bool,
    full_scan: bool,
) -> Result<()> {
    if full_scan {
        //delete all records associated with the files
        db::MatchRecord::delete_by_dat(tx, dat_id)?;
        db::FileRecord::delete_by_dat(tx, dat_id)?;
        db::DirRecord::delete_by_dat(tx, dat_id)?;
    }

    let options = ScanOptions { exclude, recursive };
    let file_count = scan_directory(tx, dat_id, scan_path, &options, &|count| {
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
    dat_id: &db::DatId,
    scan_path: &Utf8Path,
    options: &ScanOptions<'_>,
    progress_fn: &dyn Fn(u64),
) -> Result<u64> {
    let (dir, incremental) = match db::DirRecord::find_by_path_in_dat(tx, dat_id, scan_path.as_str())? {
        Some(dir) => (dir, true),
        None => {
            //no existing records, do a full scan
            let dir = db::DirRecord::insert(
                tx,
                &db::NewDir {
                    dat_id: *dat_id,
                    path: scan_path.to_string(),
                },
            )?;
            (dir, false)
        }
    };

    //these will be empty if not incremental, but its cheap enough to call them that its not worth optimising them out
    let existing_subdirs = dir.get_children(tx)?;
    let mut subdirs_by_path: BTreeSet<_> = existing_subdirs.iter().map(|dir| dir.path.as_str()).collect();

    let existing_files = dir.get_files(tx)?;
    let mut files_by_name: BTreeMap<_, _> = existing_files.iter().map(|file| (file.name.as_str(), file)).collect();

    let mut file_count = 0u64;

    let mut iter = fallible_iterator::convert(scan_path.read_dir_utf8()?);
    while let Some(entry) = iter.next()? {
        let path = entry.path();
        if util::is_hidden_file(path) {
            continue;
        }

        if path.is_dir() {
            if !options.recursive {
                continue;
            }
            subdirs_by_path.remove(path.as_str());
            file_count =
                scan_directory(tx, dat_id, path, options, &|count| progress_fn(file_count + count))?;
        } else if path.is_file() {
            if util::has_extension(path, options.exclude) {
                continue;
            }

            if util::is_zip_file(path) {
                subdirs_by_path.remove(path.as_str());
                //for zip files we need to rollback the entire directory and files if it failed to scan properly
                match db::with_savepoint(tx, |sp| {
                    scan_zip_file(&DatContext::new(sp, dat_id), path, options.exclude)
                }) {
                    Ok(files_scanned) => {
                        file_count += files_scanned;
                    }
                    Err(e) => {
                        eprintln!("Failed to scan {}. Error: {e}", path);
                    }
                }
            } else if let Some(filename) = path.file_name() {
                let exists = files_by_name.remove(filename).is_some();
                if exists && incremental {
                    //there was an existing scanned file, so skip it
                    continue;
                }
                match scan_file(&DatContext::new(tx, dat_id), &dir.id, path, filename) {
                    Ok(_) => file_count += 1,
                    Err(e) => eprintln!("Failed to scan {}. Error: {e}", path),
                }
            }
        }
        progress_fn(file_count);
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
                    .and_then(|_| db::DirRecord::delete_by_id(ctx.conn, &dir.id))
                    .if_err(|e| eprintln!("Failed to delete directory {}. Error: {e}", existing_path));
            }
            Ok(None) => eprintln!("Failed to find directory entry {}.", existing_path),
            Err(e) => eprintln!("Failed to get directory entry {}. Error: {e}", existing_path),
        }
    }
    for (_, existing_file) in stale_files {
        existing_file.delete_matches(ctx.conn)
            .and_then(|_| db::FileRecord::delete_by_id(ctx.conn, &existing_file.id))
            .if_err(|e| eprintln!("Failed to remove {}. Error: {e}", existing_file.name));
    }
}

fn scan_zip_file(ctx: &DatContext<'_>, path: &Utf8Path, exclude: &[String]) -> Result<u64> {
    let maybe_dir = db::DirRecord::find_by_path_in_dat(ctx.conn, ctx.dat_id, path.as_str())?;
    if maybe_dir.is_some() {
        //if we have scanned this zip file before, skip it during an incremental scan
        return Ok(0);
    }

    let dir = db::DirRecord::insert(
        ctx.conn,
        &db::NewDir {
            dat_id: *ctx.dat_id,
            path: path.to_string(),
        },
    )?;

    let matched = match_sets(ctx, path)?;

    let file = File::open(path)?;
    let mut zip = zip::ZipArchive::new(file).with_context(|| format!("could not open '{}' as a zip file", path))?;
    let mut file_count = 0u64;
    for i in 0..zip.len() {
        match zip.by_index(i) {
            Ok(mut inner_file) => {
                if !inner_file.is_file() {
                    continue;
                }
                if util::has_extension(inner_file.name(), exclude) {
                    continue;
                }

                file_count += 1;
                let (hash, file_size) = util::calc_hash(&mut inner_file)?;
                insert_files_and_matches(ctx, &dir.id, inner_file.name(), file_size, &hash, &matched)?;
            }
            Err(error) => bail!("{}", error),
        }
    }

    //we could be smarter here and try to infer the largest set matched
    //and assume that the set is supposed to be that if no set was matched.

    Ok(file_count)
}

fn match_sets<P: AsRef<Utf8Path>>(ctx: &DatContext<'_>, path: P) -> Result<BTreeSet<db::SetId>> {
    let name = path.as_ref().file_prefix().context("should have a file name")?;
    let sets = db::SetRecord::find_by_name(ctx.conn, ctx.dat_id, name, true)?;
    let matched: BTreeSet<db::SetId> = sets.iter().map(|record| record.id).collect();
    Ok(matched)
}

fn scan_file(ctx: &DatContext<'_>, dir_id: &db::DirId, path: &Utf8Path, filename: &str) -> Result<()> {
    //scan the file, find a match and insert
    let file = File::open(path)?;

    let mut reader = BufReader::new(&file);
    let (hash, file_size) = util::calc_hash(&mut reader)?;

    insert_files_and_matches(ctx, dir_id, filename, file_size, &hash, &BTreeSet::new())?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct FileMatch {
    pub status: db::MatchStatus,
    pub set_id: db::SetId,
    pub rom_id: db::RomId,
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
    hash: &str,
    matched_sets: &BTreeSet<db::SetId>,
) -> Result<()> {
    let file = db::FileRecord::insert(
        ctx.conn,
        &db::NewFile {
            dat_id: *ctx.dat_id,
            dir_id: *dir_id,
            name: file_name.to_string(),
            size: db::StoredU64(file_size),
            hash: hash.to_string(),
        },
    )?;

    insert_matches(ctx, &file, matched_sets)
}

fn insert_matches(
    ctx: &DatContext<'_>,
    file: &db::FileRecord,
    matched_sets: &BTreeSet<db::Id<db::SetRecord>>,
) -> Result<()> {
    let matched = match_roms(ctx, &file.name, file.size, &file.hash, matched_sets)?;
    if let Some(items) = matched {
        for item in items {
            db::MatchRecord::insert(
                ctx.conn,
                &db::NewMatch {
                    dat_id: *ctx.dat_id,
                    file_id: file.id,
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

#[rustfmt::skip] //single line match arms are more readable
fn format_file_indicator(status: Option<&db::MatchStatus>, is_tty: bool) -> &str {
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
) -> Result<String> {
    let indicator = format_file_indicator(matched.as_ref().map(|(m, _)| &m.status), is_tty);
    let result = match matched {
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
    };
    Ok(result)
}

fn format_file_status(
    conn: &Connection,
    file: &db::FileRecord,
    matched: Option<&db::MatchRecord>,
    is_tty: bool,
) -> Result<String> {
    match matched {
        Some(m) => {
            let rom = db::RomRecord::get_by_id(conn, &m.rom_id)?;
            format_match_status(file, Some((m, &rom)), is_tty)
        }
        None => format_match_status(file, None, is_tty),
    }
}

fn list_scanned_files(
    ctx: &DatContext<'_>,
    term: &TermInfo,
    mode: &ListMode,
    partial_name: Option<&str>,
) -> Result<()> {
    //get these in bulk to avoid doing a query per file when we display them
    let matches = db::MatchRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    let matches_by_file: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.file_id).or_default().push(m);
        acc
    });

    let dirs = db::DirRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    for dir in dirs {
        let files = if let Some(partial_name) = partial_name {
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
                    if should_display_file_status(Some(&fm.status), mode) {
                        lines.push(format_file_status(ctx.conn, &file, Some(fm), term.tty_out)?);
                    }
                }
            } else if should_display_file_status(None, mode) {
                lines.push(format_file_status(ctx.conn, &file, None, term.tty_out)?);
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

#[derive(Debug, PartialEq)]
enum SetStatus {
    Missing,
    Partial,
    Complete,
}

fn calculate_set_status(set_roms: &[db::RomRecord], set_matches: &[db::MatchRecord]) -> SetStatus {
    if set_matches.is_empty() {
        return SetStatus::Missing;
    }
    let matched_rom_ids: BTreeSet<_> = set_matches.iter().map(|m| m.rom_id).collect();
    if set_roms.iter().all(|rom| matched_rom_ids.contains(&rom.id)) {
        SetStatus::Complete
    } else {
        SetStatus::Partial
    }
}

#[rustfmt::skip] //single line match arms are more readable
fn format_set_indicator(status: &SetStatus, is_tty: bool) -> &str {
    match status {
        SetStatus::Missing => if is_tty { "❌" } else { "NONE" },
        SetStatus::Partial => if is_tty { "⚠️" } else { "WARN" },
        SetStatus::Complete => if is_tty { "✅" } else { " OK " },
    }
}

fn list_missing_sets(ctx: &DatContext<'_>, term: &TermInfo, partial_name: Option<&str>) -> Result<()> {
    let matches = db::MatchRecord::get_by_dat(ctx.conn, ctx.dat_id)?;
    let all_sets = db::SetRecord::get_by_dat(ctx.conn, ctx.dat_id)?;

    let matches_by_set: BTreeMap<_, Vec<_>> = matches.iter().fold(BTreeMap::new(), |mut acc, m| {
        acc.entry(&m.set_id).or_default().push(m);
        acc
    });

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
        println_if!(!matches_by_set.contains_key(&set.id), "[{status}] {}", set.name);
    }
    println!("{} / {} sets missing.", all_sets.len() - matches_by_set.len(), all_sets.len());
    Ok(())
}

fn list_found_sets(ctx: &DatContext<'_>, term: &TermInfo, partial_name: Option<&str>) -> Result<()> {
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
    let partial_status = format_set_indicator(&SetStatus::Partial, term.tty_out);
    let complete_status = format_set_indicator(&SetStatus::Complete, term.tty_out);
    for set in &found_sets {
        if let Some(partial_name) = partial_name
            && !set
                .name
                .to_ascii_lowercase()
                .contains(&partial_name.to_ascii_lowercase())
        {
            continue;
        }

        if let Some(set_roms) = roms_by_set.get(&set.id)
            && let Some(set_matches) = matches_by_set.get(&set.id)
        {
            let roms_by_romid: BTreeMap<_, _> = set_roms.iter().map(|rom| (&rom.id, rom)).collect();
            let matches_by_romid: BTreeMap<_, _> = set_matches.iter().map(|fmatch| (&fmatch.rom_id, fmatch)).collect();

            if calculate_set_status(set_roms, set_matches) == SetStatus::Complete {
                println!("[{complete_status}] {}", set.name);
            } else {
                println!("[{partial_status}] {}, set has missing roms", set.name);
            }

            for matched in set_matches {
                let file = files_by_id
                    .get(&matched.file_id)
                    .expect("Should always have a valid file retrieved");
                let rom = roms_by_romid
                    .get(&matched.rom_id)
                    .expect("Should always have a valid rom retrieved");
                let status = format_match_status(file, Some((matched, rom)), term.tty_out)?;
                println!(" {status}");
            }

            let missing_indicator = format_file_indicator(None, term.tty_out);
            for rom in set_roms {
                println_if!(
                    !matches_by_romid.contains_key(&rom.id),
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

fn rename_files(tx: &mut Transaction, dat_id: &db::DatId, term: &TermInfo) -> Result<()> {
    for directory in db::DirRecord::get_by_dat(tx, dat_id)? {
        if util::is_zip_file(&directory.path) {
            continue;
        }

        let files = directory.get_files(tx)?;
        let mut matches_by_name = BTreeMap::new();
        for file in &files {
            let file_matches = db::MatchRecord::find_by_status_for_file(tx, &file.id, &db::MatchStatus::Hash)?;
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
                let rom = db::RomRecord::get_by_id(tx, &file_match.rom_id)?;

                match db::with_savepoint(tx, |sp| {
                    let new_file = file.update_name(sp, &rom.name)?;
                    let new_match = file_match.update(sp, &db::MatchStatus::Match)?;
                    let old_path = path.join(name);
                    let new_path = path.join(&new_file.name);
                    std::fs::rename(&old_path, &new_path)?;
                    Ok((new_file, new_match))
                }) {
                    Ok((new_file, new_match)) => {
                        let indicator = format_file_indicator(Some(&new_match.status), term.tty_out);
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
mod tests {
    use super::*;

    fn stripped(input: &str) -> String {
        let mut s = input.to_string();
        strip_doctype(&mut s);
        s
    }

    #[test]
    fn strip_doctype_no_doctype() {
        let xml = r#"<?xml version="1.0"?><root/>"#;
        assert_eq!(stripped(xml), xml);
    }

    #[test]
    fn strip_doctype_simple() {
        let xml = r#"<?xml version="1.0"?><!DOCTYPE foo SYSTEM "foo.dtd"><root/>"#;
        assert_eq!(stripped(xml), r#"<?xml version="1.0"?><root/>"#);
    }

    #[test]
    fn strip_doctype_with_internal_subset() {
        let xml = r#"<!DOCTYPE foo [<!ELEMENT bar (baz)><!ELEMENT baz (#PCDATA)>]><root/>"#;
        assert_eq!(stripped(xml), "<root/>");
    }

    #[test]
    fn strip_doctype_internal_subset_gt_not_confused() {
        // The > inside the internal subset must not terminate the search early
        let xml = r#"<!DOCTYPE foo [<!ENTITY gt ">">]><root/>"#;
        assert_eq!(stripped(xml), "<root/>");
    }

    // --- parse_dat_info ---

    fn parse_info(xml: &str) -> Result<db::NewDat> {
        let doc = Document::parse(xml).unwrap();
        parse_dat_info(&doc)
    }

    #[test]
    fn parse_dat_info_valid() {
        let xml = r#"<datafile>
            <header>
                <name>My DAT</name>
                <description>A test dat</description>
                <version>1.0</version>
                <author>tester</author>
            </header>
        </datafile>"#;
        let dat = parse_info(xml).unwrap();
        assert_eq!(dat.name, "My DAT");
        assert_eq!(dat.description, "A test dat");
        assert_eq!(dat.version, "1.0");
        assert_eq!(dat.author, "tester");
        assert_eq!(dat.hash_type, "sha1");
    }

    #[test]
    fn parse_dat_info_missing_header() {
        let xml = r#"<datafile><name>My DAT</name></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_name() {
        let xml = r#"<datafile><header>
            <description>desc</description><version>1.0</version><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_description() {
        let xml = r#"<datafile><header>
            <name>n</name><version>1.0</version><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_version() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><author>auth</author>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_missing_author() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><version>1.0</version>
        </header></datafile>"#;
        assert!(parse_info(xml).is_err());
    }

    #[test]
    fn parse_dat_info_ignores_unknown_elements() {
        let xml = r#"<datafile><header>
            <name>n</name><description>d</description><version>1.0</version>
            <author>auth</author><unknown>ignored</unknown>
        </header></datafile>"#;
        assert!(parse_info(xml).is_ok());
    }

    // --- helpers ---

    fn make_rom(id: i64, dat_id: i64, set_id: i64, size: u64, hash: &str) -> db::RomRecord {
        db::RomRecord {
            id: id.into(),
            dat_id: dat_id.into(),
            set_id: set_id.into(),
            name: format!("rom_{id}"),
            size,
            hash: hash.to_string(),
        }
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

    // --- should_display_file_status ---

    #[test]
    fn display_status_unmatched_file() {
        assert!(should_display_file_status(None, &ListMode::Unmatched));
        assert!(should_display_file_status(None, &ListMode::All));
        assert!(!should_display_file_status(None, &ListMode::Warning));
        assert!(!should_display_file_status(None, &ListMode::Matched));
    }

    #[test]
    fn display_status_hash_match() {
        assert!(should_display_file_status(Some(&db::MatchStatus::Hash), &ListMode::Warning));
        assert!(should_display_file_status(Some(&db::MatchStatus::Hash), &ListMode::All));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Hash), &ListMode::Matched));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Hash), &ListMode::Unmatched));
    }

    #[test]
    fn display_status_name_match() {
        assert!(should_display_file_status(Some(&db::MatchStatus::Name), &ListMode::Warning));
        assert!(should_display_file_status(Some(&db::MatchStatus::Name), &ListMode::All));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Name), &ListMode::Matched));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Name), &ListMode::Unmatched));
    }

    #[test]
    fn display_status_full_match() {
        assert!(should_display_file_status(Some(&db::MatchStatus::Match), &ListMode::Matched));
        assert!(should_display_file_status(Some(&db::MatchStatus::Match), &ListMode::All));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Match), &ListMode::Warning));
        assert!(!should_display_file_status(Some(&db::MatchStatus::Match), &ListMode::Unmatched));
    }

    // --- resolve_match ---

    fn make_match_record(id: i64, set_id: i64, rom_id: i64) -> db::MatchRecord {
        db::MatchRecord {
            id: id.into(),
            dat_id: 1i64.into(),
            file_id: id.into(),
            status: db::MatchStatus::Match,
            set_id: set_id.into(),
            rom_id: rom_id.into(),
        }
    }

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

    // --- calculate_set_status ---

    #[test]
    fn set_status_complete_when_all_roms_matched() {
        let roms = [make_rom(1, 1, 1, 100, "a"), make_rom(2, 1, 1, 200, "b")];
        let matches = [make_match_record(1, 1, 1), make_match_record(2, 1, 2)];
        assert_eq!(calculate_set_status(&roms, &matches), SetStatus::Complete);
    }

    #[test]
    fn set_status_partial_when_some_roms_unmatched() {
        let roms = [make_rom(1, 1, 1, 100, "a"), make_rom(2, 1, 1, 200, "b")];
        let matches = [make_match_record(1, 1, 1)]; // only rom 1 matched
        assert_eq!(calculate_set_status(&roms, &matches), SetStatus::Partial);
    }

    #[test]
    fn set_status_missing_when_no_matches() {
        let roms = [make_rom(1, 1, 1, 100, "a")];
        assert_eq!(calculate_set_status(&roms, &[]), SetStatus::Missing);
    }

    #[test]
    fn set_status_complete_with_single_rom() {
        let roms = [make_rom(1, 1, 1, 100, "a")];
        let matches = [make_match_record(1, 1, 1)];
        assert_eq!(calculate_set_status(&roms, &matches), SetStatus::Complete);
    }
}
