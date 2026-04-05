use camino::Utf8PathBuf;
use clap::{Parser, Subcommand, ValueEnum};
use rustyline::Helper;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;

use crate::completion::{TreeNode, complete};

#[derive(Debug, Parser)]
#[clap(version, about, long_about = None)]
pub struct Args {
    /// select the dat file to use
    #[arg(short, long)]
    pub select: Option<usize>,

    /// command to execute, if none given will enter interactive mode
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// force enter interactive mode, if command is given
    #[arg(short, long)]
    pub interactive: bool,
}

#[derive(Debug, Parser)]
#[command(multicall = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SelectMode {
    /// select all files
    All,
    /// select only matched files
    Matched,
    /// select only misnamed or bad dumps
    Warning,
    /// select only unmatched files
    Unmatched,
}

#[derive(Debug, Subcommand)]
pub enum FileCommands {
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
        /// number of files to hash in parallel (default: 1)
        #[arg(short, long, default_value_t = 1)]
        parallel: usize,
        /// the path to use for scanning files
        #[arg(default_value=".", value_hint = clap::ValueHint::DirPath)]
        path: Utf8PathBuf,
    },
    /// list all files scanned and show their status
    List {
        /// show only files with this status
        #[arg(long, value_enum, default_value_t = SelectMode::All)]
        mode: SelectMode,
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
    /// Sort files into directory
    Sort {
        /// sort only files with this status
        #[arg(long, value_enum, default_value_t = SelectMode::Unmatched)]
        mode: SelectMode,
        /// the base path to use when moving files
        #[arg(default_value=".", value_hint = clap::ValueHint::DirPath)]
        path: Utf8PathBuf,

        // whether to keep the moved files in the database
        #[arg(long)]
        keep: bool,
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
pub enum DataCommands {
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

        /// the index of the dat file to select, as seen in list
        index: Option<usize>,
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

pub struct TermInfo {
    pub tty_in: bool,
    pub tty_out: bool,
    pub interactive: bool,
}

pub struct CompletionHelper<'a> {
    pub node: TreeNode<'a>,
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
