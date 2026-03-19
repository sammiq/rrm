# rrm: a ROM management tool in Rust

This tool uses logiqx xml format dat files, as provided by your friendly preservation site, for managing your own dumps
against known good versions of the same software.

It supports stand-alone files and sets in zip files and maintains a SQLite database to persist data across sessions.

My aim with this tool is to provide something reasonably portable and useful as a command-line only tool, with persistent tracking of scanned files and DAT databases.

## Installation

Prebuilt binaries are available on the [Releases](https://github.com/sammiq/rrm/releases) page for Linux and Windows.


## Building

You need a recent [Rust](https://www.rust-lang.org) installation (I use Rust 1.94 on Fedora 43).

Build the tool with:

    cargo build --release

IMPORTANT: Performance will be *terrible* without compiling for release, the SHA hash code is incredibly slow without compiler optimization.

## Usage

    Usage: rrm [OPTIONS] [COMMAND]

    Options:
        --select <INDEX>    Pre-select a DAT file by index
    -i, --interactive       Force interactive mode after command execution
    -h, --help              Print help
    -V, --version           Print version

### Data Commands

Manage DAT files and their contents:

    rrm data import <DAT_FILE>              Import a new DAT file
        DAT_FILE                            Path to a logiqx xml format dat file

    rrm data update <DAT_FILE> [--yes]      Update current DAT file and re-match files
        DAT_FILE                            Path to a logiqx xml format dat file
        --yes                               Skip confirmation prompt

    rrm data remove [--yes] [INDEX]         Remove current or selected DAT and all associated data
        INDEX                               Index of the DAT file to remove, as shown by list
                                            (defaults to the current DAT)
        --yes                               Skip confirmation prompt

    rrm data list                           List all installed DAT files

    rrm data select <INDEX>                 Select active DAT file by index
        INDEX                               Index of the DAT file to select, as shown by list

    rrm data records                        Show all Sets and ROMs in current DAT

    rrm data sets [PARTIAL_NAME]            Search for Sets by partial name
        PARTIAL_NAME                        Optional substring to filter sets by name

    rrm data roms [PARTIAL_NAME]            Search for ROMs by partial name
        PARTIAL_NAME                        Optional substring to filter ROMs by name

### Files Commands

Scan and manage ROM files:

    rrm files scan [OPTIONS] [PATH]         Scan directory for ROM files
        PATH                                Directory to scan (defaults to current directory)
        -R, --recursive                     Recurse into subdirectories
        --full                              Re-scan all files, not just new ones
        --exclude <EXTENSIONS>              Comma separated list of suffixes to exclude
                                            [default: m3u,dat,txt]

    rrm files list [--mode MODE] [NAME]     List scanned files
        NAME                                Optional substring to filter files by name
        --mode <MODE>                       Filter by status: all (default), matched,
                                            unmatched, or warning

    rrm files matched [NAME]                List matched files (alias for list --mode matched)
        NAME                                Optional substring to filter files by name

    rrm files unmatched [NAME]              List unmatched files (alias for list --mode unmatched)
        NAME                                Optional substring to filter files by name

    rrm files warning [NAME]                List files with warnings (alias for list --mode warning)
        NAME                                Optional substring to filter files by name

    rrm files sets [--missing] [NAME]       List found or missing game sets
        NAME                                Optional substring to filter sets by name
        --missing                           Show missing sets instead of found sets

    rrm files missing [NAME]                List missing game sets (alias for sets --missing)
        NAME                                Optional substring to filter sets by name

    rrm files rename                        Rename loose files to correct names

### Interactive Mode

Running `rrm` with no command or with the `--interactive` flag enters an interactive REPL with command autocompletion and history.

## Database

rrm stores all imported DAT files, scanned file records, and match results in a SQLite database named `rrm.db`. The database location is platform-specific:

- **Linux**: `$XDG_DATA_HOME/rrm/rrm.db` (typically `~/.local/share/rrm/rrm.db`)
- **macOS**: `~/Library/Application Support/rrm/rrm.db`
- **Windows**: `%APPDATA%\rrm\rrm.db`

A backup (`rrm.bak`) is automatically created in the same directory each time the tool is run in case you need to roll back.

## Key Features

- **Multiple DAT support**: Manage multiple DAT files simultaneously with context switching.
- **Incremental scanning**: Only scans new or modified files, tracking by hash and size.
- **ZIP archive support**: Transparently extracts and indexes files from ZIP archives.
- **Three-tier matching**: Files are matched by exact match, name match, or hash match.
- **File renaming**: Rename matched loose files to their correct names.
- **Persistent database**: SQLite-backed storage persists data across sessions.

## Limitations

- Supports only UTF-8 files and paths, as I use the [camino](https://docs.rs/crate/camino/latest) crate and it matches my use-case.
- Does not rename files inside zip files.
- Does not support compression formats other than zip.
- Does not read elements other than `<rom>` inside `<game>` from dat file.
