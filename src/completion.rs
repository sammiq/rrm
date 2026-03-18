use camino::Utf8Path;
use clap::{Arg, Command, ValueHint};

#[derive(Debug)]
pub enum TreeNode<'a> {
    Branch(String, Vec<TreeNode<'a>>),
    Option(&'a Arg),
    Positional(&'a Arg),
}

impl TreeNode<'_> {
    pub fn names(&self) -> Box<dyn Iterator<Item = String> + '_> {
        match self {
            TreeNode::Branch(name, _) => Box::new(std::iter::once(name.clone())),
            TreeNode::Option(arg) => Box::new(
                //we assume here that anything without a long or short option
                //is actually a positional
                arg.get_long()
                    .map(|long| format!("--{long}"))
                    .into_iter()
                    .chain(arg.get_short().map(|short| format!("-{short}"))),
            ),
            TreeNode::Positional(arg) => Box::new(std::iter::once(arg.get_id().to_string())),
        }
    }
}

pub fn build_completions<'a>(command: &'a Command) -> TreeNode<'a> {
    let mut arg_vec = Vec::new();
    for arg in command.get_arguments() {
        if arg.is_positional() {
            arg_vec.push(TreeNode::Positional(arg));
        } else {
            arg_vec.push(TreeNode::Option(arg));
        }
    }
    for sub in command.get_subcommands() {
        arg_vec.push(build_completions(sub))
    }
    TreeNode::Branch(command.get_name().to_string(), arg_vec)
}

/// Returns (length of partial, candidates).
pub fn complete(root_node: &TreeNode, line: &str) -> (usize, Vec<String>) {
    // Collect tokens via the Shlex iterator so we can inspect had_error afterwards.
    // If had_error is true, the line ended inside an unclosed quote: the partial token
    // was discarded by shlex, so we recover it by finding the opening quote in the original line.
    let mut lex = shlex::Shlex::new(line);
    let tokens: Vec<String> = lex.by_ref().collect();
    let partial_override: Option<String> = if lex.had_error {
        line.rfind(|c| c == '\'' || c == '"').map(|pos| line[pos + 1..].to_owned())
    } else {
        None
    };
    let trailing_space = partial_override.is_none() && (line.ends_with(' ') || line.is_empty());

    let children = match root_node {
        TreeNode::Branch(_, children) => children,
        _ => return (0, vec![]),
    };

    // Determine (walk_tokens, final_token):
    // - walk_tokens: the already-complete tokens used to navigate the tree
    // - final_token: the partial string being completed
    //
    // For unclosed-quote case: all tokens are navigation tokens, partial is raw string after quote.
    // For normal case: split last token off as partial (unless trailing space).
    let final_token_storage: String;
    let (walk_tokens, final_token): (&[String], &str) = if let Some(ref raw_partial) = partial_override {
        if tokens.is_empty() {
            // e.g. `"partial` at top level — no navigation needed
            return (raw_partial.len() + 1, candidates_matching(children, raw_partial, true));
        }
        (tokens.as_slice(), raw_partial.as_str())
    } else if tokens.is_empty() || (tokens.len() == 1 && !trailing_space) {
        let partial = tokens.first().map(String::as_str).unwrap_or("");
        return (partial.len(), candidates_matching(children, partial, false));
    } else if trailing_space {
        (tokens.as_slice(), "")
    } else {
        let (last, rest) = tokens.split_last().unwrap();
        final_token_storage = last.clone();
        (rest, final_token_storage.as_str())
    };

    // When replacing the partial in the line, we need to cover the opening quote too
    let partial_replace_len = final_token.len() + if partial_override.is_some() { 1 } else { 0 };

    let mut current_children: &[TreeNode] = children;
    let mut token_iter = walk_tokens.iter().peekable();
    while let Some(token) = token_iter.next() {
        match find_exact(current_children, token) {
            Some(TreeNode::Branch(_, next_children)) => {
                current_children = next_children;
            }
            Some(TreeNode::Option(arg)) => {
                for _ in 0..num_arg_values(arg) {
                    if token_iter.peek().is_none() {
                        // Ran out of tokens while consuming values — we're completing
                        // this option's value, not a new token
                        return (partial_replace_len, match_arg_value(arg, final_token, partial_override.is_some()));
                    }
                    token_iter.next();
                }
            }
            //positional args matching names is irrelevant
            Some(TreeNode::Positional(_)) | None => {
                if current_children.iter().any(|c| matches!(c, TreeNode::Positional(_))) {
                    //current token is positional, lets continue matching tokens
                    continue;
                } else {
                    return (0, Vec::new());
                }
            }
        }
    }

    (partial_replace_len, candidates_matching(current_children, final_token, partial_override.is_some()))
}

fn num_arg_values(arg: &Arg) -> usize {
    match arg.get_num_args() {
        Some(range) => range.min_values(),
        None => 1, // clap default: one value
    }
}

/// Find the first node whose name exactly equals `token`.
fn find_exact<'a, 'b>(nodes: &'a [TreeNode<'b>], token: &str) -> Option<&'a TreeNode<'b>> {
    nodes.iter().find(|n| n.names().any(|name| name == token))
}

/// Simple name-based prefix match used for Branch and Option nodes,
/// and Positional nodes without a path hint.
fn find_partial(node: &TreeNode, partial: &str) -> Vec<String> {
    node.names().filter(|name| name.starts_with(partial)).collect()
}

fn candidates_matching<'a>(nodes: &'a [TreeNode<'a>], partial: &str, in_quote: bool) -> Vec<String> {
    nodes
        .iter()
        .flat_map(|n| match n {
            TreeNode::Option(arg) => {
                let name_matches = find_partial(n, partial);
                // prefer flag name match; only fall back to arg value match if no flag matches partial
                if name_matches.is_empty() { match_arg_value(arg, partial, in_quote) } else { name_matches }
            }
            TreeNode::Positional(arg) => {
                // Don't fall back in this case onto the name itself
                match_arg_value(arg, partial, in_quote)
            }
            _ => find_partial(n, partial),
        })
        .collect()
}

fn match_arg_value(arg: &Arg, partial: &str, in_quote: bool) -> Vec<String> {
    match arg.get_value_hint() {
        ValueHint::FilePath | ValueHint::AnyPath => fs_candidates(partial, false, in_quote),
        ValueHint::DirPath => fs_candidates(partial, true, in_quote),
        _ => arg
            .get_possible_values()
            .iter()
            .filter(|v| !v.is_hide_set())
            .map(|v| v.get_name().to_owned())
            .filter(|name| name.starts_with(partial))
            .collect(),
    }
}

/// Return filesystem candidates for the given partial path.
/// If `dirs_only` is true, only directories are returned.
/// If `in_quote` is true, the partial is inside an unclosed quote: directory candidates are
/// returned unquoted (the caller's open quote stays open for further completion), while file
/// candidates get a closing `'` appended so the argument is properly terminated.
fn fs_candidates(partial: &str, dirs_only: bool, in_quote: bool) -> Vec<String> {
    // Split partial into the directory to list and the filename prefix to filter by
    let (dir, prefix) = if partial.is_empty() {
        (Utf8Path::new("."), "")
    } else {
        let p = Utf8Path::new(partial);
        if partial.ends_with('/') {
            // e.g. "src/" — list inside that dir with no prefix filter
            (p, "")
        } else {
            //p.parent() can return an empty path (which seems wrong), so if the partial is the first,
            //default to the current directory
            if p.iter().next() == Some(partial) {
                (Utf8Path::new("."), partial)
            } else {
                (p.parent().unwrap_or(Utf8Path::new(".")), p.file_name().unwrap_or(partial))
            }
        }
    };

    let Ok(entries) = dir.read_dir_utf8() else {
        return vec![];
    };

    entries
        .filter_map(|e| e.ok())
        .filter(|e| if dirs_only { e.file_type().map(|t| t.is_dir()).unwrap_or_default() } else { true })
        .filter_map(|e| {
            let name = e.file_name();
            if !name.starts_with(prefix) {
                return None;
            }
            // Reconstruct the full candidate path from the dir + name,
            // appending "/" to directories so the shell can continue completing
            let is_dir = e.file_type().map(|t| t.is_dir()).unwrap_or_default();
            let suffix = if is_dir { "/" } else { "" };
            let candidate = if partial.is_empty() || partial.ends_with('/') {
                format!("{}{}{}", partial, name, suffix)
            } else {
                let dir_str = dir.as_str();
                let dir_prefix = if dir_str == "." && !partial.starts_with("./") {
                    String::new()
                } else if dir_str.ends_with('/') {
                    // Root "/" or any path already ending with slash — don't add another
                    dir_str.to_owned()
                } else {
                    format!("{}/", dir_str)
                };
                format!("{}{}{}", dir_prefix, name, suffix)
            };
            // Quote rules:
            // - in_quote: replacement starts at the existing opening quote, so re-emit it.
            //   directory: `'path/to/dir/`  — quote left open for further typing
            //   file:      `'path/to/file'` — quote closed, argument complete
            // - not in_quote: use shlex quoting, but for directories strip the trailing `'`
            //   so the quote stays open (e.g. `'dir with spaces/` not `'dir with spaces/'`)
            let quoted = if in_quote {
                if is_dir { format!("'{}", candidate) } else { format!("'{}'", candidate) }
            } else {
                let s = shlex::try_quote(&candidate).expect("file paths do not contain null bytes").into_owned();
                if is_dir { s.strip_suffix('\'').unwrap_or(&s).to_owned() } else { s }
            };
            Some(quoted)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::ArgAction;
    use std::fs;
    use tempfile::TempDir;

    /// Build a test command with a mix of flags, enum-valued options, subcommands,
    /// and positional args. Help/version flags are disabled so tests have a fixed set.
    fn make_command() -> Command {
        Command::new("cmd")
            .disable_help_flag(true)
            .disable_version_flag(true)
            .arg(
                Arg::new("format")
                    .long("format")
                    .value_parser(["json", "table", "plain"])
                    .num_args(1),
            )
            .arg(Arg::new("verbose").long("verbose").short('v').action(ArgAction::SetTrue))
            .subcommand(
                Command::new("install")
                    .disable_help_flag(true)
                    .arg(Arg::new("package").value_parser(["pkg-a", "pkg-b", "pkg-c"]).num_args(1))
                    .arg(Arg::new("force").long("force").action(ArgAction::SetTrue)),
            )
            .subcommand(Command::new("remove").disable_help_flag(true))
    }

    // --- TreeNode::names() ---

    #[test]
    fn branch_names_returns_its_name() {
        let node: TreeNode = TreeNode::Branch("foo".into(), vec![]);
        assert_eq!(node.names().collect::<Vec<_>>(), vec!["foo"]);
    }

    #[test]
    fn option_names_long_and_short() {
        let cmd = make_command();
        let arg = cmd.get_arguments().find(|a| a.get_id() == "verbose").unwrap();
        let node = TreeNode::Option(arg);
        let names = node.names().collect::<Vec<_>>();
        // long name is yielded before short per names() iterator order
        assert_eq!(names, vec!["--verbose", "-v"]);
    }

    #[test]
    fn option_names_long_only() {
        let cmd = make_command();
        let arg = cmd.get_arguments().find(|a| a.get_id() == "format").unwrap();
        let node = TreeNode::Option(arg);
        assert_eq!(node.names().collect::<Vec<_>>(), vec!["--format"]);
    }

    #[test]
    fn positional_names_returns_id() {
        let cmd = make_command();
        let subcmd = cmd.get_subcommands().find(|s| s.get_name() == "install").unwrap();
        let arg = subcmd.get_arguments().find(|a| a.get_id() == "package").unwrap();
        let node = TreeNode::Positional(arg);
        assert_eq!(node.names().collect::<Vec<_>>(), vec!["package"]);
    }

    // --- build_completions ---

    #[test]
    fn build_completions_creates_branch_for_root() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        assert!(matches!(tree, TreeNode::Branch(ref name, _) if name == "cmd"));
    }

    #[test]
    fn build_completions_includes_subcommands_and_args() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let TreeNode::Branch(_, children) = &tree else { panic!("expected Branch") };
        let all_names: Vec<String> = children.iter().flat_map(|n| n.names()).collect();
        assert!(all_names.contains(&"--format".to_string()));
        assert!(all_names.contains(&"--verbose".to_string()));
        assert!(all_names.contains(&"install".to_string()));
        assert!(all_names.contains(&"remove".to_string()));
    }

    // --- complete(): top-level ---

    #[test]
    fn complete_empty_line_returns_all_top_level() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "");
        candidates.sort();
        assert_eq!(len, 0);
        assert_eq!(candidates, vec!["--format", "--verbose", "-v", "install", "remove"]);
    }

    #[test]
    fn complete_partial_subcommand_filters_and_returns_length() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, candidates) = complete(&tree, "ins");
        assert_eq!(len, 3);
        assert_eq!(candidates, vec!["install"]);
    }

    #[test]
    fn complete_partial_option_name_filters() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, candidates) = complete(&tree, "--fo");
        assert_eq!(len, 4);
        assert_eq!(candidates, vec!["--format"]);
    }

    #[test]
    fn complete_no_match_returns_empty() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (_, candidates) = complete(&tree, "xyz");
        assert!(candidates.is_empty());
    }

    #[test]
    fn complete_non_branch_root_returns_empty() {
        let cmd = make_command();
        let arg = cmd.get_arguments().next().unwrap();
        let node = TreeNode::Option(arg);
        let (len, candidates) = complete(&node, "");
        assert_eq!(len, 0);
        assert!(candidates.is_empty());
    }

    // --- complete(): subcommand navigation ---

    #[test]
    fn complete_after_subcommand_with_trailing_space() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "install ");
        candidates.sort();
        assert_eq!(len, 0);
        assert!(candidates.contains(&"--force".to_string()));
    }

    #[test]
    fn complete_partial_option_inside_subcommand() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, candidates) = complete(&tree, "install --fo");
        assert_eq!(len, 4);
        assert_eq!(candidates, vec!["--force"]);
    }

    #[test]
    fn complete_unknown_subcommand_returns_empty() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (_, candidates) = complete(&tree, "unknown ");
        assert!(candidates.is_empty());
    }

    // --- complete(): option value completion ---

    #[test]
    fn complete_option_enum_value_empty_partial() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "--format ");
        candidates.sort();
        assert_eq!(len, 0);
        assert_eq!(candidates, vec!["json", "plain", "table"]);
    }

    #[test]
    fn complete_option_enum_value_partial() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, candidates) = complete(&tree, "--format ta");
        assert_eq!(len, 2);
        assert_eq!(candidates, vec!["table"]);
    }

    #[test]
    fn complete_option_enum_value_no_match_returns_empty() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (_, candidates) = complete(&tree, "--format xyz");
        assert!(candidates.is_empty());
    }

    #[test]
    fn complete_after_option_and_value_consumed_shows_next_flags() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "--format json ");
        candidates.sort();
        assert_eq!(len, 0);
        assert!(candidates.contains(&"--verbose".to_string()));
        assert!(candidates.contains(&"-v".to_string()));
    }

    #[test]
    fn complete_option_value_fallback_when_partial_does_not_match_flag() {
        // "j" does not match any flag name, so falls back to matching enum values
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, candidates) = complete(&tree, "j");
        assert_eq!(len, 1);
        assert_eq!(candidates, vec!["json"]);
    }

    // --- complete(): positional completion ---

    #[test]
    fn complete_positional_enum_partial() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "install pkg-");
        candidates.sort();
        assert_eq!(len, 4);
        assert_eq!(candidates, vec!["pkg-a", "pkg-b", "pkg-c"]);
    }

    #[test]
    fn complete_positional_enum_empty_partial() {
        let cmd = make_command();
        let tree = build_completions(&cmd);
        let (len, mut candidates) = complete(&tree, "install ");
        candidates.sort();
        assert_eq!(len, 0);
        assert!(candidates.contains(&"pkg-a".to_string()));
        assert!(candidates.contains(&"pkg-b".to_string()));
    }

    /// Create a temp dir with a given set of files and subdirectories.
    /// Prefix entries with "/" to create directories.
    fn setup(entries: &[&str]) -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for entry in entries {
            if let Some(subdir) = entry.strip_prefix('/') {
                fs::create_dir(dir.path().join(subdir)).unwrap();
            } else {
                fs::File::create(dir.path().join(entry)).unwrap();
            }
        }
        dir
    }

    /// Prefix all expected names with the temp dir path, for convenience.
    fn prefixed(dir: &TempDir, names: &[&str]) -> Vec<String> {
        let base = dir.path().to_str().unwrap();
        names.iter().map(|n| format!("{}/{}", base, n)).collect()
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    // --- empty partial: list current dir ---

    #[test]
    fn empty_partial_lists_all() {
        let dir = setup(&["alpha.txt", "beta.txt", "/gamma"]);
        let partial = format!("{}/", dir.path().to_str().unwrap());
        let mut result = fs_candidates(&partial, false, false);
        result.sort();
        assert_eq!(result, sorted(prefixed(&dir, &["alpha.txt", "beta.txt", "gamma/"])));
    }

    // --- partial filename filter ---

    #[test]
    fn partial_filters_by_prefix() {
        let dir = setup(&["main.rs", "mod.rs", "lib.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = sorted(fs_candidates(&format!("{}/m", base), false, false));
        assert_eq!(result, sorted(prefixed(&dir, &["main.rs", "mod.rs"])));
    }

    #[test]
    fn partial_no_match_returns_empty() {
        let dir = setup(&["main.rs", "lib.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/z", base), false, false);
        assert!(result.is_empty());
    }

    #[test]
    fn exact_prefix_match_returns_single() {
        let dir = setup(&["unique.rs", "other.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/uni", base), false, false);
        assert_eq!(result, prefixed(&dir, &["unique.rs"]));
    }

    // --- directory trailing slash ---

    #[test]
    fn directories_get_trailing_slash() {
        let dir = setup(&["/subdir"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/sub", base), false, false);
        assert_eq!(result, prefixed(&dir, &["subdir/"]));
    }

    #[test]
    fn files_do_not_get_trailing_slash() {
        let dir = setup(&["readme.md"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/read", base), false, false);
        assert_eq!(result, prefixed(&dir, &["readme.md"]));
    }

    // --- dirs_only flag ---

    #[test]
    fn dirs_only_excludes_files() {
        let dir = setup(&["file.txt", "/subdir"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/", base), true, false);
        assert_eq!(result, prefixed(&dir, &["subdir/"]));
    }

    #[test]
    fn dirs_only_empty_when_no_dirs() {
        let dir = setup(&["file.txt", "other.txt"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/", base), true, false);
        assert!(result.is_empty());
    }

    // --- absolute paths ---

    #[test]
    fn absolute_path_no_double_slash() {
        let dir = setup(&["media.txt"]);
        let base = dir.path().to_str().unwrap();
        // Simulate typing e.g. "/med" where the root is our temp dir
        let partial = format!("{}/med", base);
        let result = fs_candidates(&partial, false, false);
        assert_eq!(result, vec![format!("{}/media.txt", base)]);
        // Crucially: no double slash anywhere
        assert!(result.iter().all(|s| !s.contains("//")));
    }

    #[test]
    fn trailing_slash_on_partial_dir_lists_contents() {
        let dir = setup(&["/src"]);
        fs::File::create(dir.path().join("src/main.rs")).unwrap();
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/src/", base), false, false);
        assert_eq!(result, vec![format!("{}/src/main.rs", base)]);
    }

    // --- nonexistent directory ---

    #[test]
    fn nonexistent_dir_returns_empty() {
        let result = fs_candidates("/nonexistent/path/xyz", false, false);
        assert!(result.is_empty());
    }

    // --- unclosed quote in complete() ---

    fn make_file_command() -> Command {
        Command::new("open")
            .disable_help_flag(true)
            .arg(Arg::new("file").value_hint(ValueHint::FilePath))
    }

    #[test]
    fn unclosed_double_quote_completes_partial_path() {
        let dir = setup(&["my rom.zip", "other.zip"]);
        let base = dir.path().to_str().unwrap();
        let cmd = make_file_command();
        let tree = build_completions(&cmd);
        // User typed: open "/tmp/.../my
        let partial_raw = format!("{}/my", base);
        let line = format!("open \"{}", partial_raw);
        let (len, candidates) = complete(&tree, &line);
        // len covers: opening quote + partial_raw contents
        assert_eq!(len, 1 + partial_raw.len());
        // in-quote file: replacement starts at the quote, so candidate includes opening + closing quote
        assert_eq!(candidates, vec![format!("'{}/my rom.zip'", base)]);
    }

    #[test]
    fn unclosed_single_quote_completes_partial_path() {
        let dir = setup(&["my rom.zip", "other.zip"]);
        let base = dir.path().to_str().unwrap();
        let cmd = make_file_command();
        let tree = build_completions(&cmd);
        let partial_raw = format!("{}/my", base);
        let line = format!("open '{}", partial_raw);
        let (len, candidates) = complete(&tree, &line);
        assert_eq!(len, 1 + partial_raw.len());
        assert_eq!(candidates, vec![format!("'{}/my rom.zip'", base)]);
    }

    #[test]
    fn unclosed_quote_with_no_prefix_tokens() {
        let dir = setup(&["my rom.zip", "other.zip"]);
        let base = dir.path().to_str().unwrap();
        let cmd = make_file_command();
        let tree = build_completions(&cmd);
        // Single-token command where the only token is a quoted partial (no subcommand navigation)
        // This simulates the case where there is just the open command and the quoted arg
        let line = format!("\"{}/my", base);
        // The tree root is "open", single token won't match, but should not panic
        let (_len, _candidates) = complete(&tree, &line);
        // Just verify it doesn't panic and returns reasonably
    }

    #[test]
    fn unclosed_quote_dir_candidate_leaves_quote_open() {
        let dir = setup(&["/my roms"]);
        let base = dir.path().to_str().unwrap();
        let cmd = make_file_command();
        let tree = build_completions(&cmd);
        let partial_raw = format!("{}/my", base);
        let line = format!("open '{}", partial_raw);
        let (_, candidates) = complete(&tree, &line);
        // Directory: replacement starts at the quote, candidate re-emits it but leaves it open
        assert_eq!(candidates, vec![format!("'{}/my roms/", base)]);
    }

    // --- quoting ---

    #[test]
    fn file_with_spaces_is_quoted() {
        let dir = setup(&["my rom.zip", "other.zip"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/my", base), false, false);
        assert_eq!(result, vec![format!("'{}/my rom.zip'", base)]);
    }

    #[test]
    fn directory_with_spaces_is_quoted() {
        let dir = setup(&["/my roms"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/my", base), false, false);
        // Directory with spaces: opening quote only, no closing quote — stays open for further typing
        assert_eq!(result, vec![format!("'{}/my roms/", base)]);
    }
}
