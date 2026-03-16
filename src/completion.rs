use camino::Utf8Path;
use clap::{Arg, Command, ValueHint};

#[derive(Debug)]
pub enum TreeNode<'a> {
    Branch(String, Vec<TreeNode<'a>>),
    Option(&'a Arg),
    Positional(&'a Arg),
}

impl TreeNode<'_> {
    pub fn names(&self) -> Vec<String> {
        match self {
            TreeNode::Branch(name, _) => vec![name.clone()],
            TreeNode::Option(arg) => {
                //we assume here that anything without a long or short option
                //is actually a positional
                let mut names = Vec::new();
                if let Some(long) = arg.get_long() {
                    names.push(format!("--{long}"));
                }
                if let Some(short) = arg.get_short() {
                    names.push(format!("-{short}"));
                }
                names
            }
            TreeNode::Positional(arg) => vec![arg.get_id().to_string()],
        }
    }
}

pub fn build_completions<'a>(command: &'a Command) -> TreeNode<'a> {
    build_branch(command)
}

fn build_branch<'a>(command: &'a Command) -> TreeNode<'a> {
    let mut arg_vec = Vec::new();
    for arg in command.get_arguments() {
        if arg.is_positional() {
            arg_vec.push(TreeNode::Positional(arg));
        } else {
            arg_vec.push(TreeNode::Option(arg));
        }
    }
    for sub in command.get_subcommands() {
        arg_vec.push(build_branch(sub))
    }
    TreeNode::Branch(command.get_name().to_string(), arg_vec)
}

/// Returns (length of partial, candidates).
pub fn complete(root_node: &TreeNode, line: &str) -> (usize, Vec<String>) {
    let tokens = shlex::split(line).unwrap_or_default();
    let trailing_space = line.ends_with(' ') || line.is_empty();

    let children = match root_node {
        TreeNode::Branch(_, children) => children,
        _ => return (0, vec![]),
    };

    if tokens.is_empty() || (tokens.len() == 1 && !trailing_space) {
        let partial = tokens.first().map(String::as_str).unwrap_or("");
        return (partial.len(), candidates_matching(children, partial));
    }

    let (walk_tokens, final_token) = if trailing_space {
        (tokens.as_slice(), "")
    } else {
        let (last, rest) = tokens.split_last().unwrap();
        (rest, last.as_str())
    };

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
                        return (final_token.len(), match_arg_value(arg, final_token));
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

    (final_token.len(), candidates_matching(current_children, final_token))
}

fn num_arg_values(arg: &Arg) -> usize {
    match arg.get_num_args() {
        Some(range) => range.min_values(),
        None => 1, // clap default: one value
    }
}

/// Find the first node whose name exactly equals `token`.
fn find_exact<'a, 'b>(nodes: &'a [TreeNode<'b>], token: &str) -> Option<&'a TreeNode<'b>> {
    nodes.iter().find(|n| n.names().iter().any(|name| name == token))
}

/// Simple name-based prefix match used for Branch and Option nodes,
/// and Positional nodes without a path hint.
fn find_partial(node: &TreeNode, partial: &str) -> Vec<String> {
    node.names()
        .into_iter()
        .filter(|name| name.starts_with(partial))
        .collect()
}

fn candidates_matching<'a>(nodes: &'a [TreeNode<'a>], partial: &str) -> Vec<String> {
    nodes
        .iter()
        .flat_map(|n| match n {
            TreeNode::Option(arg) | TreeNode::Positional(arg) => {
                let possible = match_arg_value(arg, partial);
                if possible.is_empty() {
                    // No enum values — fall back to name-based match (the flag itself)
                    find_partial(n, partial)
                } else {
                    possible
                }
            }
            _ => find_partial(n, partial),
        })
        .collect()
}

fn match_arg_value(arg: &Arg, partial: &str) -> Vec<String> {
    match arg.get_value_hint() {
        ValueHint::FilePath | ValueHint::AnyPath => fs_candidates(partial, false),
        ValueHint::DirPath => fs_candidates(partial, true),
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
fn fs_candidates(partial: &str, dirs_only: bool) -> Vec<String> {
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
            Some(candidate)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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
        let mut result = fs_candidates(&partial, false);
        result.sort();
        assert_eq!(result, sorted(prefixed(&dir, &["alpha.txt", "beta.txt", "gamma/"])));
    }

    // --- partial filename filter ---

    #[test]
    fn partial_filters_by_prefix() {
        let dir = setup(&["main.rs", "mod.rs", "lib.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = sorted(fs_candidates(&format!("{}/m", base), false));
        assert_eq!(result, sorted(prefixed(&dir, &["main.rs", "mod.rs"])));
    }

    #[test]
    fn partial_no_match_returns_empty() {
        let dir = setup(&["main.rs", "lib.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/z", base), false);
        assert!(result.is_empty());
    }

    #[test]
    fn exact_prefix_match_returns_single() {
        let dir = setup(&["unique.rs", "other.rs"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/uni", base), false);
        assert_eq!(result, prefixed(&dir, &["unique.rs"]));
    }

    // --- directory trailing slash ---

    #[test]
    fn directories_get_trailing_slash() {
        let dir = setup(&["/subdir"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/sub", base), false);
        assert_eq!(result, prefixed(&dir, &["subdir/"]));
    }

    #[test]
    fn files_do_not_get_trailing_slash() {
        let dir = setup(&["readme.md"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/read", base), false);
        assert_eq!(result, prefixed(&dir, &["readme.md"]));
    }

    // --- dirs_only flag ---

    #[test]
    fn dirs_only_excludes_files() {
        let dir = setup(&["file.txt", "/subdir"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/", base), true);
        assert_eq!(result, prefixed(&dir, &["subdir/"]));
    }

    #[test]
    fn dirs_only_empty_when_no_dirs() {
        let dir = setup(&["file.txt", "other.txt"]);
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/", base), true);
        assert!(result.is_empty());
    }

    // --- absolute paths ---

    #[test]
    fn absolute_path_no_double_slash() {
        let dir = setup(&["media.txt"]);
        let base = dir.path().to_str().unwrap();
        // Simulate typing e.g. "/med" where the root is our temp dir
        let partial = format!("{}/med", base);
        let result = fs_candidates(&partial, false);
        assert_eq!(result, vec![format!("{}/media.txt", base)]);
        // Crucially: no double slash anywhere
        assert!(result.iter().all(|s| !s.contains("//")));
    }

    #[test]
    fn trailing_slash_on_partial_dir_lists_contents() {
        let dir = setup(&["/src"]);
        fs::File::create(dir.path().join("src/main.rs")).unwrap();
        let base = dir.path().to_str().unwrap();
        let result = fs_candidates(&format!("{}/src/", base), false);
        assert_eq!(result, vec![format!("{}/src/main.rs", base)]);
    }

    // --- nonexistent directory ---

    #[test]
    fn nonexistent_dir_returns_empty() {
        let result = fs_candidates("/nonexistent/path/xyz", false);
        assert!(result.is_empty());
    }
}
