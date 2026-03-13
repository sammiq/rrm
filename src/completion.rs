use clap::{Arg, Command};
use shlex::Shlex;

#[derive(Debug)]
pub enum TreeNode<'a> {
    Branch(String, Vec<TreeNode<'a>>),
    Option(&'a Arg),
    Positional(&'a Arg),
}

impl TreeNode<'_> {
    pub fn name(&self) -> String {
        match self {
            TreeNode::Branch(name, _) => name.clone(),
            TreeNode::Option(arg) => {
                if let Some(long) = arg.get_long() {
                    long.to_string()
                } else if let Some(short) = arg.get_short() {
                    short.to_string()
                } else {
                    unreachable!()
                }
            }
            TreeNode::Positional(arg) => arg.get_id().to_string(),
        }
    }

    pub fn is_positional(&self) -> bool {
        matches!(self, TreeNode::Positional(_))
    }
}

pub fn build_completions<'a>(command: &'a Command) -> TreeNode<'a> {
    build_branch(command)
}

fn build_branch<'a>(command: &'a Command) -> TreeNode<'a> {
    let mut arg_vec = Vec::new();
    for arg in command.get_arguments() {
        if let Some(_) = arg.get_long() {
            //option
            arg_vec.push(TreeNode::Option(arg));
        } else if let Some(_) = arg.get_short() {
            //option
            arg_vec.push(TreeNode::Option(arg));
        } else {
            //println!("{:?}", arg);
            arg_vec.push(TreeNode::Positional(arg));
        }
    }
    for sub in command.get_subcommands() {
        arg_vec.push(build_branch(sub))
    }
    TreeNode::Branch(command.get_name().to_string(), arg_vec)
}

pub fn complete<'a>(root_node: &'a TreeNode<'a>, line: &str) -> Vec<String> {
    // Parse the line into tokens, handling quotes/escaping via shlex
    let tokens: Vec<String> = Shlex::new(line).collect();

    // Determine if the line ends with whitespace (i.e. the last token is "complete"
    // and we want completions *after* it, not *of* it).
    let trailing_space = line.ends_with(' ') || line.is_empty();

    // Start from the root's children (rule 2: ignore root itself)
    let children = match root_node {
        TreeNode::Branch(_, children) => children,
        _ => return vec![],
    };

    if tokens.is_empty() || (tokens.len() == 1 && !trailing_space) {
        // No tokens yet, or a single partial token: complete against root's children
        let partial = tokens.first().map(String::as_str).unwrap_or("");
        return candidates_matching(children, partial);
    }

    // Walk the tree consuming fully-matched tokens, stopping one before the end
    // so we can apply partial-match / next-token logic at the final level.
    let (walk_tokens, final_token) = if trailing_space {
        // Every token was fully typed; final_token is an empty partial (show all next)
        (tokens.as_slice(), "")
    } else {
        // Last token is still being typed
        let (last, rest) = tokens.split_last().unwrap();
        (rest, last.as_str())
    };

    // Traverse the tree following fully-matched tokens
    let mut current_children: &[TreeNode] = children;
    for token in walk_tokens {
        match find_exact(current_children, token) {
            Some(TreeNode::Branch(_, next_children)) => {
                current_children = next_children;
            }
            // Matched a leaf (Option / Positional) — nothing deeper to traverse
            Some(_) => return vec![],
            // No match at this level — no completions possible
            None => return vec![],
        }
    }

    candidates_matching(current_children, final_token)
}

/// Return all sibling names that start with `partial` (empty = return all).
fn candidates_matching<'a>(nodes: &'a [TreeNode<'a>], partial: &str) -> Vec<String> {
    nodes
        .iter()
        .map(|n| n.name().to_owned())
        .filter(|name| name.starts_with(partial))
        .collect()
}

/// Find the first node whose name exactly equals `token`.
fn find_exact<'a, 'b>(nodes: &'a [TreeNode<'b>], token: &str) -> Option<&'a TreeNode<'b>> {
    nodes.iter().find(|n| n.name() == token)
}
