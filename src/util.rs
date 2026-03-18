use anyhow::Result;
use sha1::{Digest, Sha1};

use camino::{Utf8Path, Utf8PathBuf};

#[cfg(windows)]
use std::os::windows::prelude::*;
use std::path::Path;

#[cfg(all(unix, not(target_os = "macos")))]
pub fn data_dir() -> Option<Utf8PathBuf> {
    env_to_path("XDG_DATA_HOME").or_else(|| home_path(".local/share"))
}

#[cfg(not(target_os = "macos"))]
fn env_to_path(env_var: &str) -> Option<Utf8PathBuf> {
    std::env::var_os(env_var).and_then(canonical_path)
}

#[cfg(unix)]
fn home_path(dirname: &str) -> Option<Utf8PathBuf> {
    std::env::home_dir()
        .and_then(|home| Utf8PathBuf::try_from(home).ok())
        .map(|home| home.join(dirname))
}

#[cfg(target_os = "windows")]
pub fn data_dir() -> Option<Utf8PathBuf> {
    env_to_path("APPDATA")
}

#[cfg(target_os = "macos")]
pub fn data_dir() -> Option<Utf8PathBuf> {
    home_path("Library/Application Support")
}

#[cfg(windows)]
pub fn is_hidden_file<P: AsRef<Utf8Path>>(file: P) -> bool {
    file.as_ref()
        .metadata()
        .map(|metadata| metadata.file_attributes() & 0x00000002 != 0)
        .unwrap_or_default()
}

pub fn canonical_path<P: AsRef<Path>>(path: P) -> Option<Utf8PathBuf> {
    path.as_ref()
        .canonicalize()
        .ok()
        .and_then(|path| Utf8PathBuf::try_from(path).ok())
}

#[cfg(not(windows))]
pub fn is_hidden_file<P: AsRef<Utf8Path>>(file: P) -> bool {
    file.as_ref()
        .file_name()
        .is_some_and(|filename| filename.starts_with('.'))
}

pub fn is_zip_file<P: AsRef<Utf8Path>>(file: P) -> bool {
    file.as_ref()
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
}

pub fn has_extension<P: AsRef<Utf8Path>, S: AsRef<str>>(file: P, extensions: &[S]) -> bool {
    file.as_ref()
        .extension()
        .is_some_and(|ext| extensions.iter().any(|e| ext.eq_ignore_ascii_case(e.as_ref())))
}

/// Print the size of a value, prefixing with K, M, G or T if required.
pub fn human_size(size: u64) -> String {
    let mut h_size = size;
    for unit in ["", "K", "M", "G"] {
        if h_size < 1024 {
            return format!("{h_size} {unit}B");
        }
        h_size /= 1024
    }
    format!("{h_size} TB")
}

pub fn calc_hash<R: std::io::Read + ?Sized>(reader: &mut R) -> Result<(String, u64)> {
    let mut hasher = Sha1::new();
    let size = std::io::copy(reader, &mut hasher)?;
    let digest = hasher.finalize();
    let hash = base16ct::lower::encode_string(&digest);
    Ok((hash, size))
}

/// This trait allows inline inspection of parts of a result.
/// This is similar to the unstable Result::inspect and Result::inspect_err
/// except that it destructs the result, and takes ownership of the value or error.
/// This is by design as it eases readability of code using the construct, and
/// forces users to use match statements where appropriate.
#[allow(dead_code)]
pub trait ResultIf<T, E> {
    fn if_ok<F: FnOnce(T)>(self, op: F);
    fn if_err<F: FnOnce(E)>(self, op: F);
}

impl<T, E> ResultIf<T, E> for std::result::Result<T, E> {
    fn if_ok<F: FnOnce(T)>(self, op: F) {
        if let Ok(t) = self {
            op(t);
        }
    }

    fn if_err<F: FnOnce(E)>(self, op: F) {
        if let Err(e) = self {
            op(e);
        }
    }
}

/// This trait allows inline inspection of parts of an option.
/// This is similar to the unstable Option::inspect
/// except that it destructs the Option, and takes ownership of the value.
/// This is by design as it eases readability of code using the construct, and
/// forces users to use match statements where appropriate.
#[allow(dead_code)]
pub trait OptionIf<T> {
    fn if_some<F: FnOnce(T)>(self, op: F);
    fn if_none<F: FnOnce()>(self, op: F);
}

impl<T> OptionIf<T> for std::option::Option<T> {
    fn if_some<F: FnOnce(T)>(self, op: F) {
        if let Some(t) = self {
            op(t);
        }
    }

    fn if_none<F: FnOnce()>(self, op: F) {
        if self.is_none() {
            op();
        }
    }
}
