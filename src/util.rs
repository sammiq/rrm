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
/// This is similar to  Result::inspect and Result::inspect_err
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
/// This is similar to Option::inspect except that it destructs the Option,
/// and takes ownership of the value.
/// This is by design as it eases readability of code using the construct, and
/// forces users to use match statements where appropriate.
pub trait OptionIf<T> {
    fn if_some<F: FnOnce(T)>(self, op: F);
}

impl<T> OptionIf<T> for std::option::Option<T> {
    fn if_some<F: FnOnce(T)>(self, op: F) {
        if let Some(t) = self {
            op(t);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // --- is_hidden_file ---

    #[cfg(not(windows))]
    #[test]
    fn hidden_file_dotfile_is_hidden() {
        assert!(is_hidden_file(".bashrc"));
    }

    #[cfg(not(windows))]
    #[test]
    fn hidden_file_regular_file_is_not_hidden() {
        assert!(!is_hidden_file("readme.txt"));
    }

    #[cfg(not(windows))]
    #[test]
    fn hidden_file_dotfile_in_subdir_is_not_hidden() {
        // the file name component ("foo") doesn't start with '.'
        assert!(!is_hidden_file(".config/foo"));
    }

    #[cfg(not(windows))]
    #[test]
    fn hidden_file_dotdir_itself_is_hidden() {
        assert!(is_hidden_file(".config"));
    }

    // --- is_zip_file ---

    #[test]
    fn zip_file_lowercase_extension() {
        assert!(is_zip_file("archive.zip"));
    }

    #[test]
    fn zip_file_uppercase_extension() {
        assert!(is_zip_file("archive.ZIP"));
    }

    #[test]
    fn zip_file_mixed_case_extension() {
        assert!(is_zip_file("archive.Zip"));
    }

    #[test]
    fn zip_file_no_extension() {
        assert!(!is_zip_file("archive"));
    }

    #[test]
    fn zip_file_different_extension() {
        assert!(!is_zip_file("archive.tar"));
    }

    // --- has_extension ---

    #[test]
    fn has_extension_match() {
        assert!(has_extension("photo.jpg", &["jpg", "jpeg", "png"]));
    }

    #[test]
    fn has_extension_case_insensitive() {
        assert!(has_extension("photo.JPG", &["jpg", "jpeg", "png"]));
    }

    #[test]
    fn has_extension_no_match() {
        assert!(!has_extension("photo.gif", &["jpg", "jpeg", "png"]));
    }

    #[test]
    fn has_extension_no_extension() {
        assert!(!has_extension("photo", &["jpg"]));
    }

    // --- human_size ---

    #[test]
    fn human_size_bytes() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(1023), "1023 B");
    }

    #[test]
    fn human_size_kilobytes() {
        assert_eq!(human_size(1024), "1 KB");
        assert_eq!(human_size(2048), "2 KB");
    }

    #[test]
    fn human_size_megabytes() {
        assert_eq!(human_size(1024 * 1024), "1 MB");
    }

    #[test]
    fn human_size_gigabytes() {
        assert_eq!(human_size(1024 * 1024 * 1024), "1 GB");
    }

    #[test]
    fn human_size_terabytes() {
        assert_eq!(human_size(1024u64 * 1024 * 1024 * 1024), "1 TB");
    }

    // --- calc_hash ---

    #[test]
    fn calc_hash_known_value() {
        // SHA1("") == da39a3ee5e6b4b0d3255bfef95601890afd80709
        let mut input: &[u8] = b"";
        let (hash, size) = calc_hash(&mut input).unwrap();
        assert_eq!(hash, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(size, 0);
    }

    #[test]
    fn calc_hash_hello_world() {
        let mut input: &[u8] = b"hello world";
        let (hash, size) = calc_hash(&mut input).unwrap();
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
        assert_eq!(size, 11);
    }

    // --- ResultIf ---

    #[test]
    fn result_if_ok_calls_closure_on_ok() {
        let mut called = false;
        let r: Result<i32, &str> = Ok(42);
        r.if_ok(|v| {
            assert_eq!(v, 42);
            called = true;
        });
        assert!(called);
    }

    #[test]
    fn result_if_ok_does_not_call_closure_on_err() {
        let mut called = false;
        let r: Result<i32, &str> = Err("oops");
        r.if_ok(|_| called = true);
        assert!(!called);
    }

    #[test]
    fn result_if_err_calls_closure_on_err() {
        let mut called = false;
        let r: Result<i32, &str> = Err("oops");
        r.if_err(|e| {
            assert_eq!(e, "oops");
            called = true;
        });
        assert!(called);
    }

    #[test]
    fn result_if_err_does_not_call_closure_on_ok() {
        let mut called = false;
        let r: Result<i32, &str> = Ok(1);
        r.if_err(|_| called = true);
        assert!(!called);
    }

    // --- OptionIf ---

    #[test]
    fn option_if_some_calls_closure_on_some() {
        let mut called = false;
        let o: Option<i32> = Some(7);
        o.if_some(|v| {
            assert_eq!(v, 7);
            called = true;
        });
        assert!(called);
    }

    #[test]
    fn option_if_some_does_not_call_closure_on_none() {
        let mut called = false;
        let o: Option<i32> = None;
        o.if_some(|_| called = true);
        assert!(!called);
    }
}
