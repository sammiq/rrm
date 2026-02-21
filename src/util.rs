use anyhow::Result;
use sha1::{Digest, Sha1};

use camino::{Utf8Path, Utf8PathBuf};

#[cfg(windows)]
use std::os::windows::prelude::*;

#[cfg(all(unix, not(target_os = "macos")))]
pub fn data_dir() -> Option<Utf8PathBuf> {
    env_to_path("XDG_CONFIG_HOME").or_else(|| home_path(".local/share"))
}

#[cfg(not(target_os = "macos"))]
fn env_to_path(env_var: &str) -> Option<Utf8PathBuf> {
    std::env::var_os(env_var).and_then(|opath| {
        Utf8PathBuf::try_from(opath)
            .ok()
            .and_then(|path| path.canonicalize_utf8().ok())
    })
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

#[cfg(not(windows))]
pub fn is_hidden_file<P: AsRef<Utf8Path>>(file: P) -> bool {
    file.as_ref()
        .file_name()
        .map(|filename| filename.starts_with('.'))
        .unwrap_or_default()
}

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
