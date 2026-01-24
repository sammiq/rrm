use std::path::Path;
use std::path::PathBuf;

#[cfg(all(unix, not(target_os = "macos")))]
pub fn data_dir() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .and_then(|path| PathBuf::from(&path).canonicalize().ok())
        .or_else(|| std::env::home_dir().map(|home| home.join(".local/share")))
}

#[cfg(target_os = "windows")]
pub fn data_dir() -> Option<PathBuf> {
    std::env::var_os("APPDATA").and_then(|path| PathBuf::from(&path).canonicalize().ok())
}

#[cfg(target_os = "macos")]
pub fn data_dir() -> Option<PathBuf> {
    std::env::home_dir().map(|home| home.join("Library/Application Support"))
}

#[cfg(windows)]
pub fn is_hidden_file<P: AsRef<Path>>(file: P) -> bool {
    file.as_ref()
        .metadata()
        .map(|metadata| metadata.file_attributes() & 0x00000002 != 0)
        .unwrap_or_default()
}

#[cfg(not(windows))]
pub fn is_hidden_file<P: AsRef<Path>>(file: P) -> bool {
    file.as_ref()
        .file_name()
        .map(|filename| filename.as_encoded_bytes().starts_with(".".as_bytes()))
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
    return format!("{h_size} TB");
}
