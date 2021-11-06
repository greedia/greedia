use std::{cmp::min, collections::HashMap, path::PathBuf};

use serde::Deserialize;

/// Tweaks are development or testing values used to modify the code.
/// Every value must have a default.
#[derive(Clone, Debug, Deserialize)]
pub struct Tweaks {
    /// Enable directory caching in the kernel. File contents will
    /// be cached, but directory listings and file metadata such
    /// as filenames will not be.
    #[serde(default)]
    pub enable_kernel_dir_caching: bool,

    /// Mount the FUSE endpoint as read-only. This will eventually
    /// be a regular config option, but for now, use a tweak.
    #[serde(default)]
    pub mount_read_only: bool,
}

impl Default for Tweaks {
    fn default() -> Self {
        Self {
            enable_kernel_dir_caching: false,
            mount_read_only: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub tweaks: Option<Tweaks>,
    pub caching: CachingConfig,
    pub smart_cachers: HashMap<String, SmartCacherConfig>,
    pub generic_cacher: GenericCache,
    pub gdrive: Option<HashMap<String, ConfigGoogleDrive>>,
    pub timecode: Option<HashMap<String, ConfigTimecodeDrive>>,
}

#[derive(Debug, Deserialize)]
pub struct CachingConfig {
    pub db_path: PathBuf,
    pub mount_point: PathBuf,
    pub soft_cache_limit: u64,
    pub min_size: u64,
    pub use_smart_caching: bool,
    pub use_generic_caching: bool,
}

#[derive(Debug, Deserialize)]
pub struct GenericCache {
    pub start: DownloadAmount,
    pub end: DownloadAmount,
}

#[derive(Debug, Deserialize)]
pub struct SoftCache {
    pub limit: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadAmount {
    pub percent: Option<f32>,
    pub bytes: Option<u64>,
}

/// SmartCacher configuration.
#[derive(Debug, Deserialize)]
pub struct SmartCacherConfig {
    /// Whether or not this SmartCacher is enabled.
    /// If the SmartCacher is reading this, it's true.
    pub enabled: bool,
    /// Approximate number of starting seconds to hard cache.
    pub seconds: u64,
}

impl DownloadAmount {
    pub fn with_size(&self, size: u64) -> u64 {
        let mut out = size;

        if let Some(percent) = self.percent {
            let percent_bytes = ((percent / 100.0) * (size as f32)).round() as u64;
            out = min(out, percent_bytes);
        }

        if let Some(bytes) = self.bytes {
            out = min(out, bytes);
        }

        out
    }
}

/// Configuration for a Google Drive.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigGoogleDrive {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    pub drive_id: String,
    pub root_path: Option<String>,
    /// service account json files that can access the drive
    pub service_accounts: Option<Vec<PathBuf>>,
    /// rclone crypt passwords used for encrypting names and files
    pub password: Option<String>,
    pub password2: Option<String>,
}

/// Configuration for a Timecode drive. This is mostly used for testing, and is hard-coded.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigTimecodeDrive {
    pub drive_id: String,
}

pub fn validate_config(_cfg: &Config) -> bool {
    // TODO: make sure no drive names are duplicated
    true
}
