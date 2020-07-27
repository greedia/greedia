use std::{
    cmp::min,
    collections::HashMap,
};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub caching: CachingConfig,
    pub smart_cachers: HashMap<String, SmartCacherConfig>,
    pub generic_cacher: GenericCache,
    pub gdrive: HashMap<String, ConfigGoogleDrive>,
}

#[derive(Debug, Deserialize)]
pub struct CachingConfig {
    pub cache_path: String,
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

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigGoogleDrive {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    pub drive_id: String,
    pub root_path: Option<String>,
    /// rclone crypt passwords used for encrypting names and files
    pub password: Option<String>,
    pub password2: Option<String>,
}
