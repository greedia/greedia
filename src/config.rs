use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub cache_path: String,
    pub soft_cache: SoftCache,
    pub dl: HashMap<String, DownloadAmount>,
    pub drive: HashMap<String, ConfigDrive>,
}

#[derive(Debug, Deserialize)]
pub struct SoftCache {
    pub limit: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadAmount {
    pub max_percent: Option<f32>,
    pub max_bytes: Option<u64>,
}

impl DownloadAmount {
    pub fn with_size(&self, size: u64) -> u64 {
        let mut out = size;

        if let Some(max_percent) = self.max_percent {
            let percent_bytes = ((max_percent / 100.0) * (size as f32)).round() as u64;
            if out > percent_bytes {
                out = percent_bytes;
            }
        }

        if let Some(max_bytes) = self.max_bytes {
            if out > max_bytes {
                out = max_bytes;
            }
        }

        out
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigDrive {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    pub drive_id: String,
    pub root_path: Option<String>,
    /// rclone crypt passwords used for encrypting names and files
    pub password: Option<String>,
    pub password2: Option<String>,
}
