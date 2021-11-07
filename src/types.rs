use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdrivePage {
    pub files: Option<Vec<GdrivePageItem>>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdrivePageItem {
    pub id: String,
    pub name: String,
    pub parents: Vec<String>,
    pub modified_time: String,
    pub md5_checksum: Option<String>,
    pub size: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScannedItem {
    pub id: String,
    pub name: String,
    pub parent: String,
    pub modified_time: DateTime<Utc>,
    pub file_info: Option<GdriveFileInfo>,
}

#[derive(Debug, Clone)]
pub struct GdriveFileInfo {
    pub md5: String,
    pub size: u64,
}
