use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Clone)]
pub struct CfKeys {
    pub access_key: String,
    pub inode_key: String,
    pub lookup_key: String,
    pub raccess_key: String,
    pub scan_key: String,
}

impl CfKeys {
    pub fn new(drive_id: &str) -> CfKeys {
        CfKeys {
            // For accessing a file by ID
            access_key: format!("access:{}", drive_id),
            // For accessing a file by inode
            inode_key: format!("inode:{}", drive_id),
            // For accessing a file by a filename within inode
            lookup_key: format!("lookup:{}", drive_id),
            // For finding references to update upon file deletion
            raccess_key: format!("raccess:{}", drive_id),
            // For keeping track of scans
            scan_key: format!("scan:{}", drive_id),
        }
    }

    pub fn as_vec(drive_id: &str) -> Vec<String> {
        vec![
            // For accessing a file by ID
            format!("access:{}", drive_id),
            // For accessing a file by inode
            format!("inode:{}", drive_id),
            // For accessing a file by a filename within inode
            format!("lookup:{}", drive_id),
            // For finding references to update upon file deletion
            format!("raccess:{}", drive_id),
            // For keeping track of scans
            format!("scan:{}", drive_id),
        ]
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub files: Option<Vec<PageItem>>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageItem {
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
    pub file_info: Option<FileInfo>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub md5: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub enum DataIdentifier {
    GlobalMd5(Vec<u8>),
    // DriveUnique(Vec<u8>)  // probably needed for future S3, etc support
}
