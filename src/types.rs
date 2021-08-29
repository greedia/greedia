use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::Deserialize;

pub struct TreeKeys {
    // For accessing a file by ID
    pub access_key: Vec<u8>,
    // For accessing a file by inode
    pub inode_key: Vec<u8>,
    // For accessing a file by a filename within inode
    pub lookup_key: Vec<u8>,
    // For finding references to update upon file deletion
    pub raccess_key: Vec<u8>,
    // For keeping track of scans
    pub scan_key: Vec<u8>,
    // For getting the status of a file's hard cache
    pub hc_meta_key: Vec<u8>,
}

impl TreeKeys {
    pub fn new(drive_type: &str, drive_id: &str) -> TreeKeys {
        let mut trailer = Vec::with_capacity(drive_type.len() + drive_id.len() + 2);
        trailer.extend_from_slice(b":");
        trailer.extend_from_slice(drive_type.as_bytes());
        trailer.extend_from_slice(b":");
        trailer.extend_from_slice(drive_id.as_bytes());

        TreeKeys {
            access_key: Self::get_key("access", &trailer),
            inode_key: Self::get_key("inode", &trailer),
            lookup_key: Self::get_key("lookup", &trailer),
            raccess_key: Self::get_key("raccess", &trailer),
            scan_key: Self::get_key("scan", &trailer),
            hc_meta_key: Self::get_key("hc_meta", &trailer),
        }
    }

    fn get_key(name: &str, trailer: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(name.len() + trailer.len());
        key.extend_from_slice(name.as_bytes());
        key.extend_from_slice(trailer);
        key
    }
}

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

/// Identifier used to find the cache storage on disk for a particular file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
#[non_exhaustive]
pub enum DataIdentifier {
    /// Data is referred to globally within Greedia by an md5 hash.
    GlobalMd5(Vec<u8>),
    // /// Data is referred to by a drive-specific ID.
    // DriveUnique(Vec<u8>, Vec<u8>), // probably needed for future S3, etc support
    /// The sctest functionality doesn't store cache data, so it can use None.
    #[cfg(feature = "sctest")]
    None,
}

#[derive(Debug, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DriveItem {
    pub access_id: String,
    pub modified_time: i64,
    pub data: DriveItemData,
}

#[derive(Debug, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ReverseAccess {
    pub parent_inode: u64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum DriveItemData {
    FileItem {
        file_name: String,
        data_id: DataIdentifier,
        size: u64,
    },
    Dir {
        items: Vec<DirItem>,
    },
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DirItem {
    pub name: String,
    pub access_id: String,
    pub inode: u64,
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct ScanState {
    last_page_token: String,
    last_modified_date: i64,
}

/// Generate an internal lookup key, given a parent inode and name.
pub fn make_lookup_key(parent: u64, name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + name.len());
    let parent_slice = parent.to_le_bytes();
    out.extend_from_slice(&parent_slice);
    out.extend_from_slice(name.as_bytes());
    out
}
