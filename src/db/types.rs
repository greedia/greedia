use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct DriveItem {
    pub access_id: String,
    pub modified_time: i64,
    pub data: DriveItemData,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct DirItem {
    pub name: String,
    pub access_id: String,
    pub inode: u64,
    pub is_dir: bool,
}

/// Identifier used to find the cache storage on disk for a particular file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DataIdentifier {
    /// Data is referred to globally within Greedia by an md5 hash.
    /// This is what gdrive uses.
    GlobalMd5(Vec<u8>),
    // /// Data is referred to by a drive-specific ID.
    // DriveUnique(Vec<u8>, Vec<u8>), // probably needed for future S3, etc support
    /// The sctest functionality doesn't store cache data, so it can use None.
    #[cfg(feature = "sctest")]
    None,
}

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct ReverseAccess {
    pub parent_inode: u64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
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