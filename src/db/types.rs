use rkyv::{
    ser::{serializers::AllocSerializer, Serializer},
    Archive, Deserialize, Infallible, Serialize,
};

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
#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
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

impl std::fmt::Debug for DataIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GlobalMd5(arg0) => {
                let md5_hex = hex::encode(arg0);
                f.write_fmt(format_args!("md5({md5_hex})"))
            }
            #[cfg(feature = "sctest")]
            Self::None => write!(f, "None"),
        }
    }
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

/// Key used for the LruTimestamp database tree.
#[derive(Debug)]
pub struct LruTimestampKey {
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Extra value, for deduplication.
    pub extra_val: u8,
}

impl LruTimestampKey {
    pub fn to_bytes(&self) -> [u8; 9] {
        let mut out = [0u8; 9];
        out[..8].copy_from_slice(&self.timestamp.to_be_bytes());
        out[8..].copy_from_slice(&self.extra_val.to_be_bytes());
        out
    }
}

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct LruTimestampData {
    pub data_id: DataIdentifier,
    pub offset: u64,
}

impl LruTimestampData {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner().to_vec()
    }
}

/// Key used for the LruData database tree.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct LruDataKey {
    #[archive(derive(Clone))]
    pub data_id: DataIdentifier,
    pub offset: u64,
}

impl LruDataKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner().to_vec()
    }
}

/// Data used for the LruData database tree.
#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct LruDataData {
    pub timestamp_key: [u8; 9],
}

impl LruDataData {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(self).unwrap();
        serializer.into_serializer().into_inner().to_vec()
    }
}

impl From<&ArchivedDataIdentifier> for DataIdentifier {
    fn from(val: &ArchivedDataIdentifier) -> Self {
        val.deserialize(&mut Infallible).unwrap()
    }
}

impl From<&ArchivedDirItem> for DirItem {
    fn from(val: &ArchivedDirItem) -> Self {
        val.deserialize(&mut Infallible).unwrap()
    }
}
impl From<&ArchivedDriveItemData> for DriveItemData {
    fn from(val: &ArchivedDriveItemData) -> Self {
        val.deserialize(&mut Infallible).unwrap()
    }
}
