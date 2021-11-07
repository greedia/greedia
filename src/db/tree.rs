use super::storage::InnerDb;
pub use super::storage_tree::InnerTree;

#[derive(Clone)]
pub struct Trees {
    /// Variables used for drive scanning
    pub(super) scan: InnerTree,
    /// Mapping from access ID to inode
    pub(super) access: InnerTree,
    /// Mapping from inode to DriveItem
    pub(super) inode: InnerTree,
    /// Mapping from parent and filename to inode
    pub(super) lookup: InnerTree,
    /// Mapping from access ID to ReverseAccess struct
    pub(super) raccess: InnerTree,
}

impl Trees {
    pub fn new(db: InnerDb, drive_type: &str, drive_id: &str) -> Self {
        let tree_keys = TreeKeys::new(drive_type, drive_id);
        Trees {
            scan: db.tree(&tree_keys.scan_key),
            access: db.tree(&tree_keys.access_key),
            inode: db.tree(&tree_keys.inode_key),
            lookup: db.tree(&tree_keys.lookup_key),
            raccess: db.tree(&tree_keys.raccess_key),
        }
    }
}

pub struct TreeKeys {
    // For keeping track of scans
    pub scan_key: Vec<u8>,
    // For accessing a file by ID
    pub access_key: Vec<u8>,
    // For accessing a file by inode
    pub inode_key: Vec<u8>,
    // For accessing a file by a filename within inode
    pub lookup_key: Vec<u8>,
    // For finding references to update upon file deletion
    pub raccess_key: Vec<u8>,
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
        }
    }

    fn get_key(name: &str, trailer: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(name.len() + trailer.len());
        key.extend_from_slice(name.as_bytes());
        key.extend_from_slice(trailer);
        key
    }
}
