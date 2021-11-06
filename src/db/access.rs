use rkyv::archived_root;

use super::{storage::InnerDb, tree::Trees};
use super::types::*;

/// Used to access and perform various actions on the database.
/// Handles both DB storage and serialization.
pub struct DbAccess {
    trees: Trees
}

impl DbAccess {
    pub fn new(db: InnerDb, drive_type: &str, drive_id: &str) -> Self {
        DbAccess { trees: Trees::new(db, drive_type, drive_id) }
    }

    pub fn get_inode(&self, inode: u64) -> ArchivedDriveItem {
        let data = self.trees.inode.get(inode.to_le_bytes());
        let archived = unsafe { archived_root(&data) };
        archived
    }
}