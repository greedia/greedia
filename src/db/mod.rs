mod access;
mod storage;
mod storage_tree;
pub mod tree; // KTODO: make private
pub mod types;

use std::path::Path;

pub use access::DbAccess;

use self::storage::InnerDb;

#[derive(Clone)]
pub struct Db {
    inner: InnerDb,
}

impl Db {
    pub fn new(db: sled::Db) -> Self {
        let inner = InnerDb::new(db);

        Self {
            inner: InnerDb::new(db),
        }
    }

    pub fn open(path: &Path) -> Result<Self, sled::Error> {
        sled::open(path).map(Self::new)
    }

    pub fn drive_access(&self, drive_type: &str, drive_id: &str) -> DbAccess {
        DbAccess::new(self.inner, drive_type, drive_id)
    }
}
