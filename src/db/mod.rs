mod access;
mod lru_access;
mod storage;
mod storage_tree;
mod tree; // KTODO: make private
pub mod types;

use std::path::{Path, PathBuf};

pub use access::DbAccess;
pub use lru_access::LruAccess;

use self::storage::InnerDb;

#[derive(Clone)]
pub struct Db {
    inner: InnerDb,
}

impl Db {
    pub fn new(db: sled::Db) -> Self {
        let inner = InnerDb::new(db);

        Self { inner }
    }

    pub fn open(path: &Path) -> Result<Self, sled::Error> {
        sled::open(path).map(Self::new)
    }

    pub fn drive_access(&self, drive_type: &str, drive_id: &str) -> DbAccess {
        DbAccess::new(self.inner.clone(), drive_type, drive_id)
    }

    pub fn lru_access(&self, cache_root: PathBuf, size_limit: u64) -> LruAccess {
        LruAccess::new(self.inner.clone(), cache_root, size_limit)
    }
}
