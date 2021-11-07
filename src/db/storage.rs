//! Storage side of database implementation. Currently using sled.
//! Everything handling the serialization side of things should be
//! handled in db/mod.rs or db/tree.rs.

pub use super::storage_tree::InnerTree;

#[derive(Debug, Clone)]
pub struct InnerDb {
    db: sled::Db,
}

impl InnerDb {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    pub fn tree(&self, name: &[u8]) -> InnerTree {
        let tree = self.db.open_tree(name).unwrap();
        InnerTree { tree }
    }
}
