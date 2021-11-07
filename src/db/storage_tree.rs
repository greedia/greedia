use sled::{IVec, Iter};

#[derive(Debug, Clone)]
pub struct InnerTree {
    pub tree: sled::Tree,
}

impl InnerTree {
    /// Get a value from the DB tree.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<IVec> {
        self.tree.get(key).unwrap()
    }

    /// Set a value in the DB tree.
    pub fn set<K, V>(&self, key: K, value: V) -> Option<IVec>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        self.tree.insert(key, value).unwrap()
    }

    /// Remove a value from the DB tree.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Option<IVec> {
        self.tree.remove(key).unwrap()
    }

    /// Compare and swap values in the DB tree.
    pub fn compare_and_swap<K, OV, NV>(&self, key: K, old: Option<OV>, new: Option<NV>) -> bool
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        NV: Into<IVec>,
    {
        Ok(Ok(())) == self.tree.compare_and_swap(key, old, new)
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    pub fn clear(&self) {
        self.tree.clear().unwrap()
    }

    pub fn iter(&self) -> Iter {
        self.tree.iter()
    }
}
