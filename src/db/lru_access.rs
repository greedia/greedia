use std::{convert::TryInto, path::PathBuf, pin::Pin};

use rkyv::{archived_root, archived_root_mut};
use self_cell::self_cell;
use sled::{IVec, Iter};

use super::{
    storage::{InnerDb, InnerTree},
    types::{
        ArchivedLruDataData, ArchivedLruTimestampData, LruDataData, LruDataKey, LruTimestampData,
        LruTimestampKey,
    },
};

/// Used to keep track of the soft cache LRU within the database.
pub struct LruAccess {
    /// Variables used for the soft cache LRU
    tree: InnerTree,
    /// Mapping from LruTimestampKey to LruTimestampData
    /// For keeping track of oldest files in the LRU
    timestamp_tree: InnerTree,
    /// Mapping from LruDataKey to LruDataData
    /// For keeping track of the data in the LRU, to update timestamps
    data_tree: InnerTree,
    /// Root path of the soft cache.
    pub cache_root: PathBuf,
    /// Soft cache size limit in bytes.
    pub size_limit: u64,
}

impl LruAccess {
    pub fn new(db: InnerDb, cache_root: PathBuf, size_limit: u64) -> Self {
        let lru_tree = db.tree(b"lru");
        let lru_timestamp_tree = db.tree(b"lru_timestamp");
        let lru_data_tree = db.tree(b"lru_data");

        Self {
            tree: lru_tree,
            timestamp_tree: lru_timestamp_tree,
            data_tree: lru_data_tree,
            cache_root,
            size_limit,
        }
    }

    pub fn get_space_usage(&self) -> u64 {
        if let Some(space_usage_val) = self.tree.get("space_usage") {
            u64::from_be_bytes(space_usage_val.as_ref().try_into().unwrap())
        } else {
            self.tree.set("space_usage", &0u64.to_be_bytes());
            0
        }
    }

    pub fn is_empty(&self) -> bool {
        self.timestamp_tree.is_empty()
    }

    pub fn clear(&self) {
        self.timestamp_tree.clear();
        self.data_tree.clear();
    }

    pub fn add_new_timestamp_key(
        &self,
        timestamp: u64,
        data: &LruTimestampData,
    ) -> Option<[u8; 9]> {
        let mut res_ts_key = None;
        for extra_val in 0..u8::MAX {
            let ts_key = LruTimestampKey {
                timestamp,
                extra_val,
            }
            .to_bytes();

            let ts_data = data.to_bytes();

            if self.timestamp_tree.compare_and_swap(
                &ts_key,
                None as Option<&[u8]>,
                Some(ts_data.as_slice()),
            ) {
                res_ts_key = Some(ts_key);
                break;
            }
        }
        // Return None if we get >255 millisecond-matching keys.
        res_ts_key
    }

    pub fn set_data(&self, key: LruDataKey, data: &[u8]) -> Option<()> {
        self.data_tree.set(key.to_bytes(), data)?;
        Some(())
    }

    pub fn get_ts_data_from_data(&self, data: IVec) -> Option<BorrowedLruTimestampData> {
        let lru_timestamp_data = BorrowedLruTimestampData::new(data, |data| {
            let archived = unsafe { archived_root::<LruTimestampData>(data) };
            LLruTimestampData { archived }
        });
        Some(lru_timestamp_data)
    }

    pub fn update_data_key(&self, data_key: LruDataKey, ts_key: [u8; 9]) -> Option<[u8; 9]> {
        let (old_ts_key, new_data) =
            if let Some(old_data) = self.data_tree.get(&data_key.to_bytes()) {
                let mut new_data = old_data.to_vec();
                let old_ts_key;
                {
                    let new_data = Pin::new(&mut new_data[..]);
                    let mut new_data = unsafe { archived_root_mut::<LruDataData>(new_data) };
                    old_ts_key = Some(new_data.timestamp_key);
                    new_data.timestamp_key = ts_key;
                }
                (old_ts_key, new_data)
            } else {
                let new_data = LruDataData {
                    timestamp_key: ts_key,
                }
                .to_bytes();
                (None, new_data)
            };
        self.set_data(data_key, &new_data);
        old_ts_key
    }

    pub fn add_new_timestamp(&self, timestamp: u64, data: LruTimestampData) -> Option<()> {
        if let Some(ts_key) = self.add_new_timestamp_key(timestamp, &data) {
            // Add data key and data
            let data_key = LruDataKey {
                data_id: data.data_id.clone(),
                offset: data.offset,
            }
            .to_bytes();

            let data_data = LruDataData {
                timestamp_key: ts_key,
            }
            .to_bytes();

            self.data_tree.set(data_key, data_data);

            Some(())
        } else {
            None
        }
    }

    pub fn rm_timestamp_key(&self, timestamp_key: &[u8]) -> Option<()> {
        self.timestamp_tree.remove(timestamp_key)?;
        Some(())
    }

    pub fn rm_data_key(&self, data_key: &[u8]) -> Option<()> {
        self.data_tree.remove(data_key)?;
        Some(())
    }

    pub fn iter_ts(&self) -> TsIterator {
        let inner_iter = self.timestamp_tree.iter();
        TsIterator { inner_iter }
    }
}

pub struct TsIterator {
    inner_iter: Iter,
}

impl TsIterator {
    pub fn next(&mut self) -> Option<(IVec, IVec)> {
        self.inner_iter.next().and_then(|i| i.ok())
    }
}

self_cell!(
    pub struct BorrowedLruDataData {
        owner: IVec,

        #[covariant]
        dependent: LLruDataData,
    }
);

pub struct LLruDataData<'a> {
    pub archived: &'a ArchivedLruDataData,
}

self_cell!(
    pub struct BorrowedLruTimestampData {
        owner: IVec,

        #[covariant]
        dependent: LLruTimestampData,
    }
);

pub struct LLruTimestampData<'a> {
    pub archived: &'a ArchivedLruTimestampData,
}
