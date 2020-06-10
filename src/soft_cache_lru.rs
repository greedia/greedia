use std::{
    collections::{BTreeSet, HashMap},
    fs::remove_file,
    path::{Path, PathBuf},
    time::SystemTime,
};
use walkdir::WalkDir;

/// Look for all files contained within the soft cache
fn scan_soft_cache(soft_cache_path: &Path) -> impl Iterator<Item = SoftCacheItemEntry> {
    // soft_cache /m/d/5/md5md5/chunk_*
    let root = WalkDir::new(&soft_cache_path).max_open(100).min_depth(5);
    root.into_iter()
        .filter_entry(|e| e.file_type().is_file())
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let metadata = e.metadata().ok()?;
            let len = metadata.len();
            let last_access_time = metadata
                .accessed()
                .ok()
                .unwrap_or_else(|| SystemTime::now());
            let md5 = e.path().parent()?.file_name()?.to_str()?.to_string();
            let file_name = e.file_name().to_str()?;

            if file_name.starts_with("chunk_") {
                let (_, offset) = file_name.split_at(6);
                let offset: u64 = offset.parse().ok()?;
                let item = SoftCacheItem { md5, offset };

                Some(SoftCacheItemEntry {
                    last_access_time,
                    len,
                    item,
                })
            } else {
                None
            }
        })
}

/// Structure that tracks files within the soft cache, and deletes files when current_size rises above max_size.
pub struct SoftCacheLru {
    soft_cache_path: PathBuf,
    entries: BTreeSet<SoftCacheItemEntry>,
    lookup: HashMap<SoftCacheItem, (SystemTime, u64)>,
    current_size: u64,
    max_size: u64,
}

impl SoftCacheLru {
    /// Create new LRU file remover for soft cache files.
    pub fn new(soft_cache_path: &Path, max_size: u64) -> SoftCacheLru {
        let mut lookup = HashMap::new();
        let mut entries = BTreeSet::new();
        let mut current_size = 0;

        for entry in scan_soft_cache(soft_cache_path) {
            current_size += entry.len;
            entries.insert(entry.clone());
            lookup.insert(entry.item, (entry.last_access_time, entry.len));
        }

        let mut soft_cache_lru = SoftCacheLru {
            soft_cache_path: soft_cache_path.to_path_buf(),
            entries,
            lookup,
            current_size,
            max_size,
        };

        soft_cache_lru.purge_to_max();
        soft_cache_lru
    }

    /// Add a new file to the soft cache, removing old files if adding this file pushes us above max_size.
    /// Does nothing if file already exists.
    pub fn add(&mut self, md5: &str, offset: u64, len: u64) {
        let item = SoftCacheItem {
            md5: md5.to_string(),
            offset,
        };
        if !self.lookup.contains_key(&item) {
            let last_access_time = SystemTime::now();
            self.current_size = self.current_size + len;
            self.entries.insert(SoftCacheItemEntry {
                last_access_time,
                len,
                item,
            });
        }

        self.purge_to_max();
    }

    /// Touch a file to update its last access time to now.
    pub fn touch(&mut self, md5: &str, offset: u64) {
        let item = SoftCacheItem {
            md5: md5.to_string(),
            offset,
        };
        if let Some((last_access_time, len)) = self.lookup.get_mut(&item) {
            let entry = SoftCacheItemEntry {
                last_access_time: *last_access_time,
                len: *len,
                item,
            };
            let new_access_time = SystemTime::now();

            // Remove btree entry
            self.entries.remove(&entry);

            // Update access time in lookup map
            *last_access_time = new_access_time;

            // Update btree entry
            self.entries.insert(SoftCacheItemEntry {
                last_access_time: new_access_time,
                len: entry.len,
                item: entry.item,
            });
        };
    }

    /// Remove enough soft cache files such that current_size < max_size.
    pub fn purge_to_max(&mut self) {
        // Don't do anything if we're already below the max.
        if self.current_size <= self.max_size {
            return;
        }

        let mut left = self.current_size - self.max_size;
        let mut new_first_entry = None;

        // Iterate through the index, in order of access time ascending,
        // to find the number of files to remove until we're under the limit.
        let entry_iter = self.entries.iter();
        for entry in entry_iter {
            if left == 0 {
                // Needed because we want to remove one EXTRA item to push us below the max_size,
                // and split_off includes this next entry in the new entry list.
                // Without this, we'll end up slightly ABOVE max_size.
                new_first_entry = Some(entry.clone());
                break;
            }
            left = if let Some(left) = left.checked_sub(entry.len) {
                left
            } else {
                0
            };
        }

        if let Some(entry) = new_first_entry {
            // Remove entries we want to keep with split_off. This will leave self.entries with
            // only the entries we want to delete from disk. Not very ergonomic, but probably
            // the most efficient way until pop_first is stable.
            let new_entries = self.entries.split_off(&entry);
            let bytes_deleted = self.purge_all_items();
            self.entries = new_entries;
            self.current_size = self.current_size - bytes_deleted;
        } else {
            // If for whatever reason new_first_entry is None, do nothing. The logical thing to
            // do here would be to remove all items (if max_size is 0), but that might break streaming.
        }
    }

    /// Remove all items currently in self.entries, returning the count of bytes that have been deleted.
    fn purge_all_items(&mut self) -> u64 {
        let mut bytes_deleted = 0;

        // Create empty BTreeSet (shouldn't allocate any memory) and swap pointers with self.entries.
        let mut entries = BTreeSet::new();
        std::mem::swap(&mut entries, &mut self.entries);

        // Consume the entries, and delete each from disk.
        for entry in entries.into_iter() {
            bytes_deleted = bytes_deleted + entry.len;
            self.delete_from_filesystem(entry.item);
        }

        bytes_deleted
    }

    /// Actually delete soft cache file from filesystem.
    fn delete_from_filesystem(&self, item: SoftCacheItem) {
        let dir_1 = item.md5.get(0..2).unwrap();
        let dir_2 = item.md5.get(2..4).unwrap();

        let path_to_del = self
            .soft_cache_path
            .join(dir_1)
            .join(dir_2)
            .join(item.md5)
            .join(format!("chunk_{}", item.offset));

        if path_to_del.exists() {
            remove_file(path_to_del).unwrap();
        }
    }
}

/// Soft cache file details, used for looking up items in LRU.
#[derive(Debug, Eq, Ord, PartialOrd, PartialEq, Clone, Hash)]
pub struct SoftCacheItem {
    md5: String,
    offset: u64,
}

/// Entry to be used in BTreeSet.
/// BTreeSet will be ordered based on the values in this struct, in order.
#[derive(Eq, Ord, PartialOrd, PartialEq, Clone)]
pub struct SoftCacheItemEntry {
    last_access_time: SystemTime,
    len: u64,
    item: SoftCacheItem,
}
