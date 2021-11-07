use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use walkdir::{DirEntry, WalkDir};

use crate::{cache_handlers::filesystem::get_file_cache_chunk_path, db::{Db, LruAccess, types::{DataIdentifier, LruDataKey, LruTimestampData}}};

/// Public interface to Lru thread.
/// Lru is backgrounded and asynchronous - rather than
/// immediately deleting files, it only does it eventually.
#[derive(Clone)]
pub struct Lru {
    cmd_sender: Sender<LruInnerMsg>,
}

impl Lru {
    pub async fn new(db: &Db, db_path: &Path, size_limit: u64) -> Lru {
        let soft_cache_root = db_path.join("soft_cache");
        let db = db.clone();
        let (cmd_sender, recv) = channel(100);
        tokio::spawn(run_background(db, size_limit, soft_cache_root, recv));
        Lru { cmd_sender }
    }

    pub async fn touch_file(&self, data_id: &DataIdentifier, offset: u64) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::UpdateFile {
                data_id: data_id.clone(),
                offset,
            })
            .await
            .unwrap();
    }

    pub async fn add_space_usage(&self, size: u64) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::AddSpaceUsage { size })
            .await
            .unwrap();
    }

    pub async fn open_file(&self, data_id: &DataIdentifier) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::OpenFile {
                data_id: data_id.clone(),
            })
            .await
            .unwrap();
    }

    pub async fn close_file(self, data_id: DataIdentifier) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::CloseFile { data_id })
            .await
            .unwrap();
    }
}

#[derive(Debug)]
enum LruInnerMsg {
    UpdateFile {
        data_id: DataIdentifier,
        offset: u64,
    },
    AddSpaceUsage {
        size: u64,
    },
    OpenFile {
        data_id: DataIdentifier,
    },
    CloseFile {
        data_id: DataIdentifier,
    },
}

/// Run the main LRU background thread.
async fn run_background(
    db: Db,
    size_limit: u64,
    soft_cache_root: PathBuf,
    mut recv: Receiver<LruInnerMsg>,
) {
    let lru = db.lru_access(soft_cache_root, size_limit);

    let mut space_usage = lru.get_space_usage();

    scan_soft_cache(&lru, &mut space_usage);

    let mut open_files = HashSet::new();

    while let Some(msg) = recv.recv().await {
        match msg {
            LruInnerMsg::UpdateFile { data_id, offset } => {
                handle_update_file(&lru, &data_id, offset);
            }
            LruInnerMsg::AddSpaceUsage { size } => {
                space_usage += size;
            }
            LruInnerMsg::OpenFile { data_id } => {
                open_files.insert(data_id);
            }
            LruInnerMsg::CloseFile { data_id } => {
                open_files.remove(&data_id);
            }
        }
        handle_cache_cleanup(&lru, &open_files, &mut space_usage);
    }
}

/// Handle cleaning up cache files if we're over the cache size.
fn handle_cache_cleanup(
    lru: &LruAccess,
    open_files: &HashSet<DataIdentifier>,
    space_usage: &mut u64,
) {
    if lru.is_empty() && *space_usage != 0 {
        // If we're over the size limit, but no items exist,
        // the LRU has become inconsistent, so re-scan.
        println!(
            "LRU inconsistent; tree empty, but space_usage is {}",
            *space_usage
        );
        scan_soft_cache(lru, space_usage);
    }

    if *space_usage < lru.size_limit {
        return;
    }

    let mut ts_iter = lru.iter_ts();
    while let Some((key, val)) = ts_iter.next() {
        // Get the data_id
        let ts_data = lru.get_ts_data_from_data(val).unwrap();
        let ts_data = ts_data.borrow_dependent().archived;
        let data_id: DataIdentifier = (&ts_data.data_id).into();

        // Skip any chunks in currently open files
        if open_files.contains(&data_id) {
            continue;
        }

        // Delete the file, ignoring any errors we may get.
        // Get the file size from stat.
        let file_path = get_file_cache_chunk_path(&lru.cache_root, &data_id, ts_data.offset.value());
        let file_len = file_path.metadata().ok().map(|x| x.len()).unwrap_or(0);
        if fs::remove_file(&file_path).is_err() {
            // If we can't delete the file, just ignore the error.
            // We've likely already recovered the space from it.
        }

        // Get the data_key to find the file size
        let data_key = LruDataKey {
            data_id,
            offset: ts_data.offset.value(),
        }
        .to_bytes();

        // Subtract from our space usage
        *space_usage = space_usage.saturating_sub(file_len);

        // Delete the data and timestamp entries.
        lru.rm_timestamp_key(&key);
        lru.rm_data_key(&data_key);

        if *space_usage < lru.size_limit {
            return;
        }
    }

    // If we reach here, we've cleaned up everything we can but still aren't under the size limit.
    // Unfortunately there isn't much we can do here.
}

/// Handle updating a file's timestamp or changing its reported size.
fn handle_update_file(lru: &LruAccess, data_id: &DataIdentifier, offset: u64) {
    // Create a new timestamp key
    let ts_key = add_new_ts_key(lru, data_id, offset);

    // Update data key, returning the old ts_key if existed
    let data_key = LruDataKey {
        data_id: data_id.clone(),
        offset,
    };
    let old_ts_key = lru.update_data_key(data_key, ts_key);

    // Delete old_ts_key if existed
    if let Some(old_ts_key) = old_ts_key {
        lru.rm_timestamp_key(&old_ts_key);
    }
}

/// Add a new key to the lru_timestamp tree, adding extra data to the key if needed.
///
/// If multiple keys are added to the tree in the same millisecond, the extra data
/// will be used to deduplicate the keys.
fn add_new_ts_key(lru: &LruAccess, data_id: &DataIdentifier, offset: u64) -> [u8; 9] {
    let data = LruTimestampData {
        data_id: data_id.clone(),
        offset,
    };

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let timestamp = since_the_epoch.as_millis() as u64;

    // Try adding with extra_val of 0 to 256
    if let Some(ts_key) = lru.add_new_timestamp_key(timestamp, &data) {
        return ts_key;
    }

    panic!("Failed to add LRU timestamp extra_val 256 times")
}

/// Check if a file is a chunk file or is dir.
fn is_chunk_file(entry: &DirEntry) -> bool {
    entry.file_type().is_dir()
        || entry
            .file_name()
            .to_str()
            .map(|s| s.starts_with("chunk_"))
            .unwrap_or(false)
}

/// From a path, get the DataIdentifier, offset, size, and timestamp of a chunk.
fn dir_entry_to_data_key(entry: &DirEntry) -> Option<(DataIdentifier, u64, u64, u64)> {
    // We're assuming only GlobalMd5 exists for now.
    // This will break when DriveUnique is added to DataIdentifier.
    // TODO: handle more than GlobalMd5.

    let path = entry.path();

    let file_name = path.file_name()?.to_str()?;

    // Assume the file name starts with chunk_
    let (_, offset) = file_name.split_at(6);
    let offset = offset.parse().ok()?;
    let meta = entry.metadata().ok()?;
    let len = meta.len();
    let ts = meta.created().ok()?;
    let ts = ts.duration_since(UNIX_EPOCH).ok()?.as_millis() as u64;

    // Get the DataIdentifier (TODO: handle more than GlobalMd5)
    let md5_hex = path.parent()?.file_name()?.to_str()?;
    let md5_bytes = hex::decode(md5_hex).ok()?;
    let data_id = DataIdentifier::GlobalMd5(md5_bytes);

    Some((data_id, offset, len, ts))
}

/// Scan all files in the soft cache, to rebuild the LRU from scratch.
fn scan_soft_cache(lru: &LruAccess, space_usage: &mut u64) {
    // Clear ts_tree and data_tree, and start from scratch
    lru.clear();
    *space_usage = 0;

    if !lru.cache_root.exists() {
        return;
    }

    let walker = WalkDir::new(lru.cache_root.clone())
        .into_iter()
        .filter_entry(is_chunk_file)
        .filter_map(|e| e.ok());
    for entry in walker {
        if entry.file_type().is_dir() {
            continue;
        }

        if let Some((data_id, offset, size, timestamp)) = dir_entry_to_data_key(&entry) {
            let data = LruTimestampData {
                data_id: data_id.clone(),
                offset,
            };

            if lru.add_new_timestamp(timestamp, data).is_some() {
                *space_usage += size;
            } else {
                continue;
            }
        }
    }
}

// /// Key used for the LruData database tree.
// #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
// struct LruDataKey {
//     #[archive(derive(Clone))]
//     data_id: DataIdentifier,
//     offset: u64,
// }
//
// impl LruDataKey {
//     fn to_bytes(&self) -> Vec<u8> {
//         serialize_rkyv(self)
//     }
// }
//
// /// Data used for the LruData database tree.
// #[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
// struct LruDataData {
//     timestamp_key: [u8; 9],
// }
//
// impl LruDataData {
//     fn to_bytes(&self) -> Vec<u8> {
//         serialize_rkyv(self)
//     }
// }
//
// /// Key used for the LruTimestamp database tree.
// #[derive(Debug)]
// struct LruTimestampKey {
//     /// Unix timestamp in milliseconds.
//     timestamp: u64,
//     /// Extra value, for deduplication.
//     extra_val: u8,
// }
//
// impl LruTimestampKey {
//     fn to_bytes(&self) -> [u8; 9] {
//         let mut out = [0u8; 9];
//         out[..8].copy_from_slice(&self.timestamp.to_be_bytes());
//         out[8..].copy_from_slice(&self.extra_val.to_be_bytes());
//         out
//     }
// }
//
// #[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
// struct LruTimestampData {
//     data_id: DataIdentifier,
//     offset: u64,
// }
//
// impl LruTimestampData {
//     fn to_bytes(&self) -> Vec<u8> {
//         serialize_rkyv(self)
//     }
// }
