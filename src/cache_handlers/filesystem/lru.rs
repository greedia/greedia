use rkyv::de::deserializers::AllocDeserializer;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::HashSet,
    convert::TryInto,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use walkdir::{DirEntry, WalkDir};

use crate::cache_handlers::filesystem::get_file_cache_chunk_path;
use crate::db::{get_rkyv, get_rkyv_mut, serialize_rkyv, Db, Tree};
use crate::types::DataIdentifier;

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
            .send(LruInnerMsg::AddSpaceUsage {
                size,
            })
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
            .send(LruInnerMsg::CloseFile {
                data_id,
            })
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
    // Tree for top-level LRU data
    let lru_tree = db.tree(b"lru");

    // Tree where data is stored in timestamp order.
    let ts_tree = db.tree(b"lru_timestamp");

    // Tree where data is indexed by data_id, hard_cache, and offset.
    let data_tree = db.tree(b"lru_data");

    let mut space_usage = get_space_usage(&lru_tree);

    scan_soft_cache(&ts_tree, &data_tree, &soft_cache_root, &mut space_usage);

    let mut open_files = HashSet::new();

    while let Some(msg) = recv.recv().await {
        // let last_space_usage = space_usage;
        match msg {
            LruInnerMsg::UpdateFile {
                data_id,
                offset,
            } => {
                handle_update_file(
                    &ts_tree,
                    &data_tree,
                    &data_id,
                    offset,
                );
            }
            LruInnerMsg::AddSpaceUsage { size } => {
                space_usage += size;
                // println!("space_usage {}, size_limit {}", space_usage, size_limit);


            }
            LruInnerMsg::OpenFile { data_id } => {
                let hex_md5 = if let DataIdentifier::GlobalMd5(x) = &data_id {
                    hex::encode(x)
                } else {
                    "".to_string()
                };
        
                println!("OPEN  DATA_ID {} (count: {})", hex_md5, open_files.len());
                open_files.insert(data_id);
            }
            LruInnerMsg::CloseFile { data_id } => {
                let hex_md5 = if let DataIdentifier::GlobalMd5(x) = &data_id {
                    hex::encode(x)
                } else {
                    "".to_string()
                };
        
                println!("CLOSE DATA_ID {} (count: {})", hex_md5, open_files.len());

                open_files.remove(&data_id);
            }
        }
        handle_cache_cleanup(
            &ts_tree,
            &data_tree,
            &soft_cache_root,
            &open_files,
            size_limit,
            &mut space_usage,
        );
        // println!("Space usage is {}", space_usage);
        // if last_space_usage != space_usage {
        //     lru_tree.insert("space_usage", &space_usage.to_be_bytes());
        // }
    }
}

fn get_space_usage(lru_tree: &Tree) -> u64 {
    if let Some(space_usage_val) = lru_tree.get("space_usage") {
        u64::from_be_bytes(space_usage_val.as_ref().try_into().unwrap())
    } else {
        lru_tree.insert("space_usage", &0u64.to_be_bytes());
        0
    }
}

/// Handle cleaning up cache files if we're over the cache size.
fn handle_cache_cleanup(
    ts_tree: &Tree,
    data_tree: &Tree,
    cache_root: &Path,
    open_files: &HashSet<DataIdentifier>,
    size_limit: u64,
    space_usage: &mut u64,
) {
    if ts_tree.is_empty() && *space_usage != 0 {
        // If we're over the size limit, but no items exist,
        // the LRU has become inconsistent, so re-scan.
        println!("LRU inconsistent; tree empty, but space_usage is {}", *space_usage);
        scan_soft_cache(ts_tree, data_tree, cache_root, space_usage);
    }

    for x in ts_tree.iter() {
        let (key, val) = x.unwrap();

        // Get the data_id
        let ts_data = get_rkyv::<LruTimestampData>(&val);
        let data_id = ts_data.data_id.deserialize(&mut AllocDeserializer).unwrap();

        // Skip any chunks in currently open files
        if open_files.contains(&data_id) {
            continue;
        }

        // Delete the file, ignoring any errors we may get.
        // Get the file size from stat.
        let file_path = get_file_cache_chunk_path(cache_root, &data_id, ts_data.offset);
        let file_len = file_path.metadata().ok().map(|x| x.len()).unwrap_or(0);
        if let Err(_) = fs::remove_file(&file_path) {
            // If we can't delete the file, just ignore the error.
            // We've likely already recovered the space from it.
        }

        // Get the data_key to find the file size
        let data_key = LruDataKey {
            data_id,
            offset: ts_data.offset,
        }
        .to_bytes();

        // subtract from our space usage
        *space_usage = space_usage.saturating_sub(file_len);

        // Delete the lru_data and ts_data entries.
        data_tree.remove(&data_key);
        ts_tree.remove(&key);

        if *space_usage < size_limit {
            return;
        }
    }

    // If we reach here, we've cleaned up everything we can but still aren't under the size limit.
    // Unfortunately there isn't much we can do here.
}

/// Handle updating a file's timestamp or changing its reported size.
fn handle_update_file(
    ts_tree: &Tree,
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64
) {
    // Create a new timestamp key
    let ts_key = add_new_ts_key(ts_tree, &data_id, offset);

    // Update data key, returning the old ts_key if existed
    let old_ts_key = update_data_key(data_tree, data_id, offset, ts_key);

    // Delete old_ts_key if existed
    if let Some(old_ts_key) = old_ts_key {
        ts_tree.remove(old_ts_key);
    }
}

/// Update an lru_data key with a new ts_key.
fn update_data_key(
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    ts_key: [u8; 9]
) -> Option<[u8; 9]> {
    let data_key = LruDataKey {
        data_id: data_id.clone(),
        offset,
    }
    .to_bytes();

    let (old_ts_key, new_data) = if let Some(old_data) = data_tree.get(&data_key) {
        let mut new_data = old_data.to_vec();
        let old_ts_key;
        {
            let mut new_data = get_rkyv_mut::<LruDataData>(&mut new_data);
            // Set timestamp_key
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
    data_tree.insert(data_key, new_data);
    old_ts_key
}

/// Add a new key to the lru_timestamp tree, adding extra data to the key if needed.
fn add_new_ts_key(ts_tree: &Tree, data_id: &DataIdentifier, offset: u64) -> [u8; 9] {
    let data = LruTimestampData {
        data_id: data_id.clone(),
        offset,
    }
    .to_bytes();

    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let timestamp = since_the_epoch.as_millis() as u64;

    // Try adding with extra_val of 0 to 256
    for extra_val in 0..=u8::MAX {
        let ts_key = LruTimestampKey {
            timestamp,
            extra_val,
        }
        .to_bytes();

        if ts_tree.compare_and_swap(&ts_key, None as Option<&[u8]>, Some(data.as_slice())) {
            return ts_key;
        }
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
    // This will break when DriveSpecific is added to DataIdentifier.
    // TODO: make more generic.

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
fn scan_soft_cache(
    ts_tree: &Tree,
    data_tree: &Tree,
    soft_cache_root: &Path,
    space_usage: &mut u64,
) {
    // Clear ts_tree and data_tree, and start from scratch
    ts_tree.clear();
    data_tree.clear();
    *space_usage = 0;

    if !soft_cache_root.exists() {
        return;
    }

    let walker = WalkDir::new(soft_cache_root).into_iter().filter_entry(is_chunk_file).filter_map(|e| e.ok());
    for entry in walker
    {
        if entry.file_type().is_dir() {
            continue;
        }

        if let Some((data_id, offset, size, ts)) = dir_entry_to_data_key(&entry) {
            let ts_data = LruTimestampData {
                data_id: data_id.clone(),
                offset,
            }
            .to_bytes();

            // Add a ts key and data
            let mut res_ts_key = None;
            for extra_val in 0..u8::MAX {
                let ts_key = LruTimestampKey {
                    timestamp: ts,
                    extra_val,
                }
                .to_bytes();

                if ts_tree.compare_and_swap(
                    &ts_key,
                    None as Option<&[u8]>,
                    Some(ts_data.as_slice()),
                ) {
                    res_ts_key = Some(ts_key);
                    break;
                }
            }

            if let Some(ts_key) = res_ts_key {
                // Add data key and data
                let data_key = LruDataKey {
                    data_id: data_id.clone(),
                    offset,
                }
                .to_bytes();

                let data_data = LruDataData {
                    timestamp_key: ts_key
                }
                .to_bytes();

                data_tree.insert(data_key, data_data);
                *space_usage += size;
            } else {
                // Got >255 millisecond-matching timestamps, so just skip this chunk.
                continue;
            }
        }
    }
}

/// Key used for the LruData database tree.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
struct LruDataKey {
    #[archive(derive(Clone))]
    data_id: DataIdentifier,
    offset: u64,
}

impl LruDataKey {
    fn to_bytes(&self) -> Vec<u8> {
        serialize_rkyv(self)
    }
}

/// Data used for the LruData database tree.
#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
struct LruDataData {
    timestamp_key: [u8; 9],
}

impl LruDataData {
    fn to_bytes(&self) -> Vec<u8> {
        serialize_rkyv(self)
    }
}

/// Key used for the LruTimestamp database tree.
/// Add the data_id and offset
#[derive(Debug)]
struct LruTimestampKey {
    /// Unix timestamp in milliseconds.
    timestamp: u64,
    /// Extra value, for deduplication.
    extra_val: u8,
}

impl LruTimestampKey {
    fn to_bytes(&self) -> [u8; 9] {
        let mut out = [0u8; 9];
        out[..8].copy_from_slice(&self.timestamp.to_be_bytes());
        out[8..].copy_from_slice(&self.extra_val.to_be_bytes());
        out
    }
}

#[derive(Debug, PartialEq, Archive, Serialize, Deserialize)]
struct LruTimestampData {
    data_id: DataIdentifier,
    offset: u64,
}

impl LruTimestampData {
    fn to_bytes(&self) -> Vec<u8> {
        serialize_rkyv(self)
    }
}
