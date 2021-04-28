use rkyv::de::deserializers::AllocDeserializer;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    convert::TryInto,
    fs,
    ops::Neg,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

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
        tokio::spawn(run_background(
            db,
            size_limit,
            soft_cache_root,
            recv,
        ));
        Lru { cmd_sender }
    }

    pub async fn touch_file(&self, data_id: &DataIdentifier, offset: u64) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::UpdateFile {
                data_id: data_id.clone(),
                offset,
                size: None,
            })
            .await
            .unwrap();
    }

    pub async fn update_file_size(
        &self,
        data_id: &DataIdentifier,
        offset: u64,
        size: u64,
    ) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::UpdateFile {
                data_id: data_id.clone(),
                offset,
                size: Some(size),
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
        size: Option<u64>,
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
    while let Some(msg) = recv.recv().await {
        let last_space_usage = space_usage;
        match msg {
            LruInnerMsg::UpdateFile {
                data_id,
                offset,
                size,
            } => {
                handle_update_file(
                    &ts_tree,
                    &data_tree,
                    &data_id,
                    offset,
                    size,
                    &mut space_usage,
                );
            }
        }
        handle_cache_cleanup(
            &ts_tree,
            &data_tree,
            &soft_cache_root,
            size_limit,
            &mut space_usage,
        );
        // println!("Space usage is {}", space_usage);
        if last_space_usage != space_usage {
            lru_tree.insert("space_usage", &space_usage.to_be_bytes());
        }
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
    soft_cache_root: &Path,
    size_limit: u64,
    space_usage: &mut u64,
) {
    // println!("space_usage: {}, size_limit: {}", *space_usage, size_limit);
    // TODO: handle space_usage elsewhere too
    while *space_usage > size_limit {
        remove_one_file(ts_tree, data_tree, soft_cache_root, space_usage);
        if ts_tree.len() == 0 {
            // If we're over the size limit, but no items exist,
            // the LRU has become inconsistent, so re-scan.
            scan_soft_cache(soft_cache_root, space_usage);
        }
    }
}

/// Remove one file from the cache, returning the space saved.
fn remove_one_file(
    ts_tree: &Tree,
    data_tree: &Tree,
    cache_root: &Path,
    space_usage: &mut u64,
) {
    if let Some((_, data)) = ts_tree.pop_min() {
        // Get data_key
        let ts_data = get_rkyv::<LruTimestampData>(&data);
        let data_id = ts_data.data_id.deserialize(&mut AllocDeserializer).unwrap();

        // Delete file
        let file_path = get_file_cache_chunk_path(cache_root, &data_id, ts_data.offset);
        if let Err(_) = fs::remove_file(&file_path) {
            // If we can't delete the file, just ignore the error.
            // We've likely already recovered the space from it.
        }

        let data_key = LruDataKey {
            data_id,
            offset: ts_data.offset,
        }
        .to_bytes();

        // Get the size out of the lru_data entry.
        let size = if let Some(data_bytes) = data_tree.get(&data_key) {
            let data_data = get_rkyv::<LruDataData>(&data_bytes);
            data_data.size
        } else {
            // We failed again. Again, if this happens, just ignore it.
            0
        };

        // Delete the lru_data entry.
        data_tree.remove(&data_key);
        *space_usage = space_usage.saturating_sub(size);
    }
}

/// Handle updating a file's timestamp or changing its reported size.
fn handle_update_file(
    ts_tree: &Tree,
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    size: Option<u64>,
    space_usage: &mut u64,
) {
    // Create a new timestamp key
    let ts_key = add_new_ts_key(ts_tree, &data_id, offset);

    // Update data key, returning the old ts_key if existed
    let old_ts_key = update_data_key(
        data_tree,
        data_id,
        offset,
        ts_key,
        size,
        space_usage,
    );

    // Delete old_ts_key if existed
    if let Some(old_ts_key) = old_ts_key {
        ts_tree.remove(old_ts_key);
    }
}

/// Update an lru_data key with a new size and ts_key.
fn update_data_key(
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    ts_key: [u8; 9],
    size: Option<u64>,
    space_usage: &mut u64,
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
            // Set timestamp_key and optionally, size
            old_ts_key = Some(new_data.timestamp_key);
            new_data.timestamp_key = ts_key;
            if let Some(size) = size {
                let diff = size as i64 - new_data.size as i64;
                if diff.is_negative() {
                    *space_usage = space_usage.saturating_sub(diff.neg() as u64);
                } else {
                    *space_usage = space_usage.saturating_add(diff as u64);
                }
                new_data.size = size;
            }
        }
        (old_ts_key, new_data)
    } else {
        let new_data = LruDataData {
            timestamp_key: ts_key,
            size: size.unwrap_or(0),
        }
        .to_bytes();
        (None, new_data)
    };
    data_tree.insert(data_key, new_data);
    old_ts_key
}

/// Add a new key to the lru_timestamp tree, adding extra data to the key if needed.
fn add_new_ts_key(
    ts_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
) -> [u8; 9] {
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

/// Scan all files in the soft cache, to rebuild the LRU from scratch.
fn scan_soft_cache(soft_cache_root: &Path, space_usage: &mut u64) {
    todo!()
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
    size: u64,
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
