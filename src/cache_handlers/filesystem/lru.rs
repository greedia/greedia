#![allow(dead_code, unused_variables)] // TODO: remove this

use rkyv::{
    archived_value, archived_value_mut,
    de::deserializers::AllocDeserializer,
    ser::{serializers::WriteSerializer, Serializer},
    Archive, Deserialize, Serialize,
};
use sled::{Db, Tree};
use std::{
    convert::TryInto,
    path::{Path, PathBuf},
    pin::Pin,
    time::SystemTime,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

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
        let hard_cache_root = db_path.join("hard_cache");
        let soft_cache_root = db_path.join("soft_cache");
        let db = db.clone();
        let inner = LruInner {
            hard_cache_root,
            soft_cache_root,
            size_limit,
            db,
        };
        let (cmd_sender, recv) = channel(10);
        tokio::spawn(run_background(inner, recv));
        Lru { cmd_sender }
    }

    pub async fn touch_file(&self, data_id: DataIdentifier, offset: u64, hard_cache: bool) {
        self.cmd_sender
            .clone()
            .send(LruInnerMsg::UpdateFile {
                data_id,
                offset,
                hard_cache,
                size: None,
            })
            .await
            .unwrap();
    }

    pub async fn update_file_size(
        &self,
        data_if: DataIdentifier,
        offset: u64,
        hard_cache: bool,
        size: u64,
    ) {
    }
}

#[derive(Debug)]
enum LruInnerMsg {
    UpdateFile {
        data_id: DataIdentifier,
        offset: u64,
        hard_cache: bool,
        size: Option<u64>,
    },
}

/// Run the main LRU background thread.
async fn run_background(inner: LruInner, mut recv: Receiver<LruInnerMsg>) {
    // Tree for top-level LRU data
    let lru_tree = inner.db.open_tree(b"lru").unwrap();

    // Tree where data is stored in timestamp order.
    let ts_tree = inner.db.open_tree(b"lru_timestamp").unwrap();

    // Tree where data is indexed by data_id, hard_cache, and offset.
    let data_tree = inner.db.open_tree(b"lru_data").unwrap();

    let mut space_usage = get_space_usage(&lru_tree);
    while let Some(msg) = recv.recv().await {
        match msg {
            LruInnerMsg::UpdateFile {
                data_id,
                offset,
                hard_cache,
                size,
            } => {
                handle_update_file(
                    &ts_tree,
                    &data_tree,
                    &data_id,
                    offset,
                    hard_cache,
                    size,
                    &mut space_usage,
                );
            }
        }
        handle_cache_cleanup(
            &lru_tree,
            &ts_tree,
            &data_tree,
            inner.size_limit,
            &mut space_usage,
        );
    }
}

fn get_space_usage(lru_tree: &Tree) -> u64 {
    if let Some(space_usage_val) = lru_tree.get("space_usage").unwrap() {
        u64::from_be_bytes(space_usage_val.as_ref().try_into().unwrap())
    } else {
        lru_tree.insert("space_usage", &0u64.to_be_bytes()).unwrap();
        0
    }
}

/// Handle cleaning up cache files if we're over the cache size.
fn handle_cache_cleanup(
    lru_tree: &Tree,
    ts_tree: &Tree,
    data_tree: &Tree,
    size_limit: u64,
    space_usage: &mut u64,
) {
    // TODO: handle space_usage elsewhere too
    while *space_usage > size_limit {
        let recovered_space = remove_one_file(ts_tree, data_tree);
        *space_usage = space_usage.saturating_sub(recovered_space);
    }
}

/// Remove one file from the cache, returning the space saved.
fn remove_one_file(ts_tree: &Tree, data_tree: &Tree) -> u64 {
    if let Some((key, data)) = ts_tree.pop_min().unwrap() {
        // Get data_key
        let a_data = unsafe { archived_value::<LruTimestampData>(data.as_ref(), 0) };
        let data_id = a_data.data_id.deserialize(&mut AllocDeserializer).unwrap();
        let data_key = LruDataKey {
            data_id,
            offset: a_data.offset,
            hard_cache: a_data.hard_cache,
        };
        // KTODO:
        // Delete file
        // Delete lru_data entry (but get size out of it first)
        // Delete lru_timestamp entry
        todo!()
    } else {
        0
    }
}

/// Handle updating a file's timestamp or changing its reported size.
fn handle_update_file(
    ts_tree: &Tree,
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    hard_cache: bool,
    size: Option<u64>,
    space_usage: &mut u64,
) {
    // Create a new timestamp key
    let ts_key = add_new_ts_key(ts_tree, &data_id, offset, hard_cache);

    // Update data key, returning the old ts_key if existed
    let old_ts_key = update_data_key(data_tree, data_id, offset, hard_cache, ts_key, size);

    // Delete old_ts_key if existed
    if let Some(old_ts_key) = old_ts_key {
        ts_tree.remove(old_ts_key).unwrap();
    }
}

/// Update an lru_data key with a new size and ts_key.
fn update_data_key(
    data_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    hard_cache: bool,
    ts_key: [u8; 9],
    size: Option<u64>,
) -> Option<[u8; 9]> {
    let data_key = LruDataKey {
        data_id: data_id.clone(),
        offset,
        hard_cache,
    }
    .to_bytes();

    let (old_ts_key, new_data) = if let Some(old_data) = data_tree.get(&data_key).unwrap() {
        let mut new_data = old_data.to_vec();
        let old_ts_key;
        {
            let new_data_pin = Pin::new(new_data.as_mut_slice());
            let mut a_data = unsafe { archived_value_mut::<LruDataData>(new_data_pin, 0) };
            // Set timestamp_key and optionally, size
            old_ts_key = Some(a_data.timestamp_key);
            a_data.timestamp_key = ts_key;
            if let Some(size) = size {
                a_data.size = size;
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
    data_tree.insert(data_key, new_data).unwrap();
    old_ts_key
}

/// Add a new key to the lru_timestamp tree, adding extra data to the key if needed.
fn add_new_ts_key(
    ts_tree: &Tree,
    data_id: &DataIdentifier,
    offset: u64,
    hard_cache: bool,
) -> [u8; 9] {
    let data = LruTimestampData {
        data_id: data_id.clone(),
        offset,
        hard_cache,
    }
    .to_bytes();

    let timestamp = SystemTime::now();
    let timestamp = 0;

    // Try adding with extra_val of 0 to 256
    for extra_val in 0..=u8::MAX {
        let ts_key = LruTimestampKey {
            timestamp,
            extra_val,
        }
        .to_bytes();

        let cas_res =
            ts_tree.compare_and_swap(&ts_key, None as Option<&[u8]>, Some(data.as_slice()));
        if let Ok(Ok(())) = cas_res {
            return ts_key;
        }
    }

    panic!("Failed to add LRU timestamp extra_val 256 times")
}

struct LruInner {
    hard_cache_root: PathBuf,
    soft_cache_root: PathBuf,
    size_limit: u64,
    db: Db,
}

/// Key used for the LruData database tree.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
struct LruDataKey {
    #[archive(derive(Clone))]
    data_id: DataIdentifier,
    offset: u64,
    hard_cache: bool,
}

impl LruDataKey {
    fn to_bytes(&self) -> Vec<u8> {
        let mut key_serializer = WriteSerializer::new(Vec::new());
        key_serializer.serialize_value(self).unwrap();
        key_serializer.into_inner()
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
        let mut serializer = WriteSerializer::new(Vec::new());
        serializer.serialize_value(self).unwrap();
        serializer.into_inner()
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
    hard_cache: bool,
}

impl LruTimestampData {
    fn to_bytes(&self) -> Vec<u8> {
        let mut serializer = WriteSerializer::new(Vec::new());
        serializer.serialize_value(self).unwrap();
        serializer.into_inner()
    }
}
