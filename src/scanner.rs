use crate::{
    downloaders::Change,
    drive_access::DriveAccess,
    hard_cache::HardCacheMetadata,
    types::{
        make_lookup_key, ArchivedDriveItem, ArchivedDriveItemData, DirItem, DriveItem,
        DriveItemData, ReverseAccess,
    },
};
use crate::{hard_cache::HardCacher, types::TreeKeys};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use chrono::{Duration, TimeZone, Utc};
use flume::Receiver;
use futures::{Stream, StreamExt, TryStreamExt};
use futures::future::join_all;
use itertools::Itertools;
use rkyv::{de::deserializers::AllocDeserializer, Archive, Deserialize, Serialize};
use sled::IVec;

use crate::{
    cache_handlers::{DownloaderError, Page, PageItem},
    db::{get_rkyv, serialize_rkyv, Db, Tree},
};

#[derive(Clone)]
struct ScanTrees {
    drive_id: String,
    /// Tree that contains general scan metadata for this drive.
    scan_tree: Tree,
    /// Tree that maps access_ids to inodes.
    access_tree: Tree,
    /// Tree that maps inodes to DriveItems.
    inode_tree: Tree,
    /// Tree that maps a dir inode + filename to an inode.
    lookup_tree: Tree,
    /// Tree that maps an access_id to a parent inode containing the item.
    raccess_tree: Tree,
    /// Tree that maps data_ids to HardCacheMetadatas.
    hc_meta_tree: Tree,
    /// Atomic value that contains the next inode.
    next_inode: Arc<AtomicU64>,
}

impl ScanTrees {
    fn new(db: Db, drive_type: &str, drive_id: &str) -> ScanTrees {
        let tree_keys = TreeKeys::new(drive_type, drive_id);

        let scan_tree = db.tree(&tree_keys.scan_key);
        let access_tree = db.tree(&tree_keys.access_key);
        let inode_tree = db.tree(&tree_keys.inode_key);
        let lookup_tree = db.tree(&tree_keys.lookup_key);
        let raccess_tree = db.tree(&tree_keys.raccess_key);
        let hc_meta_tree = db.tree(&tree_keys.hc_meta_key);

        let next_inode = scan_tree
            .get(b"next_inode")
            .map(|i| u64::from_le_bytes(i.as_ref().try_into().unwrap()))
            .unwrap_or(1);

        let next_inode = Arc::new(AtomicU64::new(next_inode));

        ScanTrees {
            drive_id: drive_id.to_string(),
            scan_tree,
            access_tree,
            inode_tree,
            lookup_tree,
            raccess_tree,
            hc_meta_tree,
            next_inode,
        }
    }

    fn next_inode(&self) -> u64 {
        let new_inode = self.next_inode.fetch_add(1, Ordering::Acquire);
        self.scan_tree
            .insert(b"next_inode", &(new_inode + 1).to_le_bytes());
        new_inode
    }
}

/// Main async task that runs the scanning for this drive.
pub async fn scan_thread(drive_access: Arc<DriveAccess>) {
    let name = drive_access.name.clone();
    let drive_type = drive_access.cache_handler.get_drive_type();
    let drive_id = drive_access.cache_handler.get_drive_id().clone();
    println!("Scanning {} ({}:{})...", name, drive_type, drive_id);

    let trees = ScanTrees::new(drive_access.db.clone(), drive_type, &drive_id);

    // Start watcher thread
    tokio::spawn(watch_thread(trees.clone(), drive_access.clone()));

    let last_page_token = trees
        .scan_tree
        .get(b"last_page_token")
        .map(|x| get_rkyv::<String>(&x).to_string());
    let last_modified_date = trees
        .scan_tree
        .get(b"last_modified_date")
        .map(|x| i64::from_le_bytes(x.as_ref().try_into().unwrap()))
        .map(|x| Utc.timestamp(x, 0));

    let scan_stream = drive_access
        .cache_handler
        .scan_pages(last_page_token, last_modified_date);

    perform_scan(&trees, scan_stream).await;

    println!("Finished scanning {} ({}:{}).", name, drive_type, drive_id);

    let (count, size) = get_drive_size(&trees.inode_tree).await;

    println!(
        "Caching {} items of size {} for drive {} ({}:{})...",
        count, size, name, drive_type, drive_id
    );

    perform_caching(&trees, drive_access).await;

    println!("Finished caching {} ({}:{}).", name, drive_type, drive_id);
}

async fn watch_thread(trees: ScanTrees, drive_access: Arc<DriveAccess>) {
    let mut change_stream = drive_access.cache_handler.watch_changes();
    let hard_cacher = HardCacher::new(drive_access.clone(), 1_000_000); // TODO: not hardcode min_size

    while let Some(changes) = change_stream.next().await {
        dbg!(&changes);
        let changes = changes.unwrap();
        let (additions, removals): (Vec<_>, _) = changes.into_iter().partition(|c| {
            if let Change::Added(_) = c {
                true
            } else {
                false
            }
        });

        // Handle the additions
        let page_items = &additions
            .into_iter()
            .filter_map(|c| {
                if let Change::Added(page_item) = c {
                    Some(page_item)
                } else {
                    None
                }
            })
            .collect();

        // Add to DB
        handle_one_page(&trees, page_items).await;

        // Hard cache files
        for page_item in page_items {
            if let Some(inode) = trees.access_tree.get(page_item.id.as_bytes()) {
                if let Some(drive_item_bytes) = trees.inode_tree.get(inode) {
                    let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
                    perform_one_cache(&trees, &hard_cacher, drive_item).await;
                }
            }
        }

        // Handle the removals
        for r in removals {
            if let Change::Removed(id) = r {
                // Lookup inode and remove it
                if let Some(inode) = trees.access_tree.get(id.as_bytes()) {
                    if let Some(drive_item_bytes) = trees.inode_tree.get(&inode) {
                        let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);

                        // Remove from hard_cache and hc_meta_tree
                        if let ArchivedDriveItemData::FileItem {
                            file_name: _,
                            data_id,
                            size: _,
                        } = &drive_item.data
                        {
                            let data_id = data_id.deserialize(&mut AllocDeserializer).unwrap();
                            let data_id_key = serialize_rkyv(&data_id);

                            trees.hc_meta_tree.remove(data_id_key.as_slice());
                            drive_access.clear_cache_item(data_id).await;
                        }
                    }
                    trees.inode_tree.remove(inode);
                }
                // Lookup raccess_tree entry and remove item from lookup_tree and parent dir
                if let Some(raccess_data) = trees.raccess_tree.get(id.as_bytes()) {
                    let raccess = get_rkyv::<ReverseAccess>(&raccess_data);

                    // Remove from parent dir
                    if let Some(parent_drive_item_bytes) =
                        trees.inode_tree.get(raccess.parent_inode.to_le_bytes())
                    {
                        let parent_drive_item = get_rkyv::<DriveItem>(&parent_drive_item_bytes);
                        let mut new_items = vec![];
                        if let ArchivedDriveItemData::Dir { items } = &parent_drive_item.data {
                            for item in items.iter() {
                                if item.name != raccess.name {
                                    new_items
                                        .push(item.deserialize(&mut AllocDeserializer).unwrap())
                                }
                            }
                        }

                        let new_parent_drive_item = DriveItem {
                            access_id: parent_drive_item.access_id.to_string(),
                            modified_time: parent_drive_item.modified_time,
                            data: DriveItemData::Dir { items: new_items },
                        };

                        let new_parent_drive_item_bytes = serialize_rkyv(&new_parent_drive_item);
                        trees.inode_tree.insert(
                            raccess.parent_inode.to_le_bytes(),
                            new_parent_drive_item_bytes.as_slice(),
                        );
                    }

                    // Remove from lookup tree
                    let lookup_key = make_lookup_key(raccess.parent_inode, &raccess.name);
                    trees.lookup_tree.remove(lookup_key.as_slice());
                }

                // Remove raccess_tree entry
                trees.raccess_tree.remove(id.as_bytes());

                // Remove access_tree entry
                trees.access_tree.remove(id.as_bytes());
            }
        }
    }
}

/// Scan the drive for new files.
async fn perform_scan(
    trees: &ScanTrees,
    mut scan_stream: Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin>,
) {
    // Start scan, saving the last_scan_start time
    let recent_now = (Utc::now() - Duration::hours(1)).timestamp();
    trees
        .scan_tree
        .insert(b"last_scan_start", &recent_now.to_le_bytes());

    loop {
        if let Some(page) = scan_stream.try_next().await.unwrap() {
            handle_one_page(trees, &page.items).await;

            if let Some(next_page_token) = page.next_page_token.as_deref() {
                // println!("NEXT PAGE TOKEN: {}", next_page_token);
                let next_page_token_bytes = serialize_rkyv(&next_page_token.to_string());
                trees
                    .scan_tree
                    .insert(b"last_page_token", next_page_token_bytes.as_slice());
            }
        } else {
            break;
        }
    }

    // Finalize scan, moving last_scan_start to last_modified_date, and removing last_page_token
    trees.scan_tree.remove(b"last_page_token");
    if let Some(last_scan_start) = trees.scan_tree.get(b"last_scan_start") {
        trees
            .scan_tree
            .insert(b"last_modified_date", last_scan_start);
        trees.scan_tree.remove(b"last_scan_start");
    }
}

/// Use HardCacher to hard cache data required for all files.
async fn perform_caching(trees: &ScanTrees, drive_access: Arc<DriveAccess>) {
    // Start 10 threads for caching
    let (send, recv) = flume::bounded(1);
    let mut thread_joiners = vec![];
    for _ in 0..1 {
        thread_joiners.push(tokio::spawn(caching_thread(
            trees.clone(),
            drive_access.clone(),
            recv.clone(),
        )));
    }

    for item in trees.inode_tree.iter() {
        let (_, value) = item.unwrap();
        send.send_async(value).await.unwrap();
    }

    join_all(thread_joiners).await;
}

async fn caching_thread(trees: ScanTrees, drive_access: Arc<DriveAccess>, recv: Receiver<IVec>) {
    //println!("Starting caching thread for {}", drive_access.name);
    {
        let hard_cacher = HardCacher::new(drive_access.clone(), 1_000_000); // TODO: not hardcode min_size
        let mut stream = recv.into_stream();
        while let Some(item) = stream.next().await {
            let item = get_rkyv::<DriveItem>(&item);
            perform_one_cache(&trees, &hard_cacher, item).await;
        }
    }

    // println!("Caching thread for {} complete.", drive_access.name);
}

/// Cache one single item. Returns true if successful, or false if attempt needs to be made later.
async fn perform_one_cache(trees: &ScanTrees, hard_cacher: &HardCacher, item: &ArchivedDriveItem) -> bool {
    if let ArchivedDriveItemData::FileItem {
        file_name,
        data_id,
        size,
    } = &item.data
    {
        let data_id = data_id.deserialize(&mut AllocDeserializer).unwrap();
        let data_id_key = serialize_rkyv(&data_id);
        let hc_bytes = trees.hc_meta_tree.get(data_id_key.as_slice());
        let hc_meta = hc_bytes
            .as_deref()
            .map(|m| get_rkyv::<HardCacheMetadata>(m));

        // KTODO: handle CacheHandlerErrors here
        match hard_cacher
                    .process(item.access_id.as_str(), file_name, &data_id, *size, hc_meta)
                    .await {
            Ok(meta) => {
                let meta_bytes = serialize_rkyv(&meta);
                trees
                    .hc_meta_tree
                    .insert(data_id_key.as_slice(), meta_bytes);
                true
            }
            Err(e) => {
                println!("GOT AN ERROR!!!");
                dbg!(&e);
                println!("TODO: try again later");
                false
            }
        }
    } else {
        // Was a directory rather than a file, so accept and don't try again.
        true
    }
}

async fn handle_one_page(trees: &ScanTrees, page_items: &Vec<PageItem>) {
    for (p, i) in page_items
        .iter()
        .group_by(|f| f.parent.clone())
        .into_iter()
        .map(|(s, d)| (s, d.collect()))
    {
        tokio::task::block_in_place(|| handle_add_items(trees, p.as_ref(), &i))
    }

    for p in page_items.iter().filter(|x| x.file_info.is_none()) {
        let modified_time = p.modified_time.timestamp();
        tokio::task::block_in_place(|| handle_update_parent(trees, &p.id, modified_time))
    }
}

/// Add items shared within a parent. First, add the items individually, then
/// create the parent with these items, or merge the items into an existing parent.
fn handle_add_items(trees: &ScanTrees, parent: &str, items: &Vec<&PageItem>) {
    // If not set, 1 is set as the next available inode. Inode 0 is reserved for the root of the drive.
    // Stored as a u64, but shouldn't have a value that goes over 2^48.
    // That's 281 trillion though, which should never be reached even in extreme circumstances.

    // Figure out the inode for this parent.
    let parent_inode = if parent == trees.drive_id {
        // Root directory, so always set inode to 0
        0
    } else if let Some(ei) = trees.access_tree.get(parent.as_bytes()) {
        // Directory exists, so merge new files in
        u64::from_le_bytes(ei.as_ref().try_into().unwrap())
    } else {
        // Directory doesn't exist, so allocate a new inode
        let parent_inode = trees.next_inode();
        parent_inode
    };

    // Keep a hashmap if ID -> inode, to map items in the parent
    let mut item_inodes: HashMap<String, u64> = HashMap::new();

    for item in items.iter() {
        let lookup_key = make_lookup_key(parent_inode, &item.name);
        // Don't add if the item already exists
        if let Some(inode_bytes) = trees.access_tree.get(item.id.as_bytes()) {
            // Make sure the lookup key still exists though
            let inode = u64::from_le_bytes(inode_bytes.as_ref().try_into().unwrap());
            item_inodes.insert(item.id.clone(), inode);
            trees
                .lookup_tree
                .insert(lookup_key.as_slice(), inode_bytes.as_ref());
            continue;
        }

        // Build DriveItem
        let drive_item = DriveItem {
            access_id: item.id.clone(),
            modified_time: item.modified_time.timestamp(),
            data: if let Some(ref fi) = item.file_info {
                DriveItemData::FileItem {
                    file_name: item.name.clone(),
                    data_id: fi.data_id.clone(),
                    size: fi.size,
                }
            } else {
                DriveItemData::Dir { items: vec![] }
            },
        };

        let drive_item_bytes = serialize_rkyv(&drive_item);

        let next_inode = trees.next_inode();

        item_inodes.insert(item.id.clone(), next_inode);

        // Add the inode key, which stores the actual drive item.
        trees
            .inode_tree
            .insert(&next_inode.to_le_bytes(), drive_item_bytes.as_slice());

        // Add the access and lookup keys, which reference the inode.
        trees
            .access_tree
            .insert(item.id.as_bytes(), &next_inode.to_le_bytes());
        trees
            .lookup_tree
            .insert(lookup_key.as_slice(), &next_inode.to_le_bytes());

        let raccess = ReverseAccess {
            parent_inode,
            name: item.name.clone(),
        };

        let raccess_data = serialize_rkyv(&raccess);

        trees
            .raccess_tree
            .insert(item.id.as_bytes(), raccess_data.as_slice());
    }

    // println!("items had {} len", items.len());
    // println!("item_inodes had {} len", item_inodes.len());

    // Merge items into parent
    let items = items
        .iter()
        .filter(|x| item_inodes.contains_key(&x.id))
        .map(|x| DirItem {
            name: x.name.clone(),
            access_id: x.id.clone(),
            inode: *item_inodes.get(&x.id).unwrap(),
            is_dir: x.file_info.is_none(),
        })
        .collect::<Vec<_>>();

    // Add existing items to new_items, if parent already exists
    let (parent_modified_time, new_items) =
        if let Some(existing_parent) = trees.inode_tree.get(&parent_inode.to_le_bytes()) {
            let existing_parent = get_rkyv::<DriveItem>(&existing_parent);
            let mut new_items = items.clone();
            let parent_modified_time = merge_parent(existing_parent, &mut new_items);
            (parent_modified_time, new_items)
        } else {
            (0, items)
        };

    // Create new parent and add all files to it
    let parent_drive_item = DriveItem {
        access_id: parent.to_string(),
        modified_time: parent_modified_time,
        data: DriveItemData::Dir { items: new_items },
    };

    let parent_drive_item_bytes = serialize_rkyv(&parent_drive_item);
    trees.inode_tree.insert(
        &parent_inode.to_le_bytes(),
        parent_drive_item_bytes.as_slice(),
    );
    trees
        .access_tree
        .insert(parent.as_bytes(), &parent_inode.to_le_bytes());
}

fn handle_update_parent(trees: &ScanTrees, parent: &str, modified_time: i64) {
    // Get the inode for the parent
    if let Some(existing_inode) = trees.access_tree.get(&parent.as_bytes()) {
        // Get the drive item data, to update the modified_time
        if let Some(existing_parent) = trees.inode_tree.get(&existing_inode) {
            let old_data = existing_parent.to_vec();
            let old_drive_item = get_rkyv::<DriveItem>(&old_data);
            if old_drive_item.modified_time == modified_time {
                return;
            }
            let new_drive_item = DriveItem {
                access_id: old_drive_item.access_id.to_string(),
                modified_time,
                data: old_drive_item
                    .data
                    .deserialize(&mut AllocDeserializer)
                    .unwrap(),
            };

            let new_drive_bytes = serialize_rkyv(&new_drive_item);
            trees
                .inode_tree
                .insert(existing_inode, new_drive_bytes.as_slice());
        }
    }
}

/// Merge an existing DriveItem parent into a list of new items.
///
/// Returns the modified time of the parent.
fn merge_parent(existing_parent: &ArchivedDriveItem, items: &mut Vec<DirItem>) -> i64 {
    let new_item_set = items.clone();
    let new_item_set: HashSet<&str> = new_item_set.iter().map(|x| x.access_id.as_str()).collect();

    if let ArchivedDriveItemData::Dir {
        items: existing_items,
    } = &existing_parent.data
    {
        for item in existing_items.iter() {
            if !new_item_set.contains(item.access_id.as_str()) {
                items.push(DirItem {
                    name: item.name.to_string(),
                    access_id: item.access_id.to_string(),
                    inode: item.inode,
                    is_dir: item.is_dir,
                })
            }
        }
        existing_parent.modified_time
    } else {
        0
    }
}

/// Get the size of a drive.
///
/// Returns number of files and total size in bytes.
async fn get_drive_size(inode_tree: &Tree) -> (u64, u64) {
    // Since this could take a while, throw this into a different thread
    let inode_tree = inode_tree.clone();
    tokio::task::spawn_blocking(|| inner_get_drive_size(inode_tree))
        .await
        .unwrap()
}

fn inner_get_drive_size(inode_tree: Tree) -> (u64, u64) {
    let mut total_count = 0;
    let mut total_size = 0;
    for item in inode_tree.iter() {
        let (_, value) = item.unwrap();
        let data = get_rkyv::<DriveItem>(&value);
        total_count += 1;
        if let ArchivedDriveItemData::FileItem {
            file_name: _,
            data_id: _,
            size,
        } = data.data
        {
            total_size += size;
        }
    }
    (total_count, total_size)
}

// TODO: move to types
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
struct ScanState {
    last_page_token: String,
    last_modified_date: i64,
}
