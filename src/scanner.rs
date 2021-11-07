use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use chrono::{Duration, Utc};
use flume::Receiver;
use futures::{future::join_all, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use sled::IVec;

use crate::{
    cache_handlers::{DownloaderError, Page, PageItem},
    db::{
        types::{
            ArchivedDriveItem, ArchivedDriveItemData, DirItem, DriveItem, DriveItemData,
            ReverseAccess,
        },
        DbAccess,
    },
    downloaders::Change,
    drive_access::DriveAccess,
    hard_cache::HardCacher,
    tweaks,
};

/// Main async task that runs the scanning for this drive.
pub async fn scan_thread(drive_access: Arc<DriveAccess>) {
    let name = drive_access.name.clone();
    let drive_type = drive_access.cache_handler.get_drive_type();
    let drive_id = drive_access.cache_handler.get_drive_id().clone();
    println!("Scanning {} ({}:{})...", name, drive_type, drive_id);
    drive_access.scanning.store(true, Ordering::Release);

    let db = drive_access.db.clone();

    // Start watcher thread
    tokio::spawn(watch_thread(db.clone(), drive_access.clone()));

    let last_page_token = db.get_last_page_token();

    let last_modified_date = db.get_last_modified_date();

    let scan_stream = drive_access
        .cache_handler
        .scan_pages(last_page_token, last_modified_date);

    perform_scan(&db, scan_stream).await;

    println!("Finished scanning {} ({}:{}).", name, drive_type, drive_id);
    drive_access.scanning.store(false, Ordering::Release);

    let (count, size) = db.get_drive_size().await;

    println!(
        "Caching {} items of size {} for drive {} ({}:{})...",
        count, size, name, drive_type, drive_id
    );

    if tweaks().disable_scanner_caching {
        println!("TWEAK: disabling scanner caching");
    } else {
        perform_caching(&db, drive_access).await;
    }

    println!("Finished caching {} ({}:{}).", name, drive_type, drive_id);
}

/// Async task that watches for changes to this drive.
async fn watch_thread(db: DbAccess, drive_access: Arc<DriveAccess>) {
    let mut change_stream = drive_access.cache_handler.watch_changes();
    let hard_cacher = HardCacher::new(drive_access.clone(), 1_000_000); // TODO: not hardcode min_size

    while let Some(changes) = change_stream.next().await {
        let changes = changes.unwrap();
        // Split this list of changes into additions and removals.
        let (additions, removals): (Vec<_>, _) = changes
            .into_iter()
            .partition(|c| matches!(c, Change::Added(_)));

        // Handle the additions.
        let page_items: Vec<PageItem> = additions
            .into_iter()
            .filter_map(|c| {
                if let Change::Added(page_item) = c {
                    Some(page_item)
                } else {
                    None
                }
            })
            .collect();

        // Add new additions to database.
        handle_one_page(&db, &page_items).await;

        // Hard cache the added files.
        for page_item in page_items {
            if let Some(drive_item) = db.get_access(&page_item.id) {
                let drive_item = drive_item.borrow_dependent().archived;
                perform_one_cache(&hard_cacher, drive_item).await;
            }
        }

        // Handle the removals.
        for r in removals {
            if let Change::Removed(id) = r {
                if let Some((_, drive_item)) = db.rm_access_full(&id) {
                    let drive_item = drive_item.borrow_dependent().archived;
                    // Remove from hard_cache and hc_meta_tree.
                    if let ArchivedDriveItemData::FileItem {
                        file_name: _,
                        data_id,
                        size: _,
                    } = &drive_item.data
                    {
                        drive_access.clear_cache_item(data_id.into()).await;
                    }
                }

                if let Some(raccess_data) = db.rm_raccess(&id) {
                    let raccess_data = raccess_data.borrow_dependent().archived;
                    db.rm_child(raccess_data.parent_inode.value(), &raccess_data.name);
                }
            }
        }
    }
}

/// Scan the drive for new files.
async fn perform_scan(
    db: &DbAccess,
    mut scan_stream: Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin>,
) {
    // Start scan, saving the last_scan_start time
    let recent_now = (Utc::now() - Duration::hours(1)).timestamp();
    db.set_last_scan_start(recent_now);

    while let Some(page) = scan_stream.try_next().await.unwrap() {
        handle_one_page(db, &page.items).await;
        if let Some(next_page_token) = page.next_page_token.as_deref() {
            db.set_last_page_token(next_page_token);
        }
    }

    // Finalize scan, moving last_scan_start to last_modified_date, and removing last_page_token
    db.set_lmd_to_lss();
    db.rm_last_page_token();
}

/// Use HardCacher to hard cache data required for all files.
async fn perform_caching(db: &DbAccess, drive_access: Arc<DriveAccess>) {
    // Start 10 threads for caching
    let mut thread_joiners = vec![];
    {
        let (send, recv) = flume::bounded(1);
        for _ in 0..10 {
            thread_joiners.push(tokio::spawn(caching_thread(
                db.clone(),
                drive_access.clone(),
                recv.clone(),
            )));
        }

        let mut inode_iter = db.iter_inode();
        while let Some(item) = inode_iter.next() {
            send.send_async(item).await.unwrap();
        }
    }
    join_all(thread_joiners).await;
}

async fn caching_thread(db: DbAccess, drive_access: Arc<DriveAccess>, recv: Receiver<IVec>) {
    let hard_cacher = HardCacher::new(drive_access.clone(), 1_000_000); // TODO: not hardcode min_size
    let mut stream = recv.into_stream();
    while let Some(data) = stream.next().await {
        let item = db.get_inode_from_data(data).unwrap();
        let item = item.borrow_dependent().archived;
        perform_one_cache(&hard_cacher, item).await;
    }
}

/// Cache one single item. Returns true if successful, or false if attempt needs to be made later.
async fn perform_one_cache(
    hard_cacher: &HardCacher,
    item: &ArchivedDriveItem,
) -> bool {
    if let ArchivedDriveItemData::FileItem {
        file_name,
        data_id,
        size,
    } = &item.data
    {
        let data_id = data_id.into();

        hard_cacher
            .process(item.access_id.as_str(), file_name, &data_id, size.value())
            .await
            .is_ok()
    } else {
        // Was a directory rather than a file, so accept and don't try again.
        true
    }
}

/// Handle adding one page of scanned items to the database.
///
/// Groups items into their respective parents, and merges everything.
async fn handle_one_page(db: &DbAccess, page_items: &[PageItem]) {
    for (p, i) in page_items
        .iter()
        .group_by(|f| f.parent.clone())
        .into_iter()
        .map(|(s, d)| (s, d.collect::<Vec<&PageItem>>()))
    {
        tokio::task::block_in_place(|| handle_add_items(db, p.as_ref(), &i))
    }

    for p in page_items.iter().filter(|x| x.file_info.is_none()) {
        let modified_time = p.modified_time.timestamp();
        tokio::task::block_in_place(|| handle_update_parent(db, &p.id, modified_time))
    }
}

/// Add items shared within a parent. First, add the items individually, then
/// create the parent with these items, or merge the items into an existing parent.
fn handle_add_items(db: &DbAccess, parent: &str, items: &[&PageItem]) {
    // If not set, 1 is set as the next available inode. Inode 0 is reserved for the root of the drive.
    // Stored as a u64, but shouldn't have a value that goes over 2^48.
    // That's 281 trillion though, which should never be reached even in extreme circumstances.

    // Figure out the inode for this parent.
    let parent_inode = if parent == db.drive_id {
        // Root directory, so always set inode to 0
        0
    } else if let Some(ei) = db.get_access_to_inode(parent) {
        // Directory exists, so merge new files in
        ei
    } else {
        // Directory doesn't exist, so allocate a new inode
        db.next_inode()
    };

    // Keep a hashmap if ID -> inode, to map items in the parent
    let mut item_inodes: HashMap<String, u64> = HashMap::new();

    for item in items.iter() {
        // Don't add if the item already exists
        if let Some(inode) = db.get_access_to_inode(&item.id) {
            // Make sure the lookup key still exists though
            item_inodes.insert(item.id.clone(), inode);
            db.set_lookup(parent_inode, &item.name, inode);
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

        let next_inode = db.next_inode();

        item_inodes.insert(item.id.clone(), next_inode);

        // Add the inode key, which stores the actual drive item.
        db.set_inode(next_inode, drive_item);

        // Add the access and lookup keys, which reference the inode.
        db.set_access(&item.id, next_inode);
        db.set_lookup(parent_inode, &item.name, next_inode);

        let raccess = ReverseAccess {
            parent_inode,
            name: item.name.clone(),
        };

        db.set_raccess(&item.id, &raccess);
    }

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
    if db.get_inode(parent_inode).is_some() {
        db.add_children(parent_inode, &items);
    } else {
        let parent_drive_item = DriveItem {
            access_id: parent.to_string(),
            modified_time: 0,
            data: DriveItemData::Dir { items },
        };
        db.set_inode(parent_inode, parent_drive_item);
    }
    db.set_access(parent, parent_inode);
}

/// Update the last modified time for a parent.
fn handle_update_parent(db: &DbAccess, parent: &str, modified_time: i64) {
    if let Some(existing_inode) = db.get_access_to_inode(parent) {
        if let Some(old_drive_item) = db.get_inode(existing_inode) {
            let old_drive_item = old_drive_item.borrow_dependent().archived;
            if old_drive_item.modified_time == modified_time {
                return;
            }
            let access_id = old_drive_item.access_id.to_string();
            let data = (&old_drive_item.data).into();
            let new_drive_item = DriveItem {
                access_id,
                modified_time,
                data,
            };

            db.set_inode(existing_inode, new_drive_item);
        }
    }
}
