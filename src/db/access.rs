use std::{
    collections::HashMap,
    convert::TryInto,
    mem::size_of,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use chrono::{DateTime, TimeZone, Utc};
use rkyv::{
    archived_root,
    ser::{serializers::AllocSerializer, Serializer},
};
use self_cell::self_cell;
use sled::{IVec, Iter};

use super::{
    storage::{InnerDb, InnerTree},
    tree::Trees,
    types::*,
};

/// Used to access and perform various actions on the database.
/// Handles both DB storage and serialization.
#[derive(Clone)]
pub struct DbAccess {
    pub drive_id: String,
    trees: Trees,
    next_inode: Arc<AtomicU64>,
}

impl DbAccess {
    pub fn new(db: InnerDb, drive_type: &str, drive_id: &str) -> Self {
        let trees = Trees::new(db, drive_type, drive_id);
        let next_inode = Self::get_next_inode(&trees).unwrap_or(1);
        let next_inode = Arc::new(AtomicU64::new(next_inode));
        DbAccess {
            drive_id: drive_id.to_string(),
            trees,
            next_inode,
        }
    }

    pub fn get_access_to_inode(&self, access_id: &str) -> Option<u64> {
        let data = self.trees.access.get(access_id.as_bytes())?;
        let inode = u64::from_le_bytes(
            data.as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );

        Some(inode)
    }

    pub fn get_access(&self, access_id: &str) -> Option<BorrowedDriveItem> {
        let inode = self.get_access_to_inode(access_id)?;
        self.get_inode(inode)
    }

    pub fn set_access(&self, access_id: &str, inode: u64) -> Option<()> {
        let data = inode.to_le_bytes();
        self.trees.access.set(access_id.as_bytes(), &data);
        Some(())
    }

    pub fn rm_access_full(&self, access_id: &str) -> Option<(u64, BorrowedDriveItem)> {
        let inode = self.rm_access(access_id)?;
        let drive_item = self.rm_inode(inode)?;
        Some((inode, drive_item))
    }

    pub fn rm_access(&self, access_id: &str) -> Option<u64> {
        let data = self.trees.access.remove(access_id.as_bytes())?;
        let inode = u64::from_le_bytes(
            data.as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );

        Some(inode)
    }

    pub fn get_inode(&self, inode: u64) -> Option<BorrowedDriveItem> {
        let data = self.trees.inode.get(inode.to_le_bytes())?;
        let drive_item =
            BorrowedDriveItem::new(data, |data| {
                let archived = unsafe { archived_root::<DriveItem>(data) };
                LDriveItem { archived }
            });
        Some(drive_item)
    }

    pub fn get_inode_from_data(&self, data: IVec) -> Option<BorrowedDriveItem> {
        let drive_item =
            BorrowedDriveItem::new(data, |data| {
                let archived = unsafe { archived_root::<DriveItem>(data) };
                LDriveItem { archived }
            });
        Some(drive_item)
    }

    pub fn iter_inode(&self) -> InodeIterator {
        let inner_iter = self.trees.inode.iter();
        InodeIterator { inner_iter }
    }

    pub fn set_inode(&self, inode: u64, drive_item: DriveItem) -> Option<()> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(&drive_item).unwrap();
        let data = serializer.into_serializer().into_inner();

        self.trees.inode.set(inode.to_le_bytes(), data.as_slice());

        Some(())
    }

    pub fn rm_inode(&self, inode: u64) -> Option<BorrowedDriveItem> {
        let data = self.trees.inode.remove(inode.to_le_bytes())?;
        let drive_item =
            BorrowedDriveItem::new(data, |data| {
                let archived = unsafe { archived_root::<DriveItem>(data) };
                LDriveItem { archived }
            });
        Some(drive_item)
    }

    pub fn get_lookup(&self, parent_inode: u64, child_name: &str) -> Option<u64> {
        let lookup_key = make_lookup_key(parent_inode, child_name);
        let value = self.trees.lookup.get(lookup_key)?;
        let child_inode = u64::from_le_bytes(
            value
                .as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );
        Some(child_inode)
    }

    pub fn set_lookup(&self, parent_inode: u64, child_name: &str, child_inode: u64) -> Option<()> {
        let lookup_key = make_lookup_key(parent_inode, child_name);
        let child_inode = &child_inode.to_le_bytes()[..];
        self.trees.lookup.set(lookup_key, child_inode);
        Some(())
    }

    /// Remove a lookup entry, returning the value.
    pub fn rm_lookup(&self, parent_inode: u64, child_name: &str) -> Option<u64> {
        let lookup_key = make_lookup_key(parent_inode, child_name);
        let value = self.trees.lookup.remove(lookup_key)?;
        let child_inode = u64::from_le_bytes(
            value
                .as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );
        Some(child_inode)
    }

    pub fn set_raccess(&self, access_id: &str, raccess: &ReverseAccess) -> Option<()> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(raccess).unwrap();

        let data = serializer.into_serializer().into_inner();
        self.trees
            .raccess
            .set(access_id.as_bytes(), data.as_slice());

        Some(())
    }

    pub fn rm_raccess(&self, access_id: &str) -> Option<BorrowedReverseAccess> {
        let data = self.trees.raccess.remove(access_id.as_bytes())?;
        let reverse_access =
            BorrowedReverseAccess::new(data, |data| {
                let archived = unsafe { archived_root::<ReverseAccess>(data) };
                LReverseAccess { archived }
            });
        Some(reverse_access)
    }

    pub fn add_children(&self, parent_inode: u64, children: &[DirItem]) -> Option<()> {
        let parent_item = self.get_inode(parent_inode)?;
        let parent_item = parent_item.borrow_dependent().archived;

        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            let mut children_set: HashMap<String, DirItem> = children
                .iter()
                .map(|x| (x.name.to_string(), x.clone()))
                .collect();
            for item in items.iter().map(|x| x.into()) {
                let item: DirItem = item;
                children_set.insert(item.name.to_string(), item);
            }

            let new_items = children_set.into_values().collect();
            let new_parent_drive_item = DriveItem {
                access_id: parent_item.access_id.to_string(),
                modified_time: parent_item.modified_time.value(),
                data: DriveItemData::Dir { items: new_items },
            };

            self.set_inode(parent_inode, new_parent_drive_item);
            Some(())
        } else {
            None
        }
    }

    /// Remove a child from a directory, returning the value.
    pub fn rm_child(&self, parent_inode: u64, child_name: &str) -> Option<()> {
        let parent_item = self.get_inode(parent_inode)?;
        let parent_item = parent_item.borrow_dependent().archived;

        let mut new_items;
        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            new_items = Vec::with_capacity(items.len());
            for item in items.iter() {
                if item.name != child_name {
                    let item: DirItem = item.into();
                    new_items.push(item);
                }
            }
        } else {
            new_items = vec![];
        }

        let new_parent_drive_item = DriveItem {
            access_id: parent_item.access_id.to_string(),
            modified_time: parent_item.modified_time.value(),
            data: DriveItemData::Dir { items: new_items },
        };

        self.set_inode(parent_inode, new_parent_drive_item);
        self.rm_lookup(parent_inode, child_name);

        Some(())
    }

    pub fn rename_child(&self, parent_inode: u64, child_name: &str, new_name: &str) -> Option<()> {
        let parent_item = self.get_inode(parent_inode)?;
        let parent_item = parent_item.borrow_dependent().archived;

        let mut new_items;
        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            new_items = Vec::with_capacity(items.len());
            for item in items.iter() {
                let mut item: DirItem = item.into();
                if item.name == child_name {
                    item.name = new_name.to_string();
                }
                new_items.push(item);
            }
        } else {
            new_items = vec![];
        }

        let new_parent_drive_item = DriveItem {
            access_id: parent_item.access_id.to_string(),
            modified_time: parent_item.modified_time.value(),
            data: DriveItemData::Dir { items: new_items },
        };

        self.set_inode(parent_inode, new_parent_drive_item);

        if let Some(old_inode) = self.rm_lookup(parent_inode, child_name) {
            self.set_lookup(parent_inode, new_name, old_inode);
        }

        Some(())
    }

    pub fn move_child(
        &self,
        parent_inode: u64,
        new_parent_inode: u64,
        child_name: &str,
        new_name: Option<&str>,
    ) -> Option<()> {
        let parent_item = self.get_inode(parent_inode)?;
        let parent_item = parent_item.borrow_dependent().archived;

        // Find original item
        let mut dir_item = None;
        let mut new_items_old_parent;
        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            new_items_old_parent = Vec::with_capacity(items.len());
            for item in items.iter() {
                if item.name == child_name {
                    dir_item = Some(item);
                } else {
                    new_items_old_parent.push(item)
                }
            }
        } else {
            new_items_old_parent = vec![];
        }
        // Bail if file not found
        let mut dir_item: DirItem = dir_item?.into();

        let new_name = new_name.unwrap_or(child_name);

        // Add to new directory
        let mut new_items_new_parent;
        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            dir_item.name = new_name.to_string();
            new_items_new_parent = Vec::with_capacity(items.len() + 1);
            for item in items.iter() {
                if item.name == new_name {
                    // Filename already found in new directory, abort
                    return None;
                }
                new_items_new_parent.push(item);
            }
        } else {
            new_items_new_parent = vec![];
        }

        // Save new directory
        let new_items_new_parent = new_items_new_parent
            .into_iter()
            .map(|ai| ai.into())
            .collect();
        let new_parent_drive_item = DriveItem {
            access_id: parent_item.access_id.to_string(),
            modified_time: parent_item.modified_time.value(),
            data: DriveItemData::Dir {
                items: new_items_new_parent,
            },
        };
        self.set_inode(new_parent_inode, new_parent_drive_item);

        // Save old directory
        let new_items_old_parent = new_items_old_parent
            .into_iter()
            .map(|ai| ai.into())
            .collect();
        let parent_drive_item = DriveItem {
            access_id: parent_item.access_id.to_string(),
            modified_time: parent_item.modified_time.value(),
            data: DriveItemData::Dir {
                items: new_items_old_parent,
            },
        };
        self.set_inode(parent_inode, parent_drive_item);

        let old_inode = self.rm_lookup(parent_inode, child_name)?;
        self.set_lookup(new_parent_inode, new_name, old_inode);

        self.rm_raccess(&parent_item.access_id);
        let new_raccess = ReverseAccess {
            parent_inode: new_parent_inode,
            name: new_name.to_string(),
        };
        self.set_raccess(&parent_item.access_id, &new_raccess);
        Some(())
    }

    pub fn get_last_page_token(&self) -> Option<String> {
        let lpt_data = self.trees.scan.get(b"last_page_token")?;
        let lpt = unsafe { archived_root::<String>(&lpt_data) };
        Some(lpt.to_string())
    }

    pub fn set_last_page_token(&self, page_token: &str) -> Option<()> {
        let mut serializer = AllocSerializer::<4096>::default();
        serializer.serialize_value(&page_token.to_string()).unwrap();

        let data = serializer.into_serializer().into_inner();
        self.trees.scan.set(b"last_page_token", data.as_slice());

        Some(())
    }

    pub fn rm_last_page_token(&self) -> Option<()> {
        self.trees.scan.remove(b"last_page_token");
        Some(())
    }

    pub fn get_last_modified_date_int(&self) -> Option<i64> {
        let lmd_data = self.trees.scan.get(b"last_modified_date")?;
        let lmd_int = i64::from_le_bytes(
            lmd_data
                .as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );
        Some(lmd_int)
    }

    pub fn get_last_modified_date(&self) -> Option<DateTime<Utc>> {
        let lmd_int = self.get_last_modified_date_int()?;
        Some(Utc.timestamp(lmd_int, 0))
    }

    pub fn set_last_modified_date_int(&self, modified_date: i64) -> Option<()> {
        let data = modified_date.to_le_bytes();
        self.trees.scan.set(b"last_modified_date", &data);
        Some(())
    }

    /// Set last_modified_date to last_scan_start value,
    /// and delete last_scan_start.
    pub fn set_lmd_to_lss(&self) -> Option<()> {
        let lmd_int = self.rm_last_scan_start()?;
        self.set_last_modified_date_int(lmd_int);
        Some(())
    }

    pub fn rm_last_scan_start(&self) -> Option<i64> {
        let lss_data = self.trees.scan.remove(b"last_scan_start")?;
        let lss_int = i64::from_le_bytes(
            lss_data
                .as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );
        Some(lss_int)
    }

    pub fn set_last_scan_start(&self, scan_start: i64) -> Option<()> {
        let data = scan_start.to_le_bytes();
        self.trees.scan.set(b"last_scan_start", &data);
        Some(())
    }

    pub fn next_inode(&self) -> u64 {
        let new_inode = self.next_inode.fetch_add(1, Ordering::Acquire);
        self.set_next_inode(new_inode + 1);
        new_inode
    }

    fn get_next_inode(trees: &Trees) -> Option<u64> {
        let ni_data = trees.scan.get(b"next_inode")?;
        let ni_int = u64::from_le_bytes(
            ni_data
                .as_ref()
                .try_into()
                .expect("try_into from_le_bytes failed. DB might be corrupt."),
        );

        Some(ni_int)
    }

    fn set_next_inode(&self, next_inode: u64) -> Option<()> {
        self.trees
            .scan
            .set(b"next_inode", &next_inode.to_le_bytes());
        Some(())
    }

    /// Get the size of a drive.
    ///
    /// Returns number of files and total size in bytes.
    pub async fn get_drive_size(&self) -> (u64, u64) {
        // Since this could take a while, throw this into a different thread
        let inode_tree = self.trees.inode.clone();
        tokio::task::spawn_blocking(|| inner_get_drive_size(inode_tree))
            .await
            .unwrap()
    }
}

fn inner_get_drive_size(inode_tree: InnerTree) -> (u64, u64) {
    let mut total_count = 0;
    let mut total_size = 0;
    for item in inode_tree.iter() {
        let (_, data) = item.unwrap();
        let drive_item = unsafe { archived_root::<DriveItem>(&data) };
        total_count += 1;
        if let ArchivedDriveItemData::FileItem {
            file_name: _,
            data_id: _,
            size,
        } = drive_item.data
        {
            total_size += size.value();
        }
    }
    (total_count, total_size)
}

/// Generate an internal lookup key, given a parent inode and name.
pub fn make_lookup_key(parent_inode: u64, child_name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(size_of::<u64>() + child_name.len());
    let parent_slice = parent_inode.to_le_bytes();
    out.extend_from_slice(&parent_slice);
    out.extend_from_slice(child_name.as_bytes());
    out
}

pub struct InodeIterator {
    inner_iter: Iter,
}

impl InodeIterator {
    pub fn next(&mut self) -> Option<IVec> {
        self.inner_iter
            .next()
            .and_then(|i| i.ok())
            .map(|(_, data)| data)
    }
}

self_cell!(
    pub struct BorrowedDriveItem {
        owner: IVec,

        #[covariant]
        dependent: LDriveItem,
    }
);

pub struct LDriveItem<'a> {
    pub archived: &'a ArchivedDriveItem,
}

self_cell!(
    pub struct BorrowedReverseAccess {
        owner: IVec,

        #[covariant]
        dependent: LReverseAccess,
    }
);

pub struct LReverseAccess<'a> {
    pub archived: &'a ArchivedReverseAccess,
}