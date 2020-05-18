use crate::{
    db_generated::{
        Dir, DirArgs, DirItem, DirItemArgs, DriveItem, DriveItemArgs, DriveItemData, FileItem,
        FileItemArgs,
    },
    downloader::Downloader,
    types::{CfKeys, Page, ScannedItem}, cache_reader::CacheReader, cache::{ReaderFileData, Cache},
};
use anyhow::{bail, format_err, Result};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use chrono::{DateTime, Duration, TimeZone, Utc};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use rocksdb::{DBPinnableSlice, WriteBatch, DB};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

static LAST_PAGE_TOKEN: &'static [u8] = b"last_page_token";
static LAST_SCAN_START: &'static [u8] = b"last_scan_start";
static LAST_MODIFIED_DATE: &'static [u8] = b"last_modified_date";
static NEXT_INODE: &'static [u8] = b"next_inode";

#[derive(Clone)]
/// Cache for a specific drive. Works at the drive ID level.
/// Multiple drives with the same ID will share this cache.
pub struct DriveCache {
    pub id: String,
    /// Downloader for this `DriveCache`.
    downloader: Arc<Downloader>,
    /// Global cache struct that handles soft_cache and hard_cache.
    cache: Arc<Cache>,
    /// RocksDB database.
    db: Arc<DB>,
    /// ColumnFamily keys, for database access.
    cf_keys: CfKeys,
    /// Informational name that can be used in stats and debug messages.
    pub info_name: String,
}

impl DriveCache {
    pub fn new(
        drive_info_name: &str,
        drive_id: &str,
        downloader: Arc<Downloader>,
        cache: Arc<Cache>,
        db: Arc<DB>,
    ) -> Result<DriveCache> {
        let info_name = drive_info_name.to_string();
        let id = drive_id.to_string();

        let cf_keys = CfKeys::new(&id);

        Ok(DriveCache {
            info_name,
            id,
            downloader,
            cache,
            db,
            cf_keys,
        })
    }

    pub fn get_db_key(&self, cf_key: &str, key: &[u8]) -> Result<Option<DBPinnableSlice>> {
        Ok(self
            .db
            .cf_handle(cf_key)
            .ok_or_else(|| {
                format_err!(
                    "Unknown cf_key {} for key {}",
                    cf_key,
                    std::str::from_utf8(key).unwrap_or("unknown")
                )
            })
            .and_then(|cf| {
                self.db
                    .get_pinned_cf(cf, key)
                    .map_err(|x| anyhow::Error::new(x))
            })?)
    }

    pub fn put_db_key(&self, cf_key: &str, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self
            .db
            .cf_handle(cf_key)
            .ok_or_else(|| {
                format_err!(
                    "Unknown cf_key {} for key {}",
                    cf_key,
                    std::str::from_utf8(key).unwrap_or("unknown")
                )
            })
            .and_then(|cf| {
                self.db
                    .put_cf(cf, key, value)
                    .map_err(|x| anyhow::Error::new(x))
            })?)
    }

    pub fn wb_put_db_key(
        &self,
        wb: &mut WriteBatch,
        cf_key: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        Ok(self
            .db
            .cf_handle(cf_key)
            .ok_or_else(|| {
                format_err!(
                    "Unknown cf_key {} for key {}",
                    cf_key,
                    std::str::from_utf8(key).unwrap_or("unknown")
                )
            })
            .and_then(|cf| wb.put_cf(cf, key, value).map_err(|x| anyhow::Error::new(x)))?)
    }

    pub fn del_db_key(&self, cf_key: &str, key: &[u8]) -> Result<()> {
        Ok(self
            .db
            .cf_handle(cf_key)
            .ok_or_else(|| {
                format_err!(
                    "Unknown cf_key {} for key {}",
                    cf_key,
                    std::str::from_utf8(key).unwrap_or("unknown")
                )
            })
            .and_then(|cf| {
                self.db
                    .delete_cf(cf, key)
                    .map_err(|x| anyhow::Error::new(x))
            })?)
    }

    pub async fn count_access_keys(&self) -> Result<(u64, u64)> {
        let db = self.db.clone();
        let cf_key = self.cf_keys.inode_key.clone();
        tokio::task::spawn_blocking(|| Self::inner_count_access_keys(db, cf_key)).await?
    }

    pub fn inner_count_access_keys(db: Arc<DB>, cf_key: String) -> Result<(u64, u64)> {
        let mut iter = db
            .cf_handle(&cf_key)
            .ok_or_else(|| format_err!("Unknown cf_key {} for counting access keys", cf_key))
            .and_then(|cf| db.raw_iterator_cf(cf).map_err(|x| anyhow::Error::new(x)))?;

        let mut count = 0;
        let mut size = 0;

        iter.seek_to_first();
        while iter.valid() {
            if let Some(val) = iter.value() {
                let item: DriveItem = flatbuffers::get_root::<DriveItem>(&val);
                if let Some(data) = item.data_as_file_item() {
                    count += 1;
                    size += data.size_();
                }
            }
            iter.next();
        }
        Ok((count, size))
    }

    pub fn get_scan_state(&self) -> Result<(Option<String>, Option<DateTime<Utc>>)> {
        let last_page_token = self
            .get_db_key(&self.cf_keys.scan_key, LAST_PAGE_TOKEN)?
            .and_then(|x| std::str::from_utf8(&x).ok().map(|y| y.to_string()));

        let last_modified_date = self
            .get_db_key(&self.cf_keys.scan_key, LAST_MODIFIED_DATE)?
            .and_then(|x| {
                if x.len() >= 4 {
                    Some(LittleEndian::read_i64(&x))
                } else {
                    None
                }
            })
            .map(|x| Utc.timestamp(x, 0));

        Ok((last_page_token, last_modified_date))
    }

    pub fn start_scan(&self) -> Result<()> {
        let recent_now = (Utc::now() - Duration::hours(1)).timestamp();
        let mut recent_now_bytes = [0u8; 8];
        LittleEndian::write_i64(&mut recent_now_bytes, recent_now);

        Ok(self.put_db_key(&self.cf_keys.scan_key, LAST_SCAN_START, &recent_now_bytes)?)
    }

    pub fn finish_scan(&self) -> Result<()> {
        self.del_db_key(&self.cf_keys.scan_key, LAST_PAGE_TOKEN)?;

        if let Some(last_scan_start) = self.get_db_key(&self.cf_keys.scan_key, LAST_SCAN_START)? {
            self.put_db_key(&self.cf_keys.scan_key, LAST_MODIFIED_DATE, &last_scan_start)?;
            self.del_db_key(&self.cf_keys.scan_key, LAST_SCAN_START)?;
        };

        Ok(())
    }

    pub fn set_last_page_token(&self, next_page_token: &str) -> Result<()> {
        Ok(self.put_db_key(
            &self.cf_keys.scan_key,
            LAST_PAGE_TOKEN,
            next_page_token.as_bytes(),
        )?)
    }

    pub async fn scan_one_page(
        &self,
        last_page_token: &Option<String>,
        last_modified_date: &Option<DateTime<Utc>>,
    ) -> Result<Page> {
        self.downloader
            .scan_one_page(
                self.id.clone(),
                last_page_token.clone(),
                last_modified_date.clone(),
            )
            .await
    }

    /// Add or merge a set of files into a specified parent.
    pub fn add_items(&self, parent: &str, items: &Vec<ScannedItem>) -> Result<()> {
        // If not set, 1 is set as the next available inode. Inode 0 is reserved for the root of the drive.
        let mut next_inode = self
            .get_db_key(&self.cf_keys.scan_key, NEXT_INODE)?
            .map(|i| LittleEndian::read_u48(&i))
            .unwrap_or(1);

        // Keep a shared buffer for inode bytes, to improve efficiency slightly
        let mut inode_bytes = [0u8; 6];
        LittleEndian::write_u48(&mut inode_bytes, next_inode);

        // Figure out the inode for this parent.
        let parent_inode = if parent == self.id {
            // Root directory, so always set inode to 0
            0
        } else if let Some(ei) = self.get_db_key(&self.cf_keys.access_key, parent.as_bytes())? {
            // Directory exists, so merge new files in
            LittleEndian::read_u48(&ei)
        } else {
            // Directory doesn't exist, so allocate a new inode
            let parent_inode = next_inode;
            next_inode += 1;
            parent_inode
        };

        // For more efficient writes, add everything at once
        let mut wb = WriteBatch::default();

        // Keep a hashmap if ID -> inode, to map items in the parent
        let mut item_inodes: HashMap<String, u64> = HashMap::new();

        for item in items.iter() {
            // Don't add if the item already exists
            if let Some(inode_bytes) =
                self.get_db_key(&self.cf_keys.access_key, item.id.as_bytes())?
            {
                // Make sure the lookup key still exists though
                let inode = LittleEndian::read_u48(&inode_bytes);
                item_inodes.insert(item.id.clone(), inode);
                self.wb_put_db_key(
                    &mut wb,
                    &self.cf_keys.lookup_key,
                    &lookup_key(parent_inode, &item.name),
                    &inode_bytes,
                )?;
                continue;
            }

            let mut fbb = FlatBufferBuilder::new();
            build_drive_item(&mut fbb, item)?;

            LittleEndian::write_u48(&mut inode_bytes, next_inode);
            item_inodes.insert(item.id.clone(), next_inode);

            // Add the inode key
            self.wb_put_db_key(
                &mut wb,
                &self.cf_keys.inode_key,
                &inode_bytes,
                &fbb.finished_data(),
            )?;

            // Add the access key
            self.wb_put_db_key(
                &mut wb,
                &self.cf_keys.access_key,
                item.id.as_bytes(),
                &inode_bytes,
            )?;

            // Add the lookup key
            self.wb_put_db_key(
                &mut wb,
                &self.cf_keys.lookup_key,
                &lookup_key(parent_inode, &item.name),
                &inode_bytes,
            )?;

            next_inode += 1;
        }

        // Merge items into parent
        let items = items
            .iter()
            .filter(|x| item_inodes.contains_key(&x.id))
            .map(|x| {
                (
                    x.name.clone(),
                    x.id.clone(),
                    *item_inodes.get(&x.id).unwrap(),
                    x.file_info.is_none(),
                )
            })
            .collect::<Vec<_>>();

        // Add existing items to new_items, if parent already exists
        LittleEndian::write_u48(&mut inode_bytes, parent_inode);
        let (parent_modified_time, new_items) = if let Some(existing_parent) =
            self.get_db_key(&self.cf_keys.inode_key, &inode_bytes)?
        {
            let existing_parent = flatbuffers::get_root::<DriveItem>(&existing_parent);
            let mut new_items = items.clone();
            let parent_modified_time = merge_parent(existing_parent, &mut new_items);
            (parent_modified_time, new_items)
        } else {
            (0, items)
        };

        // Create new parent and add all files to it
        let mut fbb = FlatBufferBuilder::new();
        build_dir_drive_item(&mut fbb, parent, parent_modified_time, new_items)?;

        LittleEndian::write_u48(&mut inode_bytes, parent_inode);
        self.wb_put_db_key(
            &mut wb,
            &self.cf_keys.inode_key,
            &inode_bytes,
            fbb.finished_data(),
        )?;
        self.wb_put_db_key(
            &mut wb,
            &self.cf_keys.access_key,
            parent.as_bytes(),
            &inode_bytes,
        )?;

        // Update next_inode value
        LittleEndian::write_u48(&mut inode_bytes, next_inode);
        self.wb_put_db_key(&mut wb, &self.cf_keys.scan_key, NEXT_INODE, &inode_bytes)?;

        // Push writebatch
        self.db.write(wb)?;

        Ok(())
    }

    /// Update the metadata on a given parent.
    pub fn update_parent(&self, access_key: &str, modified_time: u64) -> Result<()> {
        let existing_inode = self
            .get_db_key(&self.cf_keys.access_key, access_key.as_bytes())?
            .map(|x| x.to_vec());

        let existing_inode_clone = existing_inode.clone();

        let existing_parent = existing_inode
            .and_then(|i| self.get_db_key(&self.cf_keys.inode_key, &i).transpose())
            .transpose()?;

        let existing_parent = existing_parent
            .as_ref()
            .map(|ep| flatbuffers::get_root::<DriveItem>(&ep))
            .and_then(|di| di.data_as_dir());

        if let Some(existing_parent) = existing_parent {
            if let Some(items) = existing_parent.items() {
                // Update modified_date, keep items
                let mut fbb = FlatBufferBuilder::new();

                let dir_items: Vec<_> = items
                    .into_iter()
                    .flat_map(|item| {
                        if let (Some(name), Some(id)) = (item.name(), item.id()) {
                            let name = Some(fbb.create_string(name));
                            let id = Some(fbb.create_string(id));
                            Some(DirItem::create(
                                &mut fbb,
                                &DirItemArgs {
                                    name,
                                    id,
                                    inode: item.inode(),
                                    is_dir: item.is_dir(),
                                },
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();

                let dir_items_vector = Some(fbb.create_vector(&dir_items));

                let dir = Dir::create(
                    &mut fbb,
                    &DirArgs {
                        items: dir_items_vector,
                        modified_time: modified_time,
                    },
                );

                let id = Some(fbb.create_string(access_key));
                let dir_drive_item = DriveItem::create(
                    &mut fbb,
                    &DriveItemArgs {
                        id,
                        data: Some(dir.as_union_value()),
                        data_type: DriveItemData::Dir,
                    },
                );

                fbb.finish(dir_drive_item, None);
                self.put_db_key(
                    &self.cf_keys.inode_key,
                    &existing_inode_clone.unwrap(),
                    &fbb.finished_data(),
                )?;
            } else {
                bail!("Could not get items from dir.");
            }
        } else {
            bail!("Existing parent is not a directory.");
        }

        Ok(())
    }

    // This is fairly specialized for the sake of FUSE. There's probably a more generic way to do this.
    /// Read all items within a directory, with an offset to start reading at a specific position.
    pub fn read_dir<T>(
        &self,
        inode: u64,
        offset: u64,
        mut to_t: impl FnMut(ReadDirItem) -> Option<T>,
    ) -> Result<Option<Vec<T>>> {
        let mut inode_bytes = [0u8; 6];
        LittleEndian::write_u48(&mut inode_bytes, inode);

        // Get this directory's
        let db_dir = self.get_db_key(&self.cf_keys.inode_key, &inode_bytes)?;
        let dir = db_dir
            .as_ref()
            .map(|d| flatbuffers::get_root::<DriveItem>(d));

        if dir.map(|d| d.data_type() == DriveItemData::Dir) == Some(false) {
            bail!("Not a directory");
        }

        Ok(dir
            .and_then(|d| d.data_as_dir())
            .and_then(|d| d.items())
            .map(|items| {
                items
                    .into_iter()
                    .skip(offset as usize)
                    .enumerate()
                    .flat_map(|(off, item)| {
                        item.name().map(|name| {
                            let inode = item.inode();
                            let is_dir = item.is_dir();
                            let off = offset + off as u64;
                            to_t(ReadDirItem {
                                name,
                                inode,
                                is_dir,
                                off,
                            })
                        })
                    })
                    .take_while(|t| t.is_some())
                    .flat_map(|t| t)
                    .collect()
            }))
    }

    /// Read the metadata of one item.
    pub fn read_item<T>(&self, inode: u64, to_t: impl FnOnce(ReadItem) -> T) -> Result<Option<T>> {
        let mut inode_bytes = [0u8; 6];
        LittleEndian::write_u48(&mut inode_bytes, inode);

        let db_item = self.get_db_key(&self.cf_keys.inode_key, &inode_bytes)?;

        let item = db_item
            .as_ref()
            .map(|d| flatbuffers::get_root::<DriveItem>(d));

        Ok(item.and_then(driveitem_to_readitem).map(to_t))
    }

    /// Try to find the inode from a name, given a parent.
    pub fn lookup_inode(&self, parent: u64, name: &str) -> Result<Option<u64>> {
        let lookup_bytes = lookup_key(parent, name);
        let db_item = self.get_db_key(&self.cf_keys.lookup_key, &lookup_bytes)?;

        Ok(db_item.map(|di| LittleEndian::read_u48(&di)))
    }

    /// Lookup the inode and metadata of one item, given a parent inode and file name.
    pub fn lookup_item<T>(
        &self,
        parent: u64,
        name: &str,
        to_t: impl FnOnce(ReadItem) -> T,
    ) -> Result<Option<(u64, T)>> {
        let lookup_bytes = lookup_key(parent, name);
        let db_item = self.get_db_key(&self.cf_keys.lookup_key, &lookup_bytes)?;

        let inode = db_item.as_ref().map(|di| LittleEndian::read_u48(&di));

        let inode_item = db_item
            .and_then(|di| self.get_db_key(&self.cf_keys.inode_key, &di).transpose())
            .transpose()?;

        let item = inode_item
            .as_ref()
            .map(|d| flatbuffers::get_root::<DriveItem>(d));

        Ok(item
            .and_then(driveitem_to_readitem)
            .map(to_t)
            .map(|t| (inode.unwrap(), t)))
    }

    pub fn open_file(&self, inode: u64) -> Result<Option<CacheReader>> {
        let mut inode_bytes = [0u8; 6];
        LittleEndian::write_u48(&mut inode_bytes, inode);

        let db_item = self.get_db_key(&self.cf_keys.inode_key, &inode_bytes)?;

        let item = db_item
            .as_ref()
            .map(|d| flatbuffers::get_root::<DriveItem>(d));

        let item = item.and_then(|i| {
            match (i.id(), i.data_as_file_item()) {
                (Some(id), Some(item)) => Some((id, item)),
                _ => None
            }
        } );

        if let Some((file_id, file_item)) = item {
            let md5 = hex::encode(file_item.md5().unwrap());
            let file_data = ReaderFileData {
                file_id: file_id.to_string(),
                md5,
                size: file_item.size_(),
                info_inode: inode,
            };
            Ok(Some(CacheReader::new(self.downloader.clone(), self.cache.clone(), file_data)))
        } else {
            Ok(None)
        }
    }
}

/// Convert an internal flatbuffers `DriveItem` into generic `ReadItem`, if possible.
fn driveitem_to_readitem(drive_item: DriveItem) -> Option<ReadItem> {
    let out = match drive_item.data_type() {
        DriveItemData::FileItem => drive_item.data_as_file_item().and_then(|i| {
            i.md5().map(|md5| ReadItem::File {
                md5: md5,
                size: i.size_(),
                modified_time: i.modified_time(),
            })
        }),
        DriveItemData::Dir => drive_item.data_as_dir().map(|i| ReadItem::Dir {
            modified_time: i.modified_time(),
        }),
        _ => None,
    };
    out
}

/// Structure used for reading a single item in a directory.
pub struct ReadDirItem<'a> {
    /// Item name
    pub name: &'a str,
    /// Item's inode
    pub inode: u64,
    /// Whether an item is a directory or file
    pub is_dir: bool,
    /// Item's offset within the directory
    pub off: u64,
}

/// Structure used for reading a single item's metadata.
#[derive(Debug)]
pub enum ReadItem<'a> {
    File {
        md5: &'a [u8],
        size: u64,
        modified_time: u64,
    },
    Dir {
        modified_time: u64,
    },
}

// TODO: refactor this to take a FlatBufferBuilder
/// Merge an existing DriveItem parent into a list of new items.
fn merge_parent(existing_parent: DriveItem, items: &mut Vec<(String, String, u64, bool)>) -> u64 {
    let new_item_set = items.clone();
    let new_item_set: HashSet<&str> = new_item_set.iter().map(|(_, id, _, _)| &id[..]).collect();
    if let Some(existing_parent) = existing_parent.data_as_dir() {
        if let Some(existing_items) = existing_parent.items() {
            for item in existing_items.iter() {
                if !new_item_set.contains(item.id().unwrap()) {
                    items.push((
                        item.name().unwrap().to_string(),
                        item.id().unwrap().to_string(),
                        item.inode(),
                        item.is_dir(),
                    ));
                }
            }
        }

        existing_parent.modified_time()
    } else {
        0
    }
}

/// Build a DriveItem for flatbuffers from a ScannedItem.
fn build_drive_item(fbb: &mut FlatBufferBuilder, scanned_item: &ScannedItem) -> Result<()> {
    let (item_type, item) = if let Some(ref fi) = scanned_item.file_info {
        let md5 = fbb.create_vector_direct(&hex::decode(&fi.md5)?);
        let size = fi.size;

        (
            DriveItemData::FileItem,
            FileItem::create(
                fbb,
                &FileItemArgs {
                    md5: Some(md5),
                    size_: size,
                    modified_time: scanned_item.modified_time.timestamp() as u64,
                },
            )
            .as_union_value(),
        )
    } else {
        let items = fbb.create_vector::<WIPOffset<DirItem>>(&[]);
        (
            DriveItemData::Dir,
            Dir::create(
                fbb,
                &DirArgs {
                    items: Some(items),
                    modified_time: scanned_item.modified_time.timestamp() as u64,
                },
            )
            .as_union_value(),
        )
    };

    let id = Some(fbb.create_string(&scanned_item.id));

    let drive_item = DriveItem::create(
        fbb,
        &DriveItemArgs {
            id,
            data: Some(item),
            data_type: item_type,
        },
    );

    fbb.finish(drive_item, None);

    Ok(())
}

/// Build a Dir-type DriveItem for flatbuffers.
fn build_dir_drive_item(
    fbb: &mut FlatBufferBuilder,
    id: &str,
    modified_time: u64,
    items: Vec<(String, String, u64, bool)>,
) -> Result<()> {
    let dir_items: Vec<_> = items
        .into_iter()
        .map(|(name, id, inode, is_dir)| {
            let name = Some(fbb.create_string(&name));
            let id = Some(fbb.create_string(&id));
            DirItem::create(
                fbb,
                &DirItemArgs {
                    name,
                    id,
                    inode,
                    is_dir,
                },
            )
        })
        .collect();

    let dir_items_vector = fbb.create_vector(&dir_items);
    let dir = Dir::create(
        fbb,
        &DirArgs {
            items: Some(dir_items_vector),
            modified_time,
        },
    );

    let id = Some(fbb.create_string(id));

    let dir_drive_item = DriveItem::create(
        fbb,
        &DriveItemArgs {
            id,
            data: Some(dir.as_union_value()),
            data_type: DriveItemData::Dir,
        },
    );

    fbb.finish(dir_drive_item, None);

    Ok(())
}

/// Generate an internal lookup key, given a parent inode and name.
fn lookup_key(parent: u64, name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + 6);
    out.write_u48::<LittleEndian>(parent)
        .expect("Could not write 6 bytes to vec");
    out.extend_from_slice(name.as_bytes());
    out
}
