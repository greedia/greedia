use std::{
    borrow::Cow,
    convert::TryInto,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use rclone_crypt::decrypter::{self, Decrypter};
use tracing::instrument;

use crate::{cache_handlers::{
        crypt_context::CryptContext, crypt_passthrough::CryptPassthrough, CacheDriveHandler,
        CacheFileHandler, CacheHandlerError, DownloaderError,
    }, db::{Db, DbAccess}, scanner::merge_parent
    };

/// Generic result that can specify the difference between
/// something not existing, and something being the wrong type.
///
/// For example, if we need to open a directory, we could use
/// this to specify that the type is a directory (along with T),
/// the type is not a directory, or that no file or directory exists.
#[derive(Debug)]
pub enum TypeResult<T> {
    IsType(T),
    IsNotType,
    DoesNotExist,
}

/// Abstraction between the FUSE mount and the drive handlers.
///
/// This is where directory traversal is handled, as well as
/// things like encrypted drives and custom root paths.
///
/// Many of the methods contain a `to_t` or `for_each` closure
/// parameter. This allows for drive access with minimal copying.
pub struct DriveAccess {
    /// The name of the drive
    pub name: String,
    /// Cache handler that handles actual storage and retrieval
    /// of drive data. At the moment, there is only one handler
    /// that stores cache files on disk.
    pub cache_handler: Box<dyn CacheDriveHandler>,
    /// Handle to the database storing all inode-based metadata.
    pub db: DbAccess,
    /// Which directory should be considered the "root path".
    /// Everything below this directory is ignored. On first
    /// traversal, if this path exists, the inode gets stored
    /// in `root_inode`. If None, the root of the drive is
    /// the root for this `DriveAccess`.
    pub root_path: Option<PathBuf>,
    /// List of CryptContexts that could be used on this drive.
    pub crypts: Vec<Arc<CryptContext>>,
    /// This flag will be set to false by the scanner when complete.
    /// Once complete, the only updates to the cache will be done
    /// via the watcher thread, which pushes cache invalidation
    /// messages to the kernel.
    pub scanning: AtomicBool,
    /// Whether or not encryption is used on this drive.
    uses_crypt: bool,
    /// Optimization, in order to not traverse the root path repeatedly.
    root_inode: AtomicU64,
}

impl std::fmt::Debug for DriveAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriveAccess")
            .field("name", &self.name)
            .finish()
    }
}

impl DriveAccess {
    /// Create a new DriveAccess.
    pub fn new(
        name: String,
        cache_handler: Box<dyn CacheDriveHandler>,
        db: Db,
        root_path: Option<PathBuf>,
        uses_crypt: bool,
        crypts: Vec<Arc<CryptContext>>,
    ) -> DriveAccess {
        let db = db.drive_access(cache_handler.get_drive_type(), &cache_handler.get_drive_id());
        let root_inode = AtomicU64::new(u64::MAX);
        // Scanning is considered "on" by default.
        let scanning = AtomicBool::new(true);
        DriveAccess {
            name,
            cache_handler,
            db,
            root_path,
            crypts,
            scanning,
            uses_crypt,
            root_inode,
        }
    }

    /// Given an inode and offset, open a file for reading
    /// within this DriveAccess.
    ///
    /// Returns a CacheFileHandler on success.
    pub async fn open_file(
        &self,
        inode: u64,
        offset: u64,
    ) -> Result<TypeResult<Box<dyn CacheFileHandler>>, CacheHandlerError> {
        // Check if the inode exists in the db's inode_tree.
        Ok(
            if let Some(drive_item_bytes) = self.inode_tree.get(inode.to_le_bytes()) {
                // Load the DriveItem, and check that it's a file and not a directory.
                let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
                if let ArchivedDriveItemData::FileItem {
                    file_name,
                    data_id,
                    size,
                } = &drive_item.data
                {
                    println!("Opened file {}", file_name);
                    let access_id = drive_item.access_id.to_string();
                    let data_id = data_id
                        .deserialize(&mut AllocDeserializer)
                        .map_err(|_| CacheHandlerError::Deserialize)?;
                    // Once we get the data_id from the db, open the file in the CacheHandler.
                    // Don't write to hard cache, as we aren't opening from a scanner.
                    let mut file = self
                        .cache_handler
                        .open_file(access_id, data_id, *size, offset, false)
                        .await?;

                    // Check each CryptContext to see if any of them can decrypt the filename.
                    // If so, create a CryptPassthrough reader for file decryption.
                    for cc in self.crypts.iter() {
                        file.seek_to(0).await?;

                        let mut header = [0u8; decrypter::FILE_HEADER_SIZE];
                        file.read_exact(&mut header).await?;
                        if Decrypter::new(&cc.cipher.file_key, &header).is_ok() {
                            file.seek_to(0).await?;
                            return Ok(TypeResult::IsType(Box::new(
                                CryptPassthrough::new(cc, offset, file).await?,
                            )));
                        }
                    }

                    TypeResult::IsType(file)
                } else {
                    TypeResult::IsNotType
                }
            } else {
                TypeResult::DoesNotExist
            },
        )
    }

    /// Clear an item from the cache, if it exists.
    ///
    /// This is called when a file gets deleted from the
    /// underlying drive.
    pub async fn clear_cache_item(&self, data_id: DataIdentifier) {
        self.cache_handler.clear_cache_item(data_id).await;
    }

    /// Check if a directory exists and is a directory.
    /// If it is, return bool that states if scanning is in progress.
    pub fn check_dir(&self, inode: u64) -> Result<TypeResult<bool>, CacheHandlerError> {
        let scanning = self.scanning.load(Ordering::Acquire);
        Ok(
            if let Some(inode_data) = self.inode_tree.get(inode.to_le_bytes()) {
                let item = get_rkyv::<DriveItem>(&inode_data);
                if let ArchivedDriveItemData::Dir { items: _ } = item.data {
                    TypeResult::IsType(scanning)
                } else {
                    TypeResult::IsNotType
                }
            } else {
                TypeResult::DoesNotExist
            },
        )
    }

    /// Look up a filename within an inode, assuming the inode is a directory.
    ///
    /// If inode is file or file_name doesn't exist within the inode, return None.
    pub fn lookup_item<T>(
        &self,
        inode: u64,
        file_name: &str,
        to_t: impl FnOnce(u64, &ArchivedDriveItem) -> T,
    ) -> Option<T> {
        let inode = if inode == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            inode
        };

        // If we're using encryption, try to look up the encrypted file_name.
        let file_name = if self.uses_crypt {
            self.crypts
                .iter()
                .find_map(|cc| cc.cipher.encrypt_segment(file_name).ok().map(Cow::Owned))
                .unwrap_or(Cow::Borrowed(file_name))
        } else {
            Cow::Borrowed(file_name)
        };

        let inode = self.lookup_inode(inode, &file_name)?;

        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
        Some(to_t(inode, drive_item))
    }

    /// Unlink a filename within an inode, assuming the inode is a directory.
    ///
    /// If inode is file or file_name doesn't exist within the inode, return None.
    pub async fn unlink_item(&self, parent: u64, file_name: &str) -> Option<()> {
        let parent_inode = if parent == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            parent
        };

        // If we're using encryption, try to look up the encrypted file_name.
        let file_name = if self.uses_crypt {
            self.crypts
                .iter()
                .find_map(|cc| cc.cipher.encrypt_segment(file_name).ok().map(Cow::Owned))
                .unwrap_or(Cow::Borrowed(file_name))
        } else {
            Cow::Borrowed(file_name)
        };

        let inode = self.lookup_inode(parent_inode, &file_name)?;

        // Get drive item
        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);

        if let ArchivedDriveItemData::FileItem {
            file_name: _,
            data_id,
            size: _,
        } = &drive_item.data
        {
            let file_id = drive_item.access_id.to_string();
            let data_id = data_id
                .deserialize(&mut AllocDeserializer)
                .map_err(|_| CacheHandlerError::Deserialize)
                .ok()?;

            if let Err(e) = self.cache_handler.unlink_file(file_id, data_id).await {
                // Allow NotFound errors through
                if !matches!(e, CacheHandlerError::Downloader(DownloaderError::NotFound)) {
                    return None;
                }
            }
        } else {
            return None;
        };

        // Get parent's drive item
        let parent_item_bytes = self.inode_tree.get(parent_inode.to_le_bytes())?;
        let parent_item = get_rkyv::<DriveItem>(&parent_item_bytes);

        // Great a new parent drive item, without the unlinked item
        let mut new_items = vec![];
        if let ArchivedDriveItemData::Dir { items } = &parent_item.data {
            for item in items.iter() {
                if item.name != file_name.as_ref() {
                    new_items.push(item.deserialize(&mut AllocDeserializer).unwrap())
                }
            }
        }

        let new_parent_drive_item = DriveItem {
            access_id: parent_item.access_id.to_string(),
            modified_time: parent_item.modified_time,
            data: DriveItemData::Dir { items: new_items },
        };

        let new_parent_drive_item_bytes = serialize_rkyv(&new_parent_drive_item);
        self.inode_tree
            .insert(parent.to_le_bytes(), new_parent_drive_item_bytes.as_slice());

        // Remove the lookup entry
        let old_lookup_key = make_lookup_key(parent_inode, &file_name);
        self.lookup_tree.remove(old_lookup_key);

        Some(())
    }

    /// Rename and/or move a file.
    #[instrument]
    pub async fn rename_item(
        &self,
        parent: u64,
        file_name: &str,
        new_parent: Option<u64>,
        new_name: Option<&str>,
    ) -> Option<()> {
        println!(
            "rename_item ({} {}) -> ({:?} {:?})",
            parent, file_name, new_parent, new_name
        );
        // No action is being performed, so just short-circuit
        if new_parent.is_none() && new_name.is_none() {
            return Some(());
        }

        let parent_inode = if parent == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            parent
        };

        // If we're using encryption, try to look up the encrypted file_name.
        // Make sure to use the same CryptContext to re-encrypt new file
        let (cc, file_name) = if self.uses_crypt {
            self.crypts
                .iter()
                .find_map(|cc| {
                    cc.cipher
                        .encrypt_segment(file_name)
                        .ok()
                        .map(Cow::Owned)
                        .map(|file_name| (Some(cc.clone()), file_name))
                })
                .unwrap_or((None, Cow::Borrowed(file_name)))
        } else {
            (None, Cow::Borrowed(file_name))
        };

        let inode = self.lookup_inode(parent_inode, &file_name)?;

        // Get item's file_id
        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
        let item_file_id = drive_item.access_id.to_string();

        // Get parent's file_id
        let parent_item_bytes = self.inode_tree.get(parent_inode.to_le_bytes())?;
        let parent_item = get_rkyv::<DriveItem>(&parent_item_bytes);
        let parent_file_id = parent_item.access_id.to_string();

        // Get new_parent's file_id
        let new_parent_file_id = if let Some(new_parent) = new_parent {
            let new_parent_item_bytes = self.inode_tree.get(new_parent.to_le_bytes())?;
            let new_parent_item = get_rkyv::<DriveItem>(&new_parent_item_bytes);
            Some(new_parent_item.access_id.to_string())
        } else {
            None
        };

        // Get old_new_parent_ids
        let old_new_parent_ids = new_parent_file_id.map(|npfi| (parent_file_id, npfi));

        // Re-encrypt the new filename if original filename is encrypted
        let new_file_name = if let Some(new_name) = new_name {
            if let Some(cc) = cc {
                cc.cipher.encrypt_segment(new_name).ok()
            } else {
                Some(new_name.to_string())
            }
        } else {
            None
        };

        // Perform the move on the downloader
        self.cache_handler
            .rename_file(
                item_file_id.clone(),
                old_new_parent_ids,
                new_file_name.clone(),
            )
            .await
            .ok()?;

        // Move parents if new_parent set
        // Add to new parent and then remove from old parent.
        // It could be done properly with transactions, but this is good enough for now.
        // Better to have two files than a point in time with zero files, so add first.
        if let Some(new_parent) = new_parent {
            let new_file_name = new_file_name.as_deref().unwrap_or(&file_name);

            // Add to new parent first
            let new_parent_item_bytes = self.inode_tree.get(new_parent.to_le_bytes())?;
            let new_parent_item = get_rkyv::<DriveItem>(&new_parent_item_bytes);
            let mut new_items = vec![DirItem {
                name: new_file_name.to_string(),
                access_id: item_file_id.clone(),
                inode,
                is_dir: matches!(drive_item.data, ArchivedDriveItemData::Dir { items: _ }),
            }];
            let parent_modified_time = merge_parent(new_parent_item, &mut new_items);

            // Create new parent and add all files to it
            let parent_drive_item = DriveItem {
                access_id: parent.to_string(),
                modified_time: parent_modified_time,
                data: DriveItemData::Dir { items: new_items },
            };

            let parent_drive_item_bytes = serialize_rkyv(&parent_drive_item);
            self.inode_tree.insert(
                &parent_inode.to_le_bytes(),
                parent_drive_item_bytes.as_slice(),
            );

            // Add the lookup entry for the new directory item
            let lookup_key = make_lookup_key(new_parent, new_file_name);
            self.lookup_tree
                .insert(lookup_key.as_slice(), &inode.to_le_bytes());

            // Delete the old item, if found
            if let Some(parent_drive_item_bytes) = self.inode_tree.get(parent.to_le_bytes()) {
                let parent_drive_item = get_rkyv::<DriveItem>(&parent_drive_item_bytes);
                let mut new_items = vec![];
                if let ArchivedDriveItemData::Dir { items } = &parent_drive_item.data {
                    for item in items.iter() {
                        if item.name != file_name.as_ref() {
                            new_items.push(item.deserialize(&mut AllocDeserializer).unwrap())
                        }
                    }
                }

                let new_parent_drive_item = DriveItem {
                    access_id: parent_drive_item.access_id.to_string(),
                    modified_time: parent_drive_item.modified_time,
                    data: DriveItemData::Dir { items: new_items },
                };

                let new_parent_drive_item_bytes = serialize_rkyv(&new_parent_drive_item);
                self.inode_tree
                    .insert(parent.to_le_bytes(), new_parent_drive_item_bytes.as_slice());
            }
        } else if let Some(new_file_name) = new_file_name {
            // Rename file in dir item
            if let Some(parent_drive_item_bytes) = self.inode_tree.get(parent.to_le_bytes()) {
                let parent_drive_item = get_rkyv::<DriveItem>(&parent_drive_item_bytes);
                dbg!(&new_file_name, &file_name, &item_file_id, inode);

                let mut new_items = vec![DirItem {
                    name: new_file_name.to_string(),
                    access_id: item_file_id.clone(),
                    inode,
                    is_dir: matches!(drive_item.data, ArchivedDriveItemData::Dir { items: _ }),
                }];
                if let ArchivedDriveItemData::Dir { items } = &parent_drive_item.data {
                    for item in items.iter() {
                        if item.name != file_name.as_ref() {
                            new_items.push(item.deserialize(&mut AllocDeserializer).unwrap())
                        }
                    }
                }

                let new_parent_drive_item = DriveItem {
                    access_id: parent_drive_item.access_id.to_string(),
                    modified_time: parent_drive_item.modified_time,
                    data: DriveItemData::Dir { items: new_items },
                };

                let new_parent_drive_item_bytes = serialize_rkyv(&new_parent_drive_item);
                self.inode_tree
                    .insert(parent.to_le_bytes(), new_parent_drive_item_bytes.as_slice());
            }

            // Add new lookup key, remove old lookup key
            let new_lookup_key = make_lookup_key(parent_inode, &new_file_name);
            self.lookup_tree
                .insert(new_lookup_key.as_slice(), &inode.to_le_bytes());

            let old_lookup_key = make_lookup_key(parent_inode, &file_name);
            self.lookup_tree.remove(old_lookup_key);
        }

        Some(())
    }

    /// Get the attributes for a given inode.
    pub fn getattr_item<T>(
        &self,
        inode: u64,
        to_t: impl FnOnce(&ArchivedDriveItem) -> T,
    ) -> Option<T> {
        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
        Some(to_t(drive_item))
    }

    /// Read the contents of a directory inode.
    ///
    /// The offset allows reading a directory in multiple calls.
    ///
    /// The for_each closure takes in a child inode, the DirItem,
    /// and an optionally overridden filename. The overridden filename
    /// is used when encryption is used, so we can pass a decrypted
    /// filename without modifying the whole DirItem.
    ///
    /// The for_each closure should return false on each iteration to
    /// continue reading. If reading should stop, the closure should
    /// return true.
    pub fn readdir(
        &self,
        inode: u64,
        offset: u64,
        mut for_each: impl FnMut(u64, &ArchivedDirItem, Option<&str>) -> bool,
    ) -> Option<()> {
        let inode = if inode == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            inode
        };

        let mut inode_bytes = [0u8; 8];
        inode_bytes.copy_from_slice(&inode.to_le_bytes());

        // Get the DriveItem for this directory
        let dir_bytes = self.inode_tree.get(&inode_bytes)?;
        let dir = get_rkyv::<DriveItem>(&dir_bytes);

        if let ArchivedDriveItemData::Dir { items } = &dir.data {
            for (off, item) in items.iter().enumerate().skip(offset as usize) {
                let decrypted_file_name = self
                    .crypts
                    .iter()
                    .find_map(|cc| cc.cipher.decrypt_segment(&item.name).ok());
                if for_each(off as u64, item, decrypted_file_name.as_deref()) {
                    // The FUSE entry is full, so stop iterating
                    break;
                }
            }
            Some(())
        } else {
            None
        }
    }

    /// Return the root inode for this drive, given the root_path.
    fn root_inode(&self) -> Option<u64> {
        let root_path = self.root_path.as_deref()?;
        let root_inode = self.root_inode.load(Ordering::Acquire);
        if root_inode == std::u64::MAX {
            let inode_from_path = self.resolve_inode_from_path(0, root_path)?;
            self.root_inode.store(inode_from_path, Ordering::Release);
            Some(inode_from_path)
        } else {
            Some(root_inode)
        }
    }

    /// Follow a path to find an inode. Used to find the root inode when root_path is specified.
    /// Note that paths here are assumed to be unencrypted, even if crypt is enabled.
    fn resolve_inode_from_path(&self, inode: u64, path: &Path) -> Option<u64> {
        let mut last_inode = inode;
        for component in path.components() {
            let cstr = match component {
                Component::Normal(os_str) => os_str.to_str()?,
                _ => continue,
            };
            last_inode = self.lookup_inode(inode, cstr)?;
        }
        Some(last_inode)
    }

    /// Try to find the inode from a name, given a parent.
    ///
    /// Encryption must be handled before calling this method.
    fn lookup_inode(&self, parent_inode: u64, file_name: &str) -> Option<u64> {
        let lookup_bytes = make_lookup_key(parent_inode, file_name);
        let inode_bytes = self.lookup_tree.get(lookup_bytes.as_slice())?;
        Some(u64::from_le_bytes(inode_bytes.as_ref().try_into().ok()?))
    }
}
