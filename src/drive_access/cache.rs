use std::{
    borrow::Cow,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use rclone_crypt::decrypter::{self, Decrypter};
use tracing::instrument;

use crate::{
    cache_handlers::{
        crypt_context::CryptContext, crypt_passthrough::CryptPassthrough, CacheDriveHandler,
        CacheFileHandler, CacheHandlerError, DownloaderError,
    },
    db::{types::*, Db, DbAccess},
    hard_cache::HardCacheMetadata,
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
/// This is for drives that use the cache backend.
///
/// This is where directory traversal is handled, as well as
/// things like encrypted drives and custom root paths.
///
/// Many of the methods contain a `to_t` or `for_each` closure
/// parameter. This allows for drive access with minimal copying.
pub struct CacheDriveAccess {
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

impl std::fmt::Debug for CacheDriveAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriveAccess")
            .field("name", &self.name)
            .finish()
    }
}

impl CacheDriveAccess {
    /// Create a new DriveAccess.
    pub fn new(
        name: String,
        cache_handler: Box<dyn CacheDriveHandler>,
        db: Db,
        root_path: Option<PathBuf>,
        uses_crypt: bool,
        crypts: Vec<Arc<CryptContext>>,
    ) -> CacheDriveAccess {
        let db = db.drive_access(
            cache_handler.get_drive_type(),
            &cache_handler.get_drive_id(),
        );
        let root_inode = AtomicU64::new(u64::MAX);
        // Scanning is considered "on" by default.
        let scanning = AtomicBool::new(true);
        CacheDriveAccess {
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

    pub async fn open_file(
        &self,
        inode: u64,
        offset: u64,
    ) -> Result<TypeResult<Box<dyn CacheFileHandler>>, CacheHandlerError> {
        Ok(if let Some(drive_item) = self.db.get_inode(inode) {
            let drive_item = drive_item.borrow_dependent().archived;
            if let ArchivedDriveItemData::FileItem {
                file_name,
                data_id,
                size,
            } = &drive_item.data
            {
                println!("Opened file {}", file_name);
                let mut file = self
                    .cache_handler
                    .open_file(
                        drive_item.access_id.to_string(),
                        data_id.into(),
                        size.value(),
                        offset,
                        false,
                    )
                    .await?;

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
        })
    }

    /// Clear an item from the cache, if it exists.
    ///
    /// This is called when a file gets deleted from the
    /// underlying drive.
    pub async fn clear_cache_item(&self, data_id: DataIdentifier) {
        self.cache_handler.clear_cache_item(data_id).await;
    }

    pub async fn get_cache_metadata(
        &self,
        data_id: DataIdentifier,
    ) -> Result<HardCacheMetadata, CacheHandlerError> {
        self.cache_handler.get_cache_metadata(data_id).await
    }

    pub async fn set_cache_metadata(
        &self,
        data_id: DataIdentifier,
        meta: HardCacheMetadata,
    ) -> Result<(), CacheHandlerError> {
        self.cache_handler.set_cache_metadata(data_id, meta).await
    }

    /// Check if a directory exists and is a directory.
    /// If it is, return bool that states if scanning is in progress.
    pub fn check_dir(&self, inode: u64) -> Result<TypeResult<bool>, CacheHandlerError> {
        let scanning = self.scanning.load(Ordering::Acquire);
        Ok(if let Some(drive_item) = self.db.get_inode(inode) {
            let drive_item = drive_item.borrow_dependent().archived;
            if let ArchivedDriveItemData::Dir { items: _ } = drive_item.data {
                TypeResult::IsType(scanning)
            } else {
                TypeResult::IsNotType
            }
        } else {
            TypeResult::DoesNotExist
        })
    }

    /// Look up a filename within an inode, assuming the inode is a directory.
    ///
    /// If inode is file or file_name doesn't exist within the inode, return None.
    pub fn lookup<T>(
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

        let drive_item = self.db.get_inode(inode)?;
        let drive_item = drive_item.borrow_dependent().archived;
        Some(to_t(inode, drive_item))
    }

    /// Unlink a filename within an inode, assuming the inode is a directory.
    ///
    /// If inode is file or file_name doesn't exist within the inode, return None.
    pub async fn unlink(&self, parent: u64, file_name: &str) -> Option<()> {
        // KTODO: extract this to function
        let parent_inode = if parent == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            parent
        };

        // If we're using encryption, try to look up the encrypted file_name.
        // KTODO: extract this to function
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
        let drive_item = self.db.get_inode(inode)?;
        let drive_item = drive_item.borrow_dependent().archived;

        // KTODO: handle directories
        match &drive_item.data {
            ArchivedDriveItemData::FileItem {
                file_name: _,
                data_id,
                size: _,
            } => {
                let file_id = drive_item.access_id.to_string();
                let data_id = data_id.into();
                if let Err(e) = self.cache_handler.unlink_file(file_id, data_id).await {
                    // Allow NotFound errors through
                    if !matches!(e, CacheHandlerError::Downloader(DownloaderError::NotFound)) {
                        return None;
                    }
                }
            }
            ArchivedDriveItemData::Dir { items: _ } => todo!(),
        };

        self.db.rm_child(parent_inode, &file_name)?;

        Some(())
    }

    /// Rename and/or move a file.
    #[instrument]
    pub async fn rename(
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
        // Short-circuit if no action is being performed
        if new_parent.is_none() && new_name.is_none() {
            return Some(());
        }

        // KTODO: extract this to function
        let parent_inode = if parent == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            parent
        };

        // If we're using encryption, try to look up the encrypted file_name.
        // Make sure to use the same CryptContext to re-encrypt new file
        // KTODO: extract (part of) this to function
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
        let drive_item = self.db.get_inode(inode)?;
        let drive_item = drive_item.borrow_dependent().archived;
        let item_file_id = drive_item.access_id.to_string();

        // Get parent's file_id
        let parent_item = self.db.get_inode(parent_inode)?;
        let parent_item = parent_item.borrow_dependent().archived;
        let parent_file_id = parent_item.access_id.to_string();

        // Get new_parent's file_id
        let new_parent_file_id = if let Some(new_parent) = new_parent {
            let new_parent_item = self.db.get_inode(new_parent)?;
            let new_parent_item = new_parent_item.borrow_dependent().archived;
            Some(new_parent_item.access_id.to_string())
        } else {
            None
        };

        // Get old_new_parent_ids
        let old_new_parent_ids = new_parent_file_id.map(|npfi| (parent_file_id, npfi));

        // Re-encrypt the new filename if original filename was decrypted
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
                item_file_id.to_string(),
                old_new_parent_ids,
                new_file_name.clone(),
            )
            .await
            .ok()?;

        // Perform the move or rename in the DB
        if let Some(new_parent_inode) = new_parent {
            self.db.move_child(
                parent_inode,
                new_parent_inode,
                &file_name,
                new_file_name.as_deref(),
            )?;
        } else if let Some(new_file_name) = new_file_name {
            self.db
                .rename_child(parent_inode, &file_name, &new_file_name)?;
        }

        Some(())
    }

    /// Get the attributes for a given inode.
    pub fn getattr<T>(&self, inode: u64, to_t: impl FnOnce(&ArchivedDriveItem) -> T) -> Option<T> {
        let drive_item = self.db.get_inode(inode)?;
        let drive_item = drive_item.borrow_dependent().archived;
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

        let dir = self.db.get_inode(inode)?;
        let dir = dir.borrow_dependent().archived;

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
        self.db.get_lookup(parent_inode, file_name)
    }
}
