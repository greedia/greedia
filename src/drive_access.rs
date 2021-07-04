use std::{borrow::Cow, convert::TryInto, path::{Component, Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}};

use rclone_crypt::decrypter::{self, Decrypter};

use rkyv::de::deserializers::AllocDeserializer;
use rkyv::Deserialize;

use crate::{cache_handlers::{CacheHandlerError, crypt_context::CryptContext, crypt_passthrough::CryptPassthrough}, db::get_rkyv, types::DataIdentifier};
use crate::{
    cache_handlers::{CacheDriveHandler, CacheFileHandler},
    db::{Db, Tree},
    types::{make_lookup_key, ArchivedDirItem, DriveItem},
    types::{ArchivedDriveItem, ArchivedDriveItemData, TreeKeys},
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
    pub db: Db,
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
    /// Database tree that stores a mapping of inodes to DriveItems.
    inode_tree: Tree,
    /// Database tree that stores mapping of (parent_inode, file_name) -> inode.
    /// That inode can then be loaded from inode_tree.
    lookup_tree: Tree,
    /// Optimization, in order to not traverse the root path repeatedly.
    root_inode: AtomicU64,
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
        let tree_keys = TreeKeys::new(
            cache_handler.get_drive_type(),
            &cache_handler.get_drive_id(),
        );
        let inode_tree = db.tree(&tree_keys.inode_key);
        let lookup_tree = db.tree(&tree_keys.lookup_key);
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
            inode_tree,
            lookup_tree,
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
        Ok(if let Some(drive_item_bytes) = self.inode_tree.get(inode.to_le_bytes()) {
            // Load the DriveItem, and check that it's a file and not a directory.
            let drive_item = get_rkyv::<DriveItem>(&drive_item_bytes);
            if let ArchivedDriveItemData::FileItem {
                file_name: _,
                data_id,
                size,
            } = &drive_item.data
            {
                let access_id = drive_item.access_id.to_string();
                let data_id = data_id.deserialize(&mut AllocDeserializer).map_err(|_| CacheHandlerError::DeserializeError)?;
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
                            CryptPassthrough::new(&cc, offset, file).await?,
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

    /// Check if a directory exists and is a directory.
    /// If it is, return bool that states if scanning is in progress.
    pub fn check_dir(&self, inode: u64) -> Result<TypeResult<bool>, CacheHandlerError> {
        let scanning = self.scanning.load(Ordering::Release);
        Ok(if let Some(inode_data) = self.inode_tree.get(inode.to_le_bytes()) {
            let item = get_rkyv::<DriveItem>(&inode_data);
            if let ArchivedDriveItemData::Dir { items: _ } = item.data {
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
                .find_map(|cc| {
                    cc.cipher
                        .encrypt_segment(file_name)
                        .ok()
                        .map(|s| Cow::Owned(s))
                })
                .unwrap_or_else(|| Cow::Borrowed(file_name))
        } else {
            Cow::Borrowed(file_name)
        };

        let inode = self.lookup_inode(inode, &file_name)?;

        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&&drive_item_bytes);
        Some(to_t(inode, drive_item))
    }

    /// Get the attributes for a given inode.
    pub fn getattr_item<T>(
        &self,
        inode: u64,
        to_t: impl FnOnce(&ArchivedDriveItem) -> T,
    ) -> Option<T> {
        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&&drive_item_bytes);
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
