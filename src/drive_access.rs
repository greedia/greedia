use crate::{
    cache_reader::CacheRead, config::ConfigDrive, crypt_context::CryptContext,
    drive_cache::DriveCache,
};
use anyhow::Result;
use std::{
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub use crate::{
    drive_cache::{ReadDirItem, ReadItem},
    encrypted_cache_reader::EncryptedCacheReader,
};

pub struct DriveAccess {
    pub name: String,
    root_path: Option<String>,
    root_inode: AtomicU64,
    crypt: Option<CryptContext>,
    cache: Arc<DriveCache>,
}

impl DriveAccess {
    pub fn new(name: String, drive: ConfigDrive, cache: Arc<DriveCache>) -> Result<DriveAccess> {
        let crypt = if let (Some(password1), Some(password2)) = (&drive.password, &drive.password2)
        {
            Some(CryptContext::new(&password1, &password2)?)
        } else {
            None
        };

        Ok(DriveAccess {
            name,
            root_path: drive.root_path,
            root_inode: AtomicU64::new(std::u64::MAX),
            crypt,
            cache,
        })
    }

    pub fn get_scanning(&self) -> bool {
        self.cache.get_scanning()
    }

    /// Return the root inode for this drive.
    pub fn root_inode(&self) -> Option<u64> {
        if let Some(ref root_path) = self.root_path {
            let root_inode = self.root_inode.load(Ordering::Acquire);
            if root_inode == std::u64::MAX {
                let path = PathBuf::from(root_path);
                if let Some(resolved) = self.resolve_inode_from_path(0, &path) {
                    self.root_inode.store(resolved, Ordering::Release);
                    Some(resolved)
                } else {
                    None
                }
            } else {
                Some(root_inode)
            }
        } else {
            // If root_path isn't set, the root inode is 0.
            Some(0)
        }
    }

    /// Follow a path to find an inode. Used to find the root inode when root_path is specified.
    /// Note that paths are assumed to be unencrypted, even if crypt is enabled.
    fn resolve_inode_from_path(&self, inode: u64, path: &Path) -> Option<u64> {
        let mut last_inode = inode;
        for component in path.components() {
            let cstr = match component {
                Component::Normal(os_str) => os_str.to_str()?,
                _ => continue,
            };
            last_inode = self.cache.lookup_inode(inode, cstr).ok().flatten()?;
        }
        Some(last_inode)
    }

    /// Read an item by inode.
    #[inline]
    pub fn read_item<T>(&self, inode: u64, to_t: impl FnOnce(ReadItem) -> T) -> Result<Option<T>> {
        let inode = if inode == 0 {
            self.root_inode()
        } else {
            Some(inode)
        };

        if let Some(inode) = inode {
            self.cache.read_item(inode, to_t)
        } else {
            Ok(None)
        }
    }

    /// Lookup an item by name within a directory, encrypting the item names if needed.
    #[inline]
    pub fn lookup_item<T>(
        &self,
        parent: u64,
        name: &str,
        to_t: impl FnOnce(ReadItem) -> T,
    ) -> Result<Option<(u64, T)>> {
        let parent = if parent == 0 {
            self.root_inode()
        } else {
            Some(parent)
        };

        if let Some(parent) = parent {
            if let Some(ref cc) = self.crypt {
                let name = cc
                    .cipher
                    .encrypt_segment(name)
                    .ok()
                    .unwrap_or_else(|| name.to_string());
                self.cache.lookup_item(parent, &name, to_t)
            } else {
                self.cache.lookup_item(parent, name, to_t)
            }
        } else {
            Ok(None)
        }
    }

    /// Read a directory, decrypting the item names if needed.
    #[inline]
    pub fn read_dir<T>(
        &self,
        inode: u64,
        offset: u64,
        mut to_t: impl FnMut(ReadDirItem) -> Option<T>,
    ) -> Result<Option<Vec<T>>> {
        let inode = if inode == 0 {
            self.root_inode()
        } else {
            Some(inode)
        };

        if let Some(inode) = inode {
            self.cache.read_dir(inode, offset, |read_dir_item| {
                if let Some(ref cc) = self.crypt {
                    let new_name = cc
                        .cipher
                        .decrypt_segment(read_dir_item.name)
                        .ok()
                        .unwrap_or_else(|| read_dir_item.name.to_string());
                    let new_item = ReadDirItem {
                        name: &new_name,
                        inode: read_dir_item.inode,
                        is_dir: read_dir_item.is_dir,
                        off: read_dir_item.off,
                    };
                    to_t(new_item)
                } else {
                    to_t(read_dir_item)
                }
            })
        } else {
            Ok(None)
        }
    }

    pub async fn open_file(&self, inode: u64) -> Option<Box<dyn CacheRead + 'static + Send>> {
        let cache_reader = self.cache.open_file(inode).unwrap()?;
        Some(if let Some(ref cc) = self.crypt {
            Box::new(EncryptedCacheReader::new(&cc.cipher, cache_reader).await.unwrap())
        } else {
            Box::new(cache_reader)
        })
    }

    pub fn check_dir(&self, inode: u64) -> (Option<bool>, bool) {
        self.cache.check_dir(inode).unwrap()
    }
}
