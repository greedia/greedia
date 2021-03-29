use std::{
    convert::TryInto,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::db::get_rkyv;
use crate::{
    cache_handlers::{CacheDriveHandler, CacheFileHandler},
    crypt_context::CryptContext,
    db::{Db, Tree},
    types::{make_lookup_key, ArchivedDirItem, DriveItem},
    types::{ArchivedDriveItem, ArchivedDriveItemData, TreeKeys},
};

pub enum TypeResult<T> {
    IsType(T),
    IsNotType,
    DoesNotExist,
}

pub struct DriveAccess {
    pub name: String,
    pub cache_handler: Box<dyn CacheDriveHandler>,
    pub db: Db,
    pub root_path: Option<PathBuf>,
    pub crypt: Option<CryptContext>,
    inode_tree: Tree,
    lookup_tree: Tree,
    /// Optimization so we don't need to traverse for the root path repeatedly
    root_inode: AtomicU64,
}

impl DriveAccess {
    pub fn new(
        name: String,
        cache_handler: Box<dyn CacheDriveHandler>,
        db: Db,
        root_path: Option<PathBuf>,
        passwords: Option<(String, String)>,
    ) -> DriveAccess {
        let tree_keys = TreeKeys::new(
            cache_handler.get_drive_type(),
            &cache_handler.get_drive_id(),
        );
        let crypt = passwords.map(|(p1, p2)| CryptContext::new(&p1, &p2).unwrap());
        let inode_tree = db.tree(&tree_keys.inode_key);
        let lookup_tree = db.tree(&tree_keys.lookup_key);
        let root_inode = AtomicU64::new(u64::MAX);
        DriveAccess {
            name,
            cache_handler,
            db,
            root_path,
            crypt,
            inode_tree,
            lookup_tree,
            root_inode,
        }
    }

    pub async fn open_file(
        &self,
        inode: u64,
        offset: u64,
    ) -> TypeResult<Box<dyn CacheFileHandler>> {
        todo!()
    }

    /// Check if a directory exists and is a directory.
    /// If it is, return bool that states if scanning is in progress.
    pub fn check_dir(&self, inode: u64) -> TypeResult<bool> {
        let scanning = true; // TODO: not hardcode scanning
        println!("inode {}", inode);
        if let Some(inode_data) = self.inode_tree.get(inode.to_le_bytes()) {
            let item = get_rkyv::<DriveItem>(&inode_data);
            if let ArchivedDriveItemData::Dir { items: _ } = item.data {
                println!("good");
                TypeResult::IsType(scanning)
            } else {
                println!("bad");
                TypeResult::IsNotType
            }
        } else {
            println!("not there");
            TypeResult::DoesNotExist
        }
    }

    pub fn lookup_item<T>(
        &self,
        inode: u64,
        file_name: &str,
        mut to_t: impl FnOnce(u64, &ArchivedDriveItem) -> T,
    ) -> Option<T> {
        let inode = if inode == 0 {
            self.root_inode().unwrap_or(0)
        } else {
            inode
        };

        let inode = if let Some(ref cc) = self.crypt {
            let file_name = cc.cipher.encrypt_segment(file_name).ok().unwrap_or_else(|| file_name.to_string());
            self.lookup_inode(inode, &file_name)
        } else {
            self.lookup_inode(inode, file_name)
        }?;

        let drive_item_bytes = self.inode_tree.get(inode.to_le_bytes())?;
        let drive_item = get_rkyv::<DriveItem>(&&drive_item_bytes);
        Some(to_t(inode, drive_item))
    }

    pub fn getattr_item<T>(
        &self,
        inode: u64,
        mut to_t: impl FnOnce(&ArchivedDriveItem) -> T,
    ) -> Option<T> {
        todo!()
    }

    pub fn readdir(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        mut for_each: impl FnMut(u64, &ArchivedDirItem) -> bool,
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
            for (off, item) in items.iter().skip(offset as usize).enumerate() {
                if for_each(off as u64, item) {
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
        let root_path = self.root_path.as_ref()?;
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
    fn lookup_inode(&self, parent_inode: u64, file_name: &str) -> Option<u64> {
        let lookup_bytes = make_lookup_key(parent_inode, file_name);
        let inode_bytes = self.lookup_tree.get(lookup_bytes.as_slice())?;
        Some(u64::from_le_bytes(inode_bytes.as_ref().try_into().unwrap()))
    }
}
