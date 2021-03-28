use polyfuse::reply::FileAttr;

use crate::{cache_handlers::{CacheDriveHandler, CacheFileHandler}, crypt_context::CryptContext, db::{Db, Tree}, types::{DirItem, DriveItem}, types::{ArchivedDriveItemData, TreeKeys}};
use crate::db::get_rkyv;

pub enum TypeResult<T> {
    IsType(T),
    IsNotType,
    DoesNotExist,
}

pub struct DriveAccess {
    pub name: String,
    pub cache_handler: Box<dyn CacheDriveHandler>,
    pub db: Db,
    inode_tree: Tree,
    crypt: Option<CryptContext>,
}

impl DriveAccess {
    pub fn new(
        name: String,
        cache_handler: Box<dyn CacheDriveHandler>,
        db: Db,
        passwords: Option<(String, String)>,
    ) -> DriveAccess {
        let tree_keys = TreeKeys::new(cache_handler.get_drive_type(), &cache_handler.get_drive_id());
        let inode_tree = db.tree(&tree_keys.inode_key);
        DriveAccess {
            name,
            cache_handler,
            db,
            inode_tree,
            crypt: None,
        }
    }

    pub async fn open_file(&self, inode: u64, offset: u64) -> TypeResult<Box<dyn CacheFileHandler>> {
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

    pub fn lookup_item(
        &self,
        parent_inode: u64,
        file_name: &str,
        file_attr: &mut FileAttr,
    ) -> Option<u64> {
        todo!()
    }

    pub fn getattr_item(&self, inode: u64, file_attr: &mut FileAttr) -> Option<()> {
        todo!()
    }

    pub fn readdir(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        item: impl FnMut(u64, DirItem) -> bool,
    ) -> Option<()> {
        todo!()
    }
}
