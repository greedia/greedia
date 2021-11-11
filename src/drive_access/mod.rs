use std::sync::Arc;

use self::{
    cache::{CacheDriveAccess, TypeResult},
    scratch::ScratchDriveAccess,
};
use crate::{
    cache_handlers::{CacheFileHandler, CacheHandlerError},
    db::types::{ArchivedDirItem, ArchivedDriveItem},
};

pub mod cache;
pub mod scratch;

/// A type that works for both cached and scratch drives.
/// It's unfortunate that a DriveAccess trait can't be used,
/// but due to getattr requiring a generic to_t closure, this
/// is the cleaner way to handle both in mount.rs.
pub enum GenericDrive {
    Drive(Arc<CacheDriveAccess>),
    Scratch(ScratchDriveAccess),
}

impl GenericDrive {
    pub fn name(&self) -> &str {
        match self {
            GenericDrive::Drive(c) => c.name.as_str(),
            GenericDrive::Scratch(s) => s.name.as_str(),
        }
    }

    pub fn getattr<T>(&self, inode: u64, to_t: impl FnOnce(&ArchivedDriveItem) -> T) -> Option<T> {
        match self {
            GenericDrive::Drive(d) => d.getattr(inode, to_t),
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub fn readdir(
        &self,
        inode: u64,
        offset: u64,
        for_each: impl FnMut(u64, &ArchivedDirItem, Option<&str>) -> bool,
    ) -> Option<()> {
        match self {
            GenericDrive::Drive(d) => d.readdir(inode, offset, for_each),
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub fn lookup<T>(
        &self,
        inode: u64,
        file_name: &str,
        to_t: impl FnOnce(u64, &ArchivedDriveItem) -> T,
    ) -> Option<T> {
        match self {
            GenericDrive::Drive(d) => d.lookup(inode, file_name, to_t),
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub async fn unlink(&self, parent: u64, file_name: &str) -> Option<()> {
        match self {
            GenericDrive::Drive(d) => d.unlink(parent, file_name).await,
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub async fn rename(
        &self,
        parent: u64,
        file_name: &str,
        new_parent: Option<u64>,
        new_name: Option<&str>,
    ) -> Option<()> {
        match self {
            GenericDrive::Drive(d) => d.rename(parent, file_name, new_parent, new_name).await,
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub fn check_dir(&self, inode: u64) -> Result<TypeResult<bool>, CacheHandlerError> {
        match self {
            GenericDrive::Drive(d) => d.check_dir(inode),
            GenericDrive::Scratch(_) => todo!(),
        }
    }

    pub async fn open_file(
        &self,
        inode: u64,
        offset: u64,
    ) -> Result<TypeResult<Box<dyn CacheFileHandler>>, CacheHandlerError> {
        match self {
            GenericDrive::Drive(d) => d.open_file(inode, offset).await,
            GenericDrive::Scratch(_) => todo!(),
        }
    }
}
