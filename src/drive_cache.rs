use crate::{
    config::ConfigDrive, crypt_context::CryptContext, downloader::Downloader,
    soft_cache_lru::SoftCacheLru,
};
use anyhow::Result;
use rocksdb::DB;
use std::sync::Arc;

#[derive(Clone)]
pub struct DriveCache {
    pub name: String,
    pub id: String,
    downloader: Arc<Downloader>,
    db: Arc<DB>,
    sclru: Arc<SoftCacheLru>,
    crypt_context: Option<CryptContext>,
}

impl DriveCache {
    pub fn new(
        name: String,
        downloader: Arc<Downloader>,
        db: Arc<DB>,
        sclru: Arc<SoftCacheLru>,
        drive: &ConfigDrive,
    ) -> Result<DriveCache> {
        let id = drive.drive_id.clone();

        let crypt_context =
            if let (Some(password1), Some(password2)) = (&drive.password, &drive.password2) {
                Some(CryptContext::new(&password1, &password2)?)
            } else {
                None
            };

        Ok(DriveCache {
            name,
            id,
            downloader,
            db,
            sclru,
            crypt_context,
        })
    }
}
