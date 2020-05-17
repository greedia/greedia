use crate::{
    config::{ConfigDrive, DownloadAmount, SoftCache},
    downloader::Downloader,
    drive_access::DriveAccess,
    drive_cache::DriveCache,
    soft_cache_lru::SoftCacheLru,
    types::CfKeys,
};
use anyhow::Result;
use rocksdb::{Options, DB};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct Cache {}

impl Cache {
    pub async fn new(
        cache_path: &str,
        soft_cache: &SoftCache,
        head_dl: DownloadAmount,
        tail_dl: DownloadAmount,
        drives: HashMap<String, ConfigDrive>,
    ) -> Result<(Vec<Arc<DriveCache>>, Vec<DriveAccess>)> {
        // Prepare column families to be used in RocksDB
        let drive_ids: HashSet<String> = drives
            .iter()
            .map(|(_, drive)| drive.drive_id.clone())
            .collect();
        let column_families: Vec<String> = drive_ids
            .into_iter()
            .flat_map(|drive_id| CfKeys::as_vec(&drive_id))
            .collect();

        // Open the database with the column families
        let cache_path = PathBuf::from(cache_path);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = Arc::new(DB::open_cf(
            &options,
            cache_path.join("db_v1"),
            column_families,
        )?);

        let sclru = Arc::new(Mutex::new(SoftCacheLru::new(
            &cache_path.join("soft_cache"),
            soft_cache.limit,
        )));

        // Create Downloaders that will be deduplicated by client_id (for the sake of rate limiting)
        // Create DriveCaches that will be deduplicated by drive_id (to ensure no inode conflicts)
        // Then return a list of DriveAccessors that map to the [drive] sections in the config
        let mut downloaders: HashMap<String, Arc<Downloader>> = HashMap::new();
        let mut drive_caches: HashMap<String, Arc<DriveCache>> = HashMap::new();
        let mut drive_accessors = Vec::with_capacity(drives.len());
        for (name, drive) in drives {
            let downloader = if let Some(downloader) = downloaders.get(&drive.client_id) {
                downloader.clone()
            } else {
                let new_downloader = Arc::new(
                    Downloader::new(
                        drive.client_id.clone(),
                        drive.client_secret.clone(),
                        drive.refresh_token.clone(),
                    )
                    .await?,
                );
                downloaders.insert(drive.client_id.clone(), new_downloader.clone());
                new_downloader
            };
            let drive_cache = if let Some(drive_cache) = drive_caches.get(&drive.drive_id) {
                drive_cache.clone()
            } else {
                let db = db.clone();
                let new_drive_cache = Arc::new(DriveCache::new(
                    &name,
                    &drive.drive_id,
                    downloader,
                    db,
                    sclru.clone(),
                )?);
                drive_caches.insert(drive.drive_id.clone(), new_drive_cache.clone());
                new_drive_cache
            };

            drive_accessors.push(DriveAccess::new(name, drive, drive_cache)?);
        }
        let drive_caches = drive_caches.iter().map(|(_, v)| v.clone()).collect();
        Ok((drive_caches, drive_accessors))
    }
}
