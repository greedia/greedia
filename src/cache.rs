use crate::{
    config::{ConfigDrive, DownloadAmount, SoftCache},
    downloader::Downloader,
    soft_cache_lru::SoftCacheLru, drive_cache::DriveCache, types::CfKeys,
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
    ) -> Result<Vec<DriveCache>> {
        // Prepare column families to be used in RocksDB
        let drive_ids: HashSet<String> = drives
            .iter()
            .map(|(_, drive)| drive.drive_id.clone())
            .collect();
        let column_families: Vec<String> = drive_ids
            .into_iter()
            .flat_map(|drive_id| {
                CfKeys::as_vec(&drive_id)
            })
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

        // Create downloaders that will be deduplicated by client_id (for the sake of rate limiting)
        // and create the DriveCaches
        let mut downloaders: HashMap<String, Arc<Downloader>> = HashMap::new();
        let mut drive_caches = Vec::with_capacity(drives.len());
        for (name, drive) in drives {
            let downloader = if let Some(downloader) = downloaders.get(&drive.client_id) {
                downloader.clone()
            } else {
                let new_downloader = Arc::new(Downloader::new(
                    drive.client_id.clone(),
                    drive.client_secret.clone(),
                    drive.refresh_token.clone(),
                ).await?);
                downloaders.insert(drive.client_id.clone(), new_downloader.clone());
                new_downloader
            };
            let db = db.clone();
            drive_caches.push(DriveCache::new(
                name,
                downloader,
                db,
                sclru.clone(),
                &drive,
            )?);
        }
        Ok(drive_caches)
    }
}
