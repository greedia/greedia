use crate::{
    config::{ConfigDrive, DownloadAmount, SoftCache},
    downloader::Downloader,
    drive_access::DriveAccess,
    drive_cache::DriveCache,
    soft_cache_lru::SoftCacheLru,
    types::CfKeys, cache_reader::CacheType,
};
use anyhow::Result;
use rocksdb::{Options, DB};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    fs::{read_dir, DirEntry},
    stream::StreamExt,
    sync::Mutex,
};

#[derive(Debug)]
pub struct CacheFiles {
    pub cache_type: CacheType,
    pub cache_files: BTreeMap<u64, u64>,
}

impl CacheFiles {
    pub fn new(cache_type: CacheType, cache_files: BTreeMap<u64, u64>) -> CacheFiles {
        CacheFiles {
            cache_type, cache_files
        }
    }
}

pub struct ReaderFileData {
    pub file_id: String,
    pub md5: String,
    pub size: u64,
    pub info_inode: u64,
}

pub struct Cache {
    sclru: Arc<Mutex<SoftCacheLru>>,
    hard_cache_path: PathBuf,
    soft_cache_path: PathBuf,
    head_dl: DownloadAmount,
    tail_dl: DownloadAmount,
}

impl Cache {
    pub async fn new(
        db_path: &str,
        soft_cache: &SoftCache,
        head_dl: DownloadAmount,
        tail_dl: DownloadAmount,
        drives: HashMap<String, ConfigDrive>,
    ) -> Result<(Vec<Arc<DriveCache>>, Vec<DriveAccess>)> {
        let db_path = PathBuf::from(db_path);
        let sclru = Arc::new(Mutex::new(SoftCacheLru::new(
            &db_path.join("soft_cache"),
            soft_cache.limit,
        )));

        let cache = Arc::new(Cache {
            sclru,
            hard_cache_path: db_path.join("hard_cache"),
            soft_cache_path: db_path.join("soft_cache"),
            head_dl,
            tail_dl,
        });

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
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = Arc::new(DB::open_cf(
            &options,
            db_path.join("db_v1"),
            column_families,
        )?);

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
                let cache = cache.clone();
                let new_drive_cache = Arc::new(DriveCache::new(
                    &name,
                    &drive.drive_id,
                    downloader,
                    cache,
                    db,
                )?);
                drive_caches.insert(drive.drive_id.clone(), new_drive_cache.clone());
                new_drive_cache
            };

            drive_accessors.push(DriveAccess::new(name, drive, drive_cache)?);
        }
        let drive_caches = drive_caches.iter().map(|(_, v)| v.clone()).collect();
        Ok((drive_caches, drive_accessors))
    }

    pub async fn get_cache_files(&self, md5: &str) -> (CacheFiles, CacheFiles) {
        let mut chars = md5.chars();
        let char_1 = chars.next().unwrap().to_string();
        let char_2 = chars.next().unwrap().to_string();
        let char_3 = chars.next().unwrap().to_string();

        let hc_path = self
            .hard_cache_path
            .join(&char_1)
            .join(&char_2)
            .join(&char_3)
            .join(&md5);

        let sc_path = self
            .soft_cache_path
            .join(&char_1)
            .join(&char_2)
            .join(&char_3)
            .join(&md5);

        // Tokio doesn't have great Stream support right now, so this is a bit of a mess.
        let hard_cache = if hc_path.exists() {
            let mut dirs = read_dir(hc_path).await.unwrap();
            let mut btm = BTreeMap::new();
            while let Some(dir_entry) = dirs.next().await {
                let (name, len) = direntry_to_namelen(dir_entry.unwrap()).await.unwrap();
                btm.insert(name, len);
            }
            btm
        } else {
            BTreeMap::new()
        };

        let soft_cache = if sc_path.exists() {
            let mut dirs = read_dir(sc_path).await.unwrap();
            let mut btm = BTreeMap::new();
            while let Some(dir_entry) = dirs.next().await {
                let (name, len) = direntry_to_namelen(dir_entry.unwrap()).await.unwrap();
                btm.insert(name, len);
            }
            btm
        } else {
            BTreeMap::new()
        };

        (CacheFiles::new(CacheType::HardCache, hard_cache), CacheFiles::new(CacheType::SoftCache, soft_cache))
    }

    pub fn get_cache_dir_path(&self, cache_type: &CacheType, md5: &str) -> PathBuf {
        let cache_path = match cache_type {
            CacheType::HardCache => &self.hard_cache_path,
            CacheType::SoftCache => &self.soft_cache_path,
        };
        let mut chars = md5.chars();
        let char_1 = chars.next().unwrap().to_string();
        let char_2 = chars.next().unwrap().to_string();
        let char_3 = chars.next().unwrap().to_string();
        cache_path
            .join(&char_1)
            .join(&char_2)
            .join(&char_3)
            .join(md5)
    }

    pub fn add_soft_cache_file(&self, md5: &str, offset: u64, len: u64) {
        // TODO: handle SCLRU
        // let mut soft_cache_lru = self.soft_cache_lru.lock();
        // soft_cache_lru.add(md5, offset, len);
    }

    pub fn touch_soft_cache_file(&self, md5: &str, offset: u64) {
        // TODO: handle SCLRU
        //let mut soft_cache_lru = self.soft_cache_lru.lock();
        //soft_cache_lru.touch(md5, offset);
    }
}

async fn direntry_to_namelen(dir_entry: DirEntry) -> Option<(u64, u64)> {
    let file_name = dir_entry.file_name();
    let file_name = file_name.to_str().unwrap();
    let len = dir_entry.metadata().await.unwrap().len();
    if file_name.starts_with("chunk_") {
        let (_, offset) = file_name.split_at(6);
        let offset = offset.parse().unwrap();
        Some((offset, len))
    } else {
        None
    }
}