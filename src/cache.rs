use crate::{
    cache_reader::CacheType,
    config::{Config, DownloadAmount},
    downloader::Downloader,
    drive_access::DriveAccess,
    drive_cache::DriveCache,
    soft_cache_lru::SoftCacheLru,
    types::CfKeys,
};
use anyhow::Result;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::{
    cmp,
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
    md5: String,
}

impl CacheFiles {
    pub fn new(md5: &str, cache_type: CacheType, cache_files: BTreeMap<u64, u64>) -> CacheFiles {
        CacheFiles {
            md5: md5.to_string(),
            cache_type,
            cache_files,
        }
    }

    pub fn contains_range(&self, offset: u64, size: u64) -> bool {
        let expected_start = offset;
        let expected_end = offset + size;

        let mut contig_start = 0u64;
        let mut contig_end = 0u64;

        for (&file, &len) in self.cache_files.range(..=expected_end).rev() {
            let start = file;
            let end = start + len;

            // Initial values.
            if contig_start == 0 && contig_end == 0 {
                contig_start = start;
                contig_end = end;
            }
            // The cover can be extended if the start is in the
            // current contiguous section.
            else if (start >= contig_start && start <= contig_end)
                || (contig_start >= start && contig_start <= end)
            {
                contig_start = cmp::min(contig_start, start);
                contig_end = cmp::max(contig_end, end);
            }
            // The cover can be extended if either end is in the
            // current contiguous section.
            else if (end >= contig_start && end <= contig_end)
                || (contig_end >= start && contig_end <= end)
            {
                contig_start = cmp::min(contig_start, start);
                contig_end = cmp::max(contig_end, end);
            }

            // full cover
            if expected_start >= contig_start && expected_end <= contig_end {
                return true;
            }
        }

        false
    }
}

pub struct ReaderFileData {
    pub file_id: String,
    pub md5: String,
    pub size: u64,
    pub info_inode: u64,
}

/// JSON data stored with each hard-cached file.
#[derive(Serialize, Deserialize, Debug)]
pub struct HardCacheMetadata {
    /// Name of the (smart) cacher used to cache this file.
    pub cacher: String,
    /// The version of (smart) cacher used.
    pub version: u64,
}

pub struct Cache {
    sclru: Arc<Mutex<SoftCacheLru>>,
    hard_cache_path: PathBuf,
    soft_cache_path: PathBuf,
    pub hard_cache_min_size: u64,
    pub generic_cache_sizes: (DownloadAmount, DownloadAmount),
}

impl Cache {
    pub async fn new(cfg: &Config) -> Result<(Vec<Arc<DriveCache>>, Vec<DriveAccess>)> {
        let db_path = PathBuf::from(&cfg.caching.cache_path);
        let sclru = Arc::new(Mutex::new(SoftCacheLru::new(
            &db_path.join("soft_cache"),
            cfg.caching.soft_cache_limit,
        )));

        let hard_cache_path = db_path.join("hard_cache");
        let soft_cache_path = db_path.join("soft_cache");
        let hard_cache_min_size = cfg.caching.min_size;
        let generic_cache_sizes = (
            cfg.generic_cacher.start.clone(),
            cfg.generic_cacher.end.clone(),
        );

        let cache = Arc::new(Cache {
            sclru,
            hard_cache_path,
            soft_cache_path,
            hard_cache_min_size,
            generic_cache_sizes,
        });

        // Prepare column families to be used in RocksDB
        let drive_ids: HashSet<String> = cfg
            .gdrive
            .iter()
            .map(|(_, drive)| drive.drive_id.clone())
            .collect();
        let mut column_families: Vec<String> = drive_ids
            .into_iter()
            .flat_map(|drive_id| CfKeys::as_vec(&drive_id))
            .collect();

        // Open the database with the column families
        let db_path = db_path.join("db_v1");

        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut existing_column_families = DB::list_cf(&options, &db_path).unwrap_or(vec![]);
        column_families.append(&mut existing_column_families);

        let db = Arc::new(DB::open_cf(&options, &db_path, column_families)?);

        // Create Downloaders that will be deduplicated by client_id (for the sake of rate limiting)
        // Create DriveCaches that will be deduplicated by drive_id (to ensure no inode conflicts)
        // Then return a list of DriveAccessors that map to the [drive] sections in the config
        let mut downloaders: HashMap<String, Arc<Downloader>> = HashMap::new();
        let mut drive_caches: HashMap<String, Arc<DriveCache>> = HashMap::new();
        let mut drive_accessors = Vec::with_capacity(cfg.gdrive.len());
        for (name, drive) in &cfg.gdrive {
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

            drive_accessors.push(DriveAccess::new(
                name.to_string(),
                drive.clone(),
                drive_cache,
            )?);
        }
        let drive_caches = drive_caches.iter().map(|(_, v)| v.clone()).collect();
        Ok((drive_caches, drive_accessors))
    }

    pub fn get_cache_paths(&self, md5: &str) -> (PathBuf, PathBuf) {
        let hard_cache_path = self.get_cache_dir_path(&CacheType::HardCache, md5);
        let soft_cache_path = self.get_cache_dir_path(&CacheType::SoftCache, md5);
        (hard_cache_path, soft_cache_path)
    }

    pub async fn get_cache_files(&self, cache_type: CacheType, md5: &str) -> CacheFiles {
        let path = self.get_cache_dir_path(&cache_type, md5);

        // Tokio doesn't have great Stream support right now, so this is a bit of a mess.
        let cache = if path.exists() {
            let mut dirs = read_dir(path).await.unwrap();
            let mut btm = BTreeMap::new();
            while let Some(dir_entry) = dirs.next().await {
                if let Some((name, len)) = direntry_to_namelen(dir_entry.unwrap()).await {
                    btm.insert(name, len);
                }
            }
            btm
        } else {
            BTreeMap::new()
        };

        CacheFiles::new(md5, cache_type, cache)
    }

    pub async fn get_all_cache_files(&self, md5: &str) -> (CacheFiles, CacheFiles) {
        (
            self.get_cache_files(CacheType::HardCache, md5).await,
            self.get_cache_files(CacheType::SoftCache, md5).await,
        )
    }

    pub fn get_hard_cache_metadata(&self, md5: &str) -> Option<HardCacheMetadata> {
        let meta_path = self
            .get_cache_dir_path(&CacheType::HardCache, md5)
            .join("meta.json");
        let meta_file = std::fs::read(meta_path).ok()?;
        serde_json::from_slice(&meta_file).ok()
    }

    pub async fn set_hard_cache_metadata(&self, md5: &str, meta: HardCacheMetadata) -> Result<()> {
        let meta_data = serde_json::to_vec_pretty(&meta)?;
        let meta_path = self
            .get_cache_dir_path(&CacheType::HardCache, md5)
            .join("meta.json");
        let parent = meta_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(meta_path, meta_data)
            .await
            .map_err(|e| e.into())
    }

    pub fn get_cache_dir_path(&self, cache_type: &CacheType, md5: &str) -> PathBuf {
        let cache_path = match cache_type {
            CacheType::HardCache => &self.hard_cache_path,
            CacheType::SoftCache => &self.soft_cache_path,
        };
        let dir_1 = md5.get(0..2).unwrap();
        let dir_2 = md5.get(2..4).unwrap();
        cache_path.join(dir_1).join(dir_2).join(md5)
    }

    /// Move all hard_cache files to soft_cache.
    pub async fn demote_to_soft_cache(&self, md5: &str) -> Result<()> {
        todo!()
    }

    /// Remove all hard_cache files.
    pub async fn clear_hard_cache(&self, md5: &str) -> Result<()> {
        todo!()
    }

    pub fn add_soft_cache_file(&self, md5: &str, offset: u64, len: u64) {
        // TODO: Make SCLRU a background process
        // let mut soft_cache_lru = self.soft_cache_lru.lock();
        // soft_cache_lru.add(md5, offset, len);
    }

    pub fn touch_soft_cache_file(&self, md5: &str, offset: u64) {
        // TODO: Make SCLRU a background process
        //let mut soft_cache_lru = self.soft_cache_lru.lock();
        //soft_cache_lru.touch(md5, offset);
    }
}

async fn direntry_to_namelen(dir_entry: DirEntry) -> Option<(u64, u64)> {
    let file_name = dir_entry.file_name();
    let file_name = file_name.to_str()?;
    if file_name.starts_with("chunk_") {
        let (_, offset) = file_name.split_at(6);
        let offset = offset.parse().ok()?;
        let len = dir_entry.metadata().await.ok()?.len();
        Some((offset, len))
    } else {
        None
    }
}
