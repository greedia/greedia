use crate::{
    cache::{Cache, HardCacheMetadata},
    cache_reader::CacheType,
    config::{DownloadAmount, SmartCacherConfig},
    downloader::{Downloader, ReturnWhen, ToDownload},
    downloader_inner::CacheHandle,
};
use anyhow::Result;
use async_trait::async_trait;
use smart_cacher::{FileSpec, ScErr, ScOk, ScResult, SmartCacher};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io::SeekFrom;
use std::collections::BTreeSet;

mod smart_cacher;

mod sc_flac;
mod sc_mp3;
mod sc_ogg;

/*
HARD_CACHE RULES
----------------
If file is below min_size, cache the whole thing. If not, continue.
If file has a known extension as specified in SPEC.exts, attempt to SmartCacher.detect it. If it returns None, continue.
Iterate through the rest of the enabled SmartCacher.detects. If all of them return None, continue.
Attempt to arbitrarily download some percentage of the file's start and end.
*/

static SMART_CACHERS: &[&dyn SmartCacher] = &[&sc_mp3::ScMp3];
//static SMART_CACHERS: &[&dyn SmartCacher] = &[];

#[derive(Clone)]
pub struct HardCacheItem {
    id: String,
    file_name: String,
    md5: Vec<u8>,
    size: u64,
}

pub struct HardCacher {
    inner: Arc<HardCacherInner>,
}

impl HardCacher {
    /// Create a new HardCacher for production use.
    pub fn new(cache: Arc<Cache>, downloader: Arc<Downloader>) -> HardCacher {
        todo!()
    }

    /// Process one file to hard cache.
    pub fn process(&self, id: &str, file_name: &str, md5: &[u8], size: u64) {
        todo!()
    }

    /// Create a new HardCacher for sctest use.
    #[cfg(feature = "sctest")]
    pub fn new_sctest(
        input: PathBuf,
        output: PathBuf,
        seconds: u64,
        fill_byte: Option<String>,
        fill_random: bool,
    ) -> HardCacher {
        let cacher = Arc::new(HcTestCacher {
            input,
            output,
            seconds,
            fill_byte,
            fill_random,
        });

        let inner = Arc::new(HardCacherInner::new(cacher));
        HardCacher { inner }
    }

    /// Process the provided sctest file.
    #[cfg(feature = "sctest")]
    pub async fn process_sctest(&self) {
        let inner = self.inner.clone();
        inner.process_sctest().await;
    }
}

struct HardCacherInner {
    cacher: Arc<dyn HcCacher + Send + Sync>,
    cachers_by_name: HashMap<&'static str, &'static dyn SmartCacher>,
    max_header_bytes: u64,
}

impl HardCacherInner {
    pub fn new(cacher: Arc<dyn HcCacher + Send + Sync>) -> HardCacherInner {
        let mut max_header_bytes = 0;
        let mut cachers_by_name = HashMap::new();
        let mut cachers_by_ext = HashMap::new();

        for sc in SMART_CACHERS {
            let spec = sc.spec();
            max_header_bytes = max(max_header_bytes, spec.header_bytes);
            cachers_by_name.insert(spec.name, *sc);
            // TODO support multiple scs per ext
            for ext in spec.exts {
                cachers_by_ext.insert(*ext, *sc);
            }
        }

        HardCacherInner {
            cacher,
            cachers_by_name,
            max_header_bytes,
        }
    }

    #[cfg(feature = "sctest")]
    pub async fn process_sctest(self: Arc<Self>) {
        println!("SCTEST");

        // Create fake item for process_partial_cache
        let item = HardCacheItem {
            id: String::new(),
            file_name: String::new(),
            md5: Vec::new(),
            size: 0,
        };

        self.process_partial_cache(&item).await;
    }

    pub async fn process_partial_cache(&self, item: &HardCacheItem) {
        let cache_item = self.cacher.get_item(item).await;
        let mut hcd = HardCacheDownloader::new(cache_item).await;

        let md5 = hex::encode(&item.md5);
        let file_spec = hard_cache_item_to_file_spec(item);

        // TODO: not hardcode this
        let smart_cacher_config = SmartCacherConfig {
            enabled: true,
            seconds: 10,
        };

        let header_data = hcd
            .read_data(0, self.max_header_bytes)
            .await
            .to_vec();

        for cacher in SMART_CACHERS {
            let spec = cacher.spec();
            let res: ScResult = cacher
                .cache(
                    &header_data[..spec.header_bytes as usize],
                    &smart_cacher_config,
                    &file_spec,
                    &mut hcd,
                )
                .await;

            match res {
                Ok(ScOk::Finalize) => {
                    // Write metadata, save here
                    hcd.save().await;
                    hcd.set_metadata(
                            HardCacheMetadata {
                                cacher: spec.name.to_string(),
                                version: spec.version,
                            },
                        )
                        .await
                        .unwrap();
                }
                Err(ScErr::Cancel) => {
                    hcd.save().await;
                    continue;
                }
                Err(ScErr::CancelWithMove) => {
                    hcd.save().await;
                    hcd.cancel_with_move().await.unwrap();
                    continue;
                }
                Err(ScErr::Trash) => {
                    hcd.close().await;
                    hcd.trash().await.unwrap();
                    continue;
                }
            }

            if let Ok(ScOk::Finalize) = res {
                // Write metadata, save here
                hcd.save().await;
                hcd.set_metadata(
                        HardCacheMetadata {
                            cacher: spec.name.to_string(),
                            version: spec.version,
                        },
                    )
                    .await
                    .unwrap();
                return;
            }
        }

        // If we reach here, try a generic start-end downloader.
        let (start_dl, end_dl) = &self.cacher.generic_cache_sizes();

        // Signal to cache the start data.
        let start_data_size = start_dl.with_size(item.size);
        hcd.cache_data(0, start_data_size);

        // Signal to cache the end data.
        let end_data_size = end_dl.with_size(item.size);
        hcd.cache_data(item.size - end_data_size, end_data_size);

        // Download and save data to disk.
        hcd.save().await;

        // Specify that we have successfully cached with generic_cacher.
        hcd.set_metadata(
                HardCacheMetadata {
                    cacher: "generic_cacher".to_string(),
                    version: 0,
                },
            )
            .await
            .unwrap();
    }
}

/// Trait to allow data from an arbitrary input to an arbitrary output.
#[async_trait]
trait HcCacher {
    async fn get_item(&self, item: &HardCacheItem) -> Box<dyn HcCacherItem + Send + Sync>;
    fn generic_cache_sizes(&self) -> (DownloadAmount, DownloadAmount);
}

#[async_trait]
trait HcCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8>;
    async fn read_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>) -> Vec<u8>;
    fn cache_data(&mut self, offset: u64, size: u64);
    fn cache_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>);
    fn cache_data_to(&mut self, offset: u64);

    async fn set_metadata(&mut self, meta: HardCacheMetadata) -> Result<()>;
    async fn cancel_with_move(&mut self) -> Result<()>; // demote_to_soft_cache
    async fn trash(&mut self) -> Result<()>; // clear_hard_cache
    async fn save(&mut self);
}

struct HcDownloadCacher {}
//impl HcCacher for HcDownloadCacher {} // TODO implement regular Download Cacher

#[cfg(feature = "sctest")]
struct HcTestCacher {
    input: PathBuf,
    output: PathBuf,
    seconds: u64,
    fill_byte: Option<String>,
    fill_random: bool,
}

#[cfg(feature = "sctest")]
#[async_trait]
impl HcCacher for HcTestCacher {
    async fn get_item(&self, item: &HardCacheItem) -> Box<dyn HcCacherItem + Send + Sync> {
        if !item.id.is_empty() {
            panic!("BUG: sctest id is not empty.");
        }
        if !item.md5.is_empty() {
            panic!("BUG: sctest md5 is not empty.");
        }
        // TODO set file_name and size from item

        let input = File::open(&self.input).await.unwrap();
        let output = File::create(&self.output).await.unwrap();
        Box::new(HcTestCacherItem {
            input,
            output,
            bridge_points: BTreeSet::new(),
            ranges_to_cache: BTreeMap::new(),
            bytes_cached: 0,
            bytes_read: 0,
        })
    }
    fn generic_cache_sizes(&self) -> (DownloadAmount, DownloadAmount) {
        let start = DownloadAmount {
            percent: Some(0.5),
            bytes: Some(1_000),
        };
        let end = DownloadAmount {
            percent: Some(0.5),
            bytes: Some(1_000),
        };

        (start, end)
    }
}

#[cfg(feature = "sctest")]
struct HcTestCacherItem {
    input: File,
    output: File,
    bridge_points: BTreeSet<u64>,
    ranges_to_cache: BTreeMap<u64, u64>,
    bytes_read: u64,
    bytes_cached: u64,
}

#[cfg(feature = "sctest")]
#[async_trait]
impl HcCacherItem for HcTestCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8> {
        println!("read_data {} {}", offset, size);
        self.bytes_read += size;
        self.bridge_points.insert(offset+size);
        self.input.seek(SeekFrom::Start(offset)).await.unwrap();
        let mut buf = vec![0u8; size as usize];
        self.input.read_exact(&mut buf).await.unwrap();
        buf
    }
    async fn read_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>) -> Vec<u8> {
        println!("read_data_bridged {} {} {:?}", offset, size, max_bridge_len);
        let last_bridge_point = if let Some(last_bridge_point) = self.bridge_points.range(..offset).rev().next() {
            *last_bridge_point
        } else {
            0
        };

        if let Some(max_bridge_len) = max_bridge_len {
            if offset - last_bridge_point <= max_bridge_len {
                self.ranges_to_cache.insert(last_bridge_point, offset - last_bridge_point);
            }
        } else {
            self.ranges_to_cache.insert(last_bridge_point, offset - last_bridge_point);
        }

        self.read_data(offset, size).await
    }
    fn cache_data(&mut self, offset: u64, size: u64) {
        println!("cache_data {} {}", offset, size);
        self.ranges_to_cache.insert(offset, size);
    }
    fn cache_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>) {
        println!("cache_data_bridged {} {} {:?}", offset, size, max_bridge_len);
        todo!()
    }
    fn cache_data_to(&mut self, offset: u64) {
        todo!()
    }
    async fn set_metadata(&mut self, meta: HardCacheMetadata) -> Result<()> {
        todo!()
    }
    async fn cancel_with_move(&mut self) -> Result<()> {
        todo!()
    }
    async fn trash(&mut self) -> Result<()> {
        todo!()
    }

    async fn save(&mut self) {
        self.output.flush();
        dbg!(&self.ranges_to_cache);
        todo!()
    }

}


pub struct HardCacheDownloader {
    item: Box<dyn HcCacherItem + Send + Sync>,
}

impl HardCacheDownloader {
    async fn new(
        item: Box<dyn HcCacherItem + Send + Sync>,
    ) -> HardCacheDownloader {
        // TODO: check hard/soft caches for existing data

        HardCacheDownloader {
            item,
        }
    }

    /// Download `size` bytes of data from `offset` and return it here.
    /// Any data that is read will also be cached.
    pub async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8>{
        if size == 0 {
            return vec![];
        }

        self.item.read_data(offset, size).await
    }

    /// Download `size` bytes of data from `offset` and return it here, bridging from a previous call.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous-offset call (or unlimited if `max_bridge_len` is None).
    pub async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Vec<u8> {
        self.item.read_data_bridged(offset, size, max_bridge_len).await
    }

    /// Download `size` bytes of data from `offset`, but do not return it here.
    pub fn cache_data(&mut self, offset: u64, size: u64) {
        self.item.cache_data(offset, size)
    }

    /// Download all data from the beginning of the file up to `offset`, but do not return it here.
    pub fn cache_data_to(&mut self, offset: u64) {
        self.item.cache_data_to(offset)
    }

    /// Download `size` bytes of data from `offset`, bridging from a previous call, but do not return it here.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous call (or unlimited if `max_bridge_len` is None).
    pub fn cache_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) {
        self.item.cache_data_bridged(offset, size, max_bridge_len)
    }

    async fn cache_fully(&mut self) {
        todo!()
    }

    /// Save all uncached data to disk.
    async fn save(&mut self) {
        self.item.save().await
    }

    /// Close any downloaded streams, and leave existing data as-is.
    /// cache.clear_hard_cache and cache.demote_to_soft_cache can be used for further processing.
    async fn close(&mut self) {
        todo!()
    }

    async fn set_metadata(&mut self, meta: HardCacheMetadata) -> Result<()> {
        self.item.set_metadata(meta).await
    }

    async fn cancel_with_move(&mut self) -> Result<()> {
        self.item.cancel_with_move().await
    }

    async fn trash(&mut self) -> Result<()> {
        self.item.trash().await
    }
}

fn file_name_ext(file_name: &str) -> Option<&str> {
    if !file_name.contains(".") {
        return None;
    }

    file_name.rsplit_terminator(".").next()
}

fn hard_cache_item_to_file_spec(item: &HardCacheItem) -> FileSpec {
    FileSpec {
        name: item.file_name.clone(),
        size: item.size,
    }
}























// ------------------------------------------------------------------------------

/*
/// Structure that manages adding new files to the hard cache.
pub struct HardCacherOld {
    downloader: Arc<Downloader>,
    inner: Arc<HardCacherInnerOld>,
}

impl HardCacherOld {
    /// Create a new HardCacher for production use.
    pub fn new(cache: Arc<Cache>, downloader: Arc<Downloader>) -> HardCacherOld {
        let inner = Arc::new(HardCacherInnerOld::new(cache));
        HardCacherOld { downloader, inner }
    }

    /// Process one file to hard cache.
    pub fn process(&self, id: &str, file_name: &str, md5: &[u8], size: u64) {
        let downloader = self.downloader.clone();
        let inner = self.inner.clone();

        let id = id.to_string();
        let file_name = file_name.to_string();
        let md5 = md5.to_vec();
        let item = HardCacheItem {
            id,
            file_name,
            md5,
            size,
        };
        tokio::spawn(inner.process(downloader, item));
    }
}

pub struct HardCacherInnerOld {
    cache: Arc<Cache>,
    cachers_by_name: HashMap<&'static str, &'static dyn SmartCacher>,
    max_header_bytes: u64,
}

impl HardCacherInnerOld {
    pub fn new(cache: Arc<Cache>) -> HardCacherInnerOld {
        let mut max_header_bytes = 0;
        let mut cachers_by_name = HashMap::new();
        let mut cachers_by_ext = HashMap::new();

        for sc in SMART_CACHERS {
            let spec = sc.spec();
            max_header_bytes = max(max_header_bytes, spec.header_bytes);
            cachers_by_name.insert(spec.name, *sc);
            for ext in spec.exts {
                cachers_by_ext.insert(*ext, *sc);
            }
        }
        HardCacherInnerOld {
            cache,
            cachers_by_name,
            max_header_bytes,
        }
    }

    pub fn is_latest(&self, meta: HardCacheMetadata) -> bool {
        if let Some(sc) = self.cachers_by_name.get(&meta.cacher[..]) {
            meta.version >= sc.spec().version
        } else {
            false
        }
    }

    pub async fn process(self: Arc<Self>, downloader: Arc<Downloader>, item: HardCacheItem) {
        let md5 = hex::encode(&item.md5);
        println!(
            "Processing {} ({}), size {}",
            item.file_name, md5, item.size
        );
        // If below min_size, call min_size cacher.
        if item.size <= self.cache.hard_cache_min_size {
            self.process_full_cache(downloader, &item).await;
            return;
        } else {
            // Grab the metadata file and check if finalized.
            let is_finalized = self
                .cache
                .get_hard_cache_metadata(&md5)
                .map(|meta| self.is_latest(meta))
                .unwrap_or(false);

            // If not finalized, try to cache the file partially.
            if !is_finalized {
                self.process_partial_cache(downloader, &item, &md5).await;
            }
        }
    }

    /// Download a file in its entirety.
    pub async fn process_full_cache(&self, downloader: Arc<Downloader>, item: &HardCacheItem) {
        let mut hcd = HardCacheDownloaderOld::new(self.cache.clone(), downloader, item).await;
        hcd.cache_fully().await;
    }

    /// Download a file partially.
    pub async fn process_partial_cache(
        &self,
        downloader: Arc<Downloader>,
        item: &HardCacheItem,
        md5: &str,
    ) {
        let mut hcd = HardCacheDownloaderOld::new(self.cache.clone(), downloader, item).await;

        // TODO: not hardcode this
        let smart_cacher_config = SmartCacherConfig {
            enabled: true,
            seconds: 10,
        };

        let file_spec = hard_cache_item_to_file_spec(item);

        // TODO: handle prioritized cachers based on file extension.

        let header_data = hcd
            .read_data(0, self.max_header_bytes as usize)
            .await
            .to_vec();
        for cacher in SMART_CACHERS {
            let spec = cacher.spec();
            let res: ScResult = cacher
                .cache(
                    &header_data[..spec.header_bytes as usize],
                    &smart_cacher_config,
                    &file_spec,
                    &mut hcd,
                )
                .await;

            match res {
                Ok(ScOk::Finalize) => {
                    // Write metadata, save here
                    hcd.save().await;
                    self.cache
                        .set_hard_cache_metadata(
                            &md5,
                            HardCacheMetadata {
                                cacher: spec.name.to_string(),
                                version: spec.version,
                            },
                        )
                        .await
                        .unwrap();
                }
                Err(ScErr::Cancel) => {
                    hcd.save().await;
                    continue;
                }
                Err(ScErr::CancelWithMove) => {
                    hcd.save().await;
                    self.cache.demote_to_soft_cache(&md5).await.unwrap();
                    continue;
                }
                Err(ScErr::Trash) => {
                    hcd.close().await;
                    self.cache.clear_hard_cache(md5).await.unwrap();
                    continue;
                }
            }

            if let Ok(ScOk::Finalize) = res {
                // Write metadata, save here
                hcd.save().await;
                self.cache
                    .set_hard_cache_metadata(
                        &md5,
                        HardCacheMetadata {
                            cacher: spec.name.to_string(),
                            version: spec.version,
                        },
                    )
                    .await
                    .unwrap();
                return;
            }
        }

        // If we reach here, try a generic start-end downloader.
        let (start_dl, end_dl) = &self.cache.generic_cache_sizes;

        // Signal to cache the start data.
        let start_data_size = start_dl.with_size(item.size);
        dbg!(start_dl, start_data_size);
        hcd.cache_data(0, start_data_size as usize);

        // Signal to cache the end data.
        let end_data_size = end_dl.with_size(item.size);
        hcd.cache_data((item.size - end_data_size) as usize, end_data_size as usize);

        // Download and save data to disk.
        hcd.save().await;

        // Specify that we have successfully cached with generic_cacher.
        self.cache
            .set_hard_cache_metadata(
                &md5,
                HardCacheMetadata {
                    cacher: "generic_cacher".to_string(),
                    version: 0,
                },
            )
            .await
            .unwrap();
    }
}

pub struct HardCacheDownloaderOld {
    cache: Arc<Cache>,
    downloader: Arc<Downloader>,
    item: HardCacheItem,
    md5: String,

    // TODO: create handle to downloaders
    open_streams: BTreeMap<u64, CacheHandle>,
    //
    ranges_to_cache: BTreeMap<u64, u64>,
}

// TODO: pass crypt hints to DriveCaches to be picked up here

impl HardCacheDownloaderOld {
    async fn new(
        cache: Arc<Cache>,
        downloader: Arc<Downloader>,
        item: &HardCacheItem,
    ) -> HardCacheDownloaderOld {
        let md5 = hex::encode(&item.md5);
        let item = item.clone();

        let open_streams = BTreeMap::new();
        let ranges_to_cache = BTreeMap::new();

        // TODO: check hard/soft caches for existing data
        let hard_cache = cache.get_cache_files(CacheType::HardCache, &md5).await;

        HardCacheDownloaderOld {
            cache,
            downloader,
            item,
            md5,

            open_streams,
            ranges_to_cache,
        }
    }

    async fn cache_fully(&mut self) {
        let hard_cache = self
            .cache
            .get_cache_files(CacheType::HardCache, &self.md5)
            .await;
        if hard_cache.contains_range(0, self.item.size) {
            return; // We've already fully cached this file
        } else {
            self.cache_data(0, self.item.size as usize);
            self.save().await;
        }
    }

    /// Consolidate ranges_to_cache.
    fn consolidate_ranges(&mut self) {
        // TODO
    }

    /// Save all uncached data to disk.
    async fn save(&mut self) {
        self.consolidate_ranges();
        dbg!(&self.ranges_to_cache);
        for (offset, size) in &self.ranges_to_cache {
            let range = ToDownload::Range(*offset, *offset + *size);
            // TODO: check for gaps in hard_cache byteranger
            // KTODO THIS NEXT
            /*self.downloader
            .cache_data(self.item.id, path, ReturnWhen::Finished, range)
            .await;*/
        }
        todo!("Implement save function")
    }

    /// Close any downloaded streams, and leave existing data as-is.
    /// cache.clear_hard_cache and cache.demote_to_soft_cache can be used for further processing.
    async fn close(&mut self) {
        // TODO: handle downloader streams
        todo!()
    }

    /// Download `size` bytes of data from `offset` and return it here.
    /// Any data that is read will also be cached.
    pub async fn read_data<'b>(&'b mut self, offset: usize, size: usize) -> &'b [u8] {
        if size == 0 {
            return &[];
        }
        println!("read_data {} {}", offset, size);
        todo!()
    }

    /// Download `size` bytes of data from `offset` and return it here, bridging from a previous call.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous-offset call (or unlimited if `max_bridge_len` is None).
    pub async fn read_data_bridged<'b>(
        &'b mut self,
        offset: usize,
        size: usize,
        max_bridge_len: Option<usize>,
    ) -> &'b [u8] {
        todo!()
    }

    /// Download `size` bytes of data from `offset`, but do not return it here.
    pub fn cache_data(&mut self, offset: usize, size: usize) {
        let offset = offset as u64;
        let size = size as u64;
        // Only add to ranges_to_cache.
        // Allow overlapping ranges - these will be consolidated later.
        if let Some(rtc_size) = self.ranges_to_cache.get_mut(&(offset as u64)) {
            let max_size = max(*rtc_size, size);
            *rtc_size = max_size;
        } else {
            self.ranges_to_cache.insert(offset, size);
        }
    }

    /// Download all data from the beginning of the file up to `offset`, but do not return it here.
    pub fn cache_data_to(&mut self, offset: usize) {
        let offset = offset as u64;
        if let Some(rtc_size) = self.ranges_to_cache.get_mut(&0) {
            let max_size = max(*rtc_size, offset);
            *rtc_size = max_size;
        } else {
            self.ranges_to_cache.insert(offset, offset);
        }
        todo!()
    }

    /// Download `size` bytes of data from `offset`, bridging from a previous call, but do not return it here.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous call (or unlimited if `max_bridge_len` is None).
    pub fn cache_data_bridged(
        &mut self,
        offset: usize,
        size: usize,
        max_bridge_len: Option<usize>,
    ) {
        todo!()
    }
}
*/
