use crate::{
    cache::{Cache, HardCacheMetadata},
    cache_reader::CacheType,
    config::SmartCacherConfig,
    downloader::{Downloader, ReturnWhen, ToDownload}, downloader_inner::CacheHandle,
};
use smart_cacher::{FileSpec, ScErr, ScOk, ScResult, SmartCacher};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

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

//static SMART_CACHERS: &[&dyn SmartCacher] = &[&sc_mp3::ScMp3, &sc_flac::ScFlac];
static SMART_CACHERS: &[&dyn SmartCacher] = &[];

#[derive(Clone)]
pub struct HardCacheItem {
    id: String,
    file_name: String,
    md5: Vec<u8>,
    size: u64,
}

/// Structure that manages adding new files to the hard cache.
pub struct HardCacher {
    downloader: Arc<Downloader>,
    inner: Arc<HardCacherInner>,
}

impl HardCacher {
    pub fn new(cache: Arc<Cache>, downloader: Arc<Downloader>) -> HardCacher {
        let inner = Arc::new(HardCacherInner::new(cache));
        HardCacher { downloader, inner }
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

pub struct HardCacherInner {
    cache: Arc<Cache>,
    cachers_by_name: HashMap<&'static str, &'static dyn SmartCacher>,
    max_header_bytes: u64,
}

impl HardCacherInner {
    pub fn new(cache: Arc<Cache>) -> HardCacherInner {
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
        HardCacherInner {
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
        let mut hcd = HardCacheDownloader::new(self.cache.clone(), downloader, item).await;
        hcd.cache_fully().await;
    }

    /// Download a file partially.
    pub async fn process_partial_cache(
        &self,
        downloader: Arc<Downloader>,
        item: &HardCacheItem,
        md5: &str,
    ) {
        let mut hcd = HardCacheDownloader::new(self.cache.clone(), downloader, item).await;

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

pub struct HardCacheDownloader {
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

impl HardCacheDownloader {
    async fn new(
        cache: Arc<Cache>,
        downloader: Arc<Downloader>,
        item: &HardCacheItem,
    ) -> HardCacheDownloader {
        let md5 = hex::encode(&item.md5);
        let item = item.clone();

        let open_streams = BTreeMap::new();
        let ranges_to_cache = BTreeMap::new();

        // TODO: check hard/soft caches for existing data
        let hard_cache = cache.get_cache_files(CacheType::HardCache, &md5).await;

        HardCacheDownloader {
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
