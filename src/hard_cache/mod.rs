use async_trait::async_trait;
use futures::{Future, FutureExt};
use smart_cacher::{FileSpec, ScErr, ScOk, ScResult, SmartCacher};
use tokio::io::{AsyncRead, ReadBuf};
use std::{collections::{BTreeMap, HashMap}, ffi::OsStr, marker::PhantomData, path::Path, pin::Pin, sync::Arc, task::{Context, Poll}};

#[cfg(feature="sctest")]
use std::path::PathBuf;

use rkyv::{Archive, Deserialize, Serialize, de::deserializers::AllocDeserializer};

use crate::{cache_handlers::{CacheFileHandler, crypt_passthrough::CryptPassthrough}, config::{DownloadAmount, SmartCacherConfig}, drive_access2::DriveAccess, types::DataIdentifier};
use self::smart_cacher::SMART_CACHER_VERSION;

#[cfg(feature = "sctest")]
mod sctest;
mod smart_cacher;

mod sc_flac;
mod sc_mp3;
mod sc_ogg;
mod sc_mkv;

/*
HARD_CACHE RULES
----------------
If a file's hard cache metadata states the global hard cache version matches, don't touch the file. (TODO)
If file is below min_size, cache the whole thing. If not, continue.
If file has a known extension as specified in SPEC.exts, attempt to smart cache it. If failed, continue.
Iterate through the rest of the enabled smart cachers. If all of them fail, continue.
Attempt to arbitrarily download some percentage of the file's start and end.
*/

static SMART_CACHERS: &[&dyn SmartCacher] = &[&sc_mp3::ScMp3, &sc_mkv::ScMkv];

#[derive(Clone)]
pub struct HardCacheItem {
    access_id: String,
    file_name: String,
    data_id: DataIdentifier,
    size: u64,
}
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct HardCacheMetadata {
    /// Name of the (smart) cacher used to cache this file.
    pub cacher: String,
    /// The global smart cacher version used to cache this file. (TODO)
    pub version: u64,
    // The revision of the smart_cachers and generic_cacher sections of the config.
    // pub config_revision: u64
}

pub struct HardCacher {
    cacher: Arc<dyn HcCacher + Send + Sync>,
    cachers_by_ext: HashMap<&'static str, &'static dyn SmartCacher>,
    min_size: u64,
}

impl HardCacher {
    /// Create a new HardCacher for production use.
    pub fn new(drive_access: Arc<DriveAccess>, min_size: u64) -> HardCacher {
        let cacher = Arc::new(HcDownloadCacher { drive_access });

        Self::new_inner(cacher, min_size)
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
        let cacher = Arc::new(sctest::HcTestCacher {
            input,
            output,
            seconds,
            fill_byte,
            fill_random,
        });

        Self::new_inner(cacher, 0)
    }

    fn new_inner(cacher: Arc<dyn HcCacher + Send + Sync>, min_size: u64) -> HardCacher {
        let mut cachers_by_ext = HashMap::new();

        for sc in SMART_CACHERS {
            let spec = sc.spec();
            for ext in spec.exts {
                cachers_by_ext.insert(*ext, *sc);
            }
        }

        HardCacher {
            cacher,
            cachers_by_ext,
            min_size,
        }
    }

    /// Process one file to hard cache.
    pub async fn process(
        &self,
        access_id: &str,
        file_name: &str,
        data_id: &DataIdentifier,
        size: u64,
        meta: Option<&ArchivedHardCacheMetadata>,
    ) -> Option<HardCacheMetadata> {
        // KTODO: try filename decryption here
        
        let item = HardCacheItem {
            access_id: access_id.to_string(),
            file_name: file_name.to_string(),
            data_id: data_id.clone(),
            size,
        };

        self.process_inner(item, meta).await
    }

    /// Process the provided sctest file.
    #[cfg(feature = "sctest")]
    pub async fn process_sctest(&self, file_name: String, size: u64) {
        println!("SCTEST");

        // Create fake item for process_partial_cache
        let item = HardCacheItem {
            access_id: String::new(),
            file_name,
            data_id: DataIdentifier::None,
            size,
        };

        self.process_inner(item, None).await;
    }

    async fn process_inner(
        &self,
        item: HardCacheItem,
        meta: Option<&ArchivedHardCacheMetadata>,
    ) -> Option<HardCacheMetadata> {
        // Short-circuit if finalized with latest version of smart_cacher
        if let Some(ref meta) = meta {
            if self.is_latest(meta) {
                return Some(meta
                    .deserialize(&mut AllocDeserializer)
                    .unwrap());
            }
        }
        let file_spec = hard_cache_item_to_file_spec(&item);
        let item_size = item.size;

        let preferred_cacher = Path::new(&item.file_name).extension().and_then(OsStr::to_str).and_then(|ext| self.cachers_by_ext.get(ext));

        let cache_item = self.cacher.get_item(item).await;
        let mut hcd = HardCacheDownloader::new(cache_item).await;

        // If below min_size, call min_size cacher
        if item_size <= self.min_size {
            // println!("Using min_size cacher");
            hcd.cache_data_fully().await;
            hcd.save().await;
            return Some(HardCacheMetadata {
                cacher: "min_size_cacher".to_string(),
                version: 0,
            });
        }

        // TODO: not hardcode this
        let smart_cacher_config = SmartCacherConfig {
            enabled: true,
            seconds: 10,
        };

        if let Some(pc) = preferred_cacher {
            let spec = pc.spec();
            println!("Trying preferred cacher {}", spec.name);
            let res: ScResult = pc
                .cache(
                    &smart_cacher_config,
                    &file_spec,
                    &mut hcd,
                )
                .await;

            match res {
                Ok(ScOk::Finalize) => {
                    // Write metadata, save here
                    // println!("Success with smart cacher {}", spec.name);
                    hcd.save().await;
                    return Some(HardCacheMetadata {
                        cacher: spec.name.to_string(),
                        version: SMART_CACHER_VERSION,
                    });
                }
                Err(ScErr::Cancel) => {
                    // println!("Smart cacher {} failed, trying next...", spec.name);
                    hcd.save().await;
                }
            }
        }

        for cacher in SMART_CACHERS {
            let spec = cacher.spec();
            if let Some(pc) = preferred_cacher {
                let pc_spec = pc.spec();
                if spec.name == pc_spec.name {
                    continue;
                }
            }
            println!("Trying smart cacher {}", spec.name);
            let res: ScResult = cacher
                .cache(
                    &smart_cacher_config,
                    &file_spec,
                    &mut hcd,
                )
                .await;

            match res {
                Ok(ScOk::Finalize) => {
                    // Write metadata, save here
                    // println!("Success with smart cacher {}", spec.name);
                    hcd.save().await;
                    return Some(HardCacheMetadata {
                        cacher: spec.name.to_string(),
                        version: SMART_CACHER_VERSION,
                    });
                }
                Err(ScErr::Cancel) => {
                    // println!("Smart cacher {} failed, trying next...", spec.name);
                    hcd.save().await;
                    continue;
                }
            }
        }

        println!("Using generic cacher");

        // If we reach here, try a generic start-end downloader.
        let (start_dl, end_dl) = &self.cacher.generic_cache_sizes();

        // Signal to cache the start data.
        let start_data_size = start_dl.with_size(item_size);
        hcd.cache_data(0, start_data_size).await;

        // Signal to cache the end data.
        let end_data_size = end_dl.with_size(item_size);
        hcd.cache_data(item_size - end_data_size, end_data_size)
            .await;

        // Download and save data to disk.
        hcd.save().await;

        // Specify that we have successfully cached with generic_cacher.
        Some(HardCacheMetadata {
            cacher: "generic_cacher".to_string(),
            version: 0,
        })
    }

    pub fn is_latest(&self, meta: &ArchivedHardCacheMetadata) -> bool {
        meta.version >= SMART_CACHER_VERSION
    }
}

/// Trait to allow data from an arbitrary input to an arbitrary output.
#[async_trait]
pub trait HcCacher {
    async fn get_item(&self, item: HardCacheItem) -> Box<dyn HcCacherItem + Send + Sync>;
    fn generic_cache_sizes(&self) -> (DownloadAmount, DownloadAmount);
}

#[async_trait]
pub trait HcCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8>;
    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Vec<u8>;
    async fn cache_data(&mut self, offset: u64, size: u64);
    async fn cache_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>);
    async fn cache_data_to(&mut self, offset: u64);
    async fn cache_data_fully(&mut self);
    async fn save(&mut self);
}

struct HcDownloadCacher {
    drive_access: Arc<DriveAccess>,
}

#[async_trait]
impl HcCacher for HcDownloadCacher {
    async fn get_item(&self, item: HardCacheItem) -> Box<dyn HcCacherItem + Send + Sync> {
        Box::new(HcDownloadCacherItem::new(self.drive_access.clone(), item).await)
    }
    fn generic_cache_sizes(&self) -> (DownloadAmount, DownloadAmount) {
        // TODO: not hardcode this
        let start_dl = DownloadAmount {
            percent: Some(1.0),
            bytes: Some(1_000),
        };

        let end_dl = DownloadAmount {
            percent: Some(1.0),
            bytes: Some(1_000),
        };

        (start_dl, end_dl)
    }
}

struct HcDownloadCacherItem {
    /// Access to the cache filesystem.
    drive_access: Arc<DriveAccess>,
    /// Details about the item to download.
    item: HardCacheItem,
    /// Set of readers at specific offsets.
    readers: BTreeMap<u64, Box<dyn CacheFileHandler>>,
    /// Whether or not the filename was decrypted successfully
    /// (assuming there is a crypt_context in drive_access)
    crypt: bool,
}

impl HcDownloadCacherItem {
    async fn new(
        drive_access: Arc<DriveAccess>,
        mut item: HardCacheItem,
    ) -> HcDownloadCacherItem {
        let mut crypt = false;
        if let Some(ref cc) = drive_access.crypt {
            if let Some(file_name) = cc
                .cipher
                .decrypt_segment(&item.file_name)
                .ok() {
                    crypt = true;
                    item.file_name = file_name;
                }
        }

        HcDownloadCacherItem {
            drive_access,
            item,
            readers: BTreeMap::new(),
            crypt,
        }
    }

    /// Open a new reader, depending on whether filename is encrypted or not.
    async fn open_reader(&mut self, offset: u64) -> Box<dyn CacheFileHandler> {
        if self.crypt {
            if let Some(ref crypt_context) = self.drive_access.crypt {
                let reader = self
                    .drive_access
                    .cache_handler
                    .open_file(self.item.access_id.clone(), self.item.data_id.clone(), self.item.size, offset, true)
                    .await
                    .unwrap();

                if let Some(reader) = CryptPassthrough::new(crypt_context, reader).await {
                    return Box::new(reader)
                }
            }
        }

        self
            .drive_access
            .cache_handler
            .open_file(self.item.access_id.clone(), self.item.data_id.clone(), self.item.size, offset, true)
            .await
            .unwrap()
    }
}

#[async_trait]
impl HcCacherItem for HcDownloadCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8> {
        // println!("read_data {} {}", offset, size);

        let mut buf = vec![0u8; size as usize];
        if let Some(mut reader) = self.readers.remove(&offset) {
            reader.read_exact(&mut buf).await;
            self.readers.insert(offset+size, reader);
        } else {
            let mut reader = self.open_reader(offset).await;
            reader.read_exact(&mut buf).await;
            self.readers.insert(offset+size, reader);
        }

        buf
    }

    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Vec<u8> {
        let prev_offset = self.readers.range_mut(..=offset).rev().next().map(|(prev_offset, _)| *prev_offset);
        if let Some(prev_offset) = prev_offset {
            let bridge_len = offset.saturating_sub(prev_offset);
            if let Some(max_bridge_len) = max_bridge_len {
                if bridge_len < max_bridge_len {
                    // Bridge up to current offset
                    if let Some(mut reader) = self.readers.remove(&prev_offset) {
                        reader.cache_exact(bridge_len as usize).await;
                        let mut buf = vec![0u8; size as usize];
                        reader.read_exact(&mut buf).await;
                        self.readers.insert(offset+size, reader);
                        return buf;
                    }
                }
            } else {
                if let Some(mut reader) = self.readers.remove(&prev_offset) {
                    reader.cache_exact(bridge_len as usize).await;
                    let mut buf = vec![0u8; size as usize];
                    reader.read_exact(&mut buf).await;
                    self.readers.insert(offset+size, reader);
                    return buf;
                }
            }
        }
        // If we can't bridge, just do read_data instead
        self.read_data(offset, size).await
    }

    async fn cache_data(&mut self, offset: u64, size: u64) {
        // println!("cache_data {} {}", offset, size);

        if let Some(mut reader) = self.readers.remove(&offset) {
            reader.cache_exact(size as usize).await;

            self.readers.insert(offset+size, reader);
        } else {
            let mut reader = self
                .drive_access
                .cache_handler
                .open_file(self.item.access_id.clone(), self.item.data_id.clone(), size, offset, true)
                .await
                .unwrap();

            reader.cache_exact(size as usize).await;

            self.readers.insert(offset+size, reader);
        }
    }

    async fn cache_data_bridged(&mut self, _offset: u64, _size: u64, _max_bridge_len: Option<u64>) {
        todo!()
    }


    async fn cache_data_to(&mut self, offset: u64) {
        // println!("cache_data_to {}", offset);
        // Ignore any other readers - make a new one, and go from start to offset
        let mut reader = self
            .drive_access
            .cache_handler
            .open_file(self.item.access_id.clone(), self.item.data_id.clone(), offset, 0, true)
            .await
            .unwrap();
        reader.cache_exact(offset as usize).await;
        
        // Throw into readers, just in case we read from that section later
        if !self.readers.contains_key(&offset) {
            self.readers.insert(offset, reader);
        }
    }

    async fn cache_data_fully(&mut self) {
        todo!()
    }

    async fn save(&mut self) {
    }
}

pub struct HardCacheDownloader {
    item: Box<dyn HcCacherItem + Send + Sync>,
}

impl HardCacheDownloader {
    async fn new(item: Box<dyn HcCacherItem + Send + Sync>) -> HardCacheDownloader {
        HardCacheDownloader { item }
    }

    /// Download `size` bytes of data from `offset` and return it here.
    /// Any data that is read will also be cached.
    pub async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8> {
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
        self.item
            .read_data_bridged(offset, size, max_bridge_len)
            .await
    }

    /// Download `size` bytes of data from `offset`, but do not return it here.
    pub async fn cache_data(&mut self, offset: u64, size: u64) {
        self.item.cache_data(offset, size).await
    }

    /// Download all data from the beginning of the file up to `offset`, but do not return it here.
    pub async fn cache_data_to(&mut self, offset: u64) {
        self.item.cache_data_to(offset).await
    }

    /// Download `size` bytes of data from `offset`, bridging from a previous call, but do not return it here.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous call (or unlimited if `max_bridge_len` is None).
    pub async fn _cache_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) {
        self.item
            .cache_data_bridged(offset, size, max_bridge_len)
            .await
    }

    pub async fn cache_data_fully(&mut self) {
        self.item.cache_data_fully().await
    }

    /// Save all uncached data to disk.
    async fn save(&mut self) {
        self.item.save().await
    }

    pub fn reader<'a>(&'a mut self, offset: u64) -> HardCacheReader<'a> {
        HardCacheReader {
            dl: self,
            offset,
            last_fut: None,
            _phantom: PhantomData,
        }
    }
}

pub struct HardCacheReader<'a> {
    dl: *mut HardCacheDownloader,
    offset: u64,
    last_fut: Option<Pin<Box<dyn Future<Output = Vec<u8>> + 'static>>>, // references HardCacheDownloader, must not escape
    _phantom: PhantomData<&'a mut HardCacheDownloader>,
}

unsafe impl<'a> Send for HardCacheReader<'a> {}
unsafe impl<'a> Sync for HardCacheReader<'a> {}

impl<'a> AsyncRead for HardCacheReader<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        let dl = self.dl;
        let offset = self.offset;
        let last_fut = self.last_fut.get_or_insert_with(|| {
            // SAFETY: As long as last_fut is not leaked outside of HardCacheReader, this should be safe.
            unsafe { &mut *dl }.read_data(offset, buf.remaining() as u64).boxed()
        });
        match last_fut.poll_unpin(cx) {
            Poll::Ready(x) => {
                buf.put_slice(x.as_slice());
                self.last_fut = None;
                self.offset += x.len() as u64;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                Poll::Pending
            },
        }
    }
}

fn hard_cache_item_to_file_spec(item: &HardCacheItem) -> FileSpec {
    FileSpec {
        name: item.file_name.clone(),
        size: item.size,
    }
}