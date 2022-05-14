use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use camino::Utf8Path;
#[cfg(feature = "sctest")]
use camino::Utf8PathBuf;
use futures::{ready, Future, FutureExt};
// use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize, Serialize};
use smart_cacher::{FileSpec, ScErr, ScOk, ScResult, SmartCacher};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use self::smart_cacher::SMART_CACHER_VERSION;
use crate::{
    cache_handlers::{
        crypt_context::CryptContext, crypt_passthrough::CryptPassthrough, CacheFileHandler,
        CacheHandlerError,
    },
    config::{DownloadAmount, SmartCacherConfig},
    db::types::DataIdentifier,
    drive_access::DriveAccess,
};

#[cfg(feature = "sctest")]
mod sctest;
mod smart_cacher;

mod sc_flac;
mod sc_mkv;
mod sc_mp3;
mod sc_ogg;

/*
HARD_CACHE RULES
----------------
If a file's hard cache metadata states the global hard cache version matches, don't touch the file.
If file is below min_size, cache the whole thing. If not, continue.
If file has a known extension as specified in SPEC.exts, attempt to smart cache it. If failed, continue.
Iterate through the rest of the enabled smart cachers. If all of them fail, continue.
Attempt to arbitrarily download some percentage of the file's start and end.
*/

static SMART_CACHERS: &[&dyn SmartCacher] = &[&sc_mp3::ScMp3, &sc_mkv::ScMkv];
/// Skip these extensions, and just generic cache them.
static SKIP_EXTS: &[&str] = &["avi", "mp4", "m4v"];

#[derive(Debug, Clone)]
pub struct HardCacheItem {
    /// Downloader access_id.
    access_id: String,
    /// File name. May be decrypted file name, in which case encrypted file name is in crypt_item.file_name.
    file_name: String,
    /// File size. May be decrypted file size, in which case encrypted file size is in crypt_item.size.
    file_size: u64,
    /// Data ID, for cache on disk.
    data_id: DataIdentifier,
    /// Crypt parameters, if file is encrypted.
    crypt_item: Option<CryptItem>,
    // crypt_context: Option<Arc<CryptContext>>,
}

#[derive(Clone)]
pub struct CryptItem {
    /// Raw encrypted file size.
    size: u64,
    /// Crypt context, for decryption.
    context: Arc<CryptContext>,
}

impl std::fmt::Debug for CryptItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CryptItem")
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HardCacheMetadata {
    /// Name of the (smart) cacher used to cache this file.
    pub cacher: String,
    /// The global smart cacher version used to cache this file.
    pub version: u64,
}

pub struct HardCacher {
    drive_access: Option<Arc<DriveAccess>>,
    cacher: Arc<dyn HcCacher + Send + Sync>,
    // TODO: there may eventually be multiple smart cachers per ext,
    // so this should probably be defined as a Vec<SmartCacher>.
    cachers_by_ext: HashMap<&'static str, &'static dyn SmartCacher>,
    min_size: u64,
}

impl HardCacher {
    /// Create a new HardCacher for production use.
    pub fn new(drive_access: Arc<DriveAccess>, min_size: u64) -> HardCacher {
        let cacher = Arc::new(HcDownloadCacher {
            drive_access: drive_access.clone(),
        });

        Self::new_inner(cacher, min_size, Some(drive_access))
    }

    /// Create a new HardCacher for sctest use.
    #[cfg(feature = "sctest")]
    pub fn new_sctest(
        input: Utf8PathBuf,
        output: Utf8PathBuf,
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

        Self::new_inner(cacher, 0, None)
    }

    fn new_inner(
        cacher: Arc<dyn HcCacher + Send + Sync>,
        min_size: u64,
        drive_access: Option<Arc<DriveAccess>>,
    ) -> HardCacher {
        let mut cachers_by_ext = HashMap::new();

        for sc in SMART_CACHERS {
            let spec = sc.spec();
            for ext in spec.exts {
                cachers_by_ext.insert(*ext, *sc);
            }
        }

        HardCacher {
            drive_access,
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
    ) -> Result<(), CacheHandlerError> {
        // If drive has crypt data, try to decrypt file
        let (crypt_item, dec_file_name, dec_file_size) =
            if let Some(drive_access) = self.drive_access.as_ref() {
                if let Some((crypt_item, dec_file_name, dec_size)) =
                    drive_access.crypts.iter().find_map(|cc| {
                        let enc_file_name = file_name;
                        let enc_size = size;
                        let dec_file_name = cc.cipher.decrypt_segment(enc_file_name).ok()?;
                        let dec_size = CryptContext::get_crypt_file_size(enc_size);
                        let crypt_item = CryptItem {
                            size: enc_size,
                            context: cc.clone(),
                        };
                        Some((crypt_item, dec_file_name, dec_size))
                    })
                {
                    (Some(crypt_item), dec_file_name, dec_size)
                } else {
                    (None, file_name.to_string(), size)
                }
            } else {
                (None, file_name.to_string(), size)
            };

        let item = HardCacheItem {
            access_id: access_id.to_string(),
            file_name: dec_file_name,
            file_size: dec_file_size,
            data_id: data_id.clone(),
            crypt_item,
        };

        self.process_inner(item).await
    }

    /// Process the provided sctest file.
    #[cfg(feature = "sctest")]
    pub async fn process_sctest(&self, file_name: String, file_size: u64) {
        println!("SCTEST");

        // Create fake item for process_partial_cache
        let item = HardCacheItem {
            access_id: String::new(),
            file_name,
            file_size,
            data_id: DataIdentifier::None,
            crypt_item: None,
        };

        self.process_inner(item).await.unwrap();
    }

    async fn process_inner(&self, item: HardCacheItem) -> Result<(), CacheHandlerError> {
        let data_id = item.data_id.clone();
        // Short-circuit if finalized with latest version of smart_cacher
        if let Some(drive_access) = &self.drive_access {
            if let Ok(meta) = drive_access.get_cache_metadata(data_id.clone()).await {
                if self.is_latest(&meta) {
                    return Ok(());
                }
            }
        }
        let file_spec = hard_cache_item_to_file_spec(&item);
        let item_size = item.file_size;

        let ext = Utf8Path::new(&item.file_name).extension();

        let skip_ext = ext.map(|ext| SKIP_EXTS.contains(&ext)).unwrap_or(false);

        let preferred_cacher = if skip_ext {
            None
        } else {
            ext.and_then(|ext| self.cachers_by_ext.get(ext))
        };

        let file_item = item.clone();

        // The compiler complains that _ is unreachable, however
        // this is needed in sctest mode, so squelch the warning.
        #[allow(unreachable_patterns)]
        let hex_md5 = match &file_item.data_id {
            DataIdentifier::GlobalMd5(x) => hex::encode(x),
            _ => "(not md5)".to_string(),
        };
        let cache_item = self.cacher.get_item(item).await;
        let mut hcd = HardCacheDownloader::new(cache_item, file_item).await;

        // If below min_size, call min_size cacher
        if item_size <= self.min_size {
            println!(
                "Using min_size cacher for {} ({} bytes, '{}')",
                file_spec.name, file_spec.size, hex_md5
            );
            hcd.cache_data_fully().await?;
            hcd.save().await;
            let hcm = HardCacheMetadata {
                cacher: "min_size_cacher".to_string(),
                version: 0,
            };
            if let Some(drive_access) = &self.drive_access {
                drive_access
                    .set_cache_metadata(data_id.clone(), hcm)
                    .await?;
                return Ok(());
            }
        }

        // TODO: not hardcode this
        let smart_cacher_config = SmartCacherConfig {
            enabled: true,
            seconds: 10,
        };

        if let Some(pc) = preferred_cacher {
            let spec = pc.spec();
            // println!(
            //     "Trying preferred cacher {} on {} ({} bytes, '{}')",
            //     spec.name, file_spec.name, file_spec.size, hex_md5
            // );
            let res: ScResult = pc.cache(&smart_cacher_config, &file_spec, &mut hcd).await;

            match res {
                Ok(ScOk::Finalize) => {
                    // Write metadata, save here
                    // println!("Success with smart cacher {}", spec.name);
                    hcd.save().await;
                    let hcm = HardCacheMetadata {
                        cacher: spec.name.to_string(),
                        version: SMART_CACHER_VERSION,
                    };
                    if let Some(drive_access) = &self.drive_access {
                        drive_access
                            .set_cache_metadata(data_id.clone(), hcm)
                            .await?;
                    }
                    return Ok(());
                }
                Err(ScErr::Cancel) => {
                    println!(
                        "Preferred cacher {} failed on {}, trying next...",
                        spec.name, file_spec.name
                    );
                }
                Err(ScErr::CacheHandlerError(e)) => {
                    println!("Cache handler error {} {:?}", e, e);
                    return Err(e);
                }
            }
        }

        if !skip_ext {
            for cacher in SMART_CACHERS {
                let spec = cacher.spec();
                if let Some(pc) = preferred_cacher {
                    let pc_spec = pc.spec();
                    if spec.name == pc_spec.name {
                        continue;
                    }
                }
                println!(
                    "Trying smart cacher {} for {} ({} bytes, '{}')",
                    spec.name, file_spec.name, file_spec.size, hex_md5
                );
                let res: ScResult = cacher
                    .cache(&smart_cacher_config, &file_spec, &mut hcd)
                    .await;

                match res {
                    Ok(ScOk::Finalize) => {
                        // Write metadata, save here
                        println!("Success with smart cacher {}", spec.name);
                        hcd.save().await;
                        let hcm = HardCacheMetadata {
                            cacher: spec.name.to_string(),
                            version: SMART_CACHER_VERSION,
                        };
                        if let Some(drive_access) = &self.drive_access {
                            drive_access
                                .set_cache_metadata(data_id.clone(), hcm)
                                .await?;
                        }
                        return Ok(());
                    }
                    Err(ScErr::Cancel) => {
                        println!("Smart cacher {} failed, trying next...", spec.name);
                    }
                    Err(ScErr::CacheHandlerError(e)) => return Err(e),
                }
            }
        }

        // If we reach here, try a generic start-end downloader.
        if !skip_ext {
            println!(
                "Using generic cacher for {} ({} bytes, '{}')",
                file_spec.name, file_spec.size, hex_md5
            );
        }

        let (start_dl, end_dl) = &self.cacher.generic_cache_sizes();

        // Signal to cache the start data.
        let start_data_size = start_dl.with_size(item_size);
        hcd.cache_data(0, start_data_size).await?;

        // Signal to cache the end data.
        let end_data_size = end_dl.with_size(item_size);
        hcd.cache_data(item_size - end_data_size, end_data_size)
            .await?;

        // Download and save data to disk.
        hcd.save().await;

        // Specify that we have successfully cached with generic_cacher.
        let hcm = HardCacheMetadata {
            cacher: "generic_cacher".to_string(),
            version: 0,
        };
        if let Some(drive_access) = &self.drive_access {
            drive_access
                .set_cache_metadata(data_id.clone(), hcm)
                .await?;
        }
        Ok(())
    }

    pub fn is_latest(&self, meta: &HardCacheMetadata) -> bool {
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
    async fn read_data(&mut self, offset: u64, size: u64) -> Result<Vec<u8>, CacheHandlerError>;
    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<Vec<u8>, CacheHandlerError>;
    async fn cache_data(&mut self, offset: u64, size: u64) -> Result<(), CacheHandlerError>;
    async fn cache_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<(), CacheHandlerError>;
    async fn cache_data_to(&mut self, offset: u64) -> Result<(), CacheHandlerError>;
    async fn cache_data_fully(&mut self) -> Result<(), CacheHandlerError>;
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
}

impl HcDownloadCacherItem {
    async fn new(drive_access: Arc<DriveAccess>, item: HardCacheItem) -> HcDownloadCacherItem {
        HcDownloadCacherItem {
            drive_access,
            item,
            readers: BTreeMap::new(),
        }
    }

    /// Open a new reader, depending on whether filename is encrypted or not.
    async fn open_reader(
        &mut self,
        offset: u64,
    ) -> Result<Box<dyn CacheFileHandler>, CacheHandlerError> {
        if let Some(ref crypt_item) = self.item.crypt_item {
            let reader = self
                .drive_access
                .cache_handler
                .open_file(
                    self.item.access_id.clone(),
                    self.item.data_id.clone(),
                    crypt_item.size, // FIXME: TODO: is this the bug? should be full size?
                    0,
                    true,
                )
                .await?;

            return Ok(Box::new(
                CryptPassthrough::new(&crypt_item.context, offset, reader).await?,
            ));
        }

        self.drive_access
            .cache_handler
            .open_file(
                self.item.access_id.clone(),
                self.item.data_id.clone(),
                self.item.file_size,
                offset,
                true,
            )
            .await
    }
}

#[async_trait]
impl HcCacherItem for HcDownloadCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Result<Vec<u8>, CacheHandlerError> {
        let mut buf = vec![0u8; size as usize];
        if let Some(mut reader) = self.readers.remove(&offset) {
            reader.read_exact(&mut buf).await?;
            self.readers.insert(offset + size, reader);
        } else {
            // TODO: Look for readers <10KB before our position, and bridge from them
            let mut reader = self.open_reader(offset).await?;
            reader.read_exact(&mut buf).await?;
            self.readers.insert(offset + size, reader);
        }

        Ok(buf)
    }

    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<Vec<u8>, CacheHandlerError> {
        let prev_offset = self
            .readers
            .range_mut(..=offset)
            .rev()
            .next()
            .map(|(prev_offset, _)| *prev_offset);
        if let Some(prev_offset) = prev_offset {
            let bridge_len = offset.saturating_sub(prev_offset);
            if let Some(max_bridge_len) = max_bridge_len {
                if bridge_len < max_bridge_len {
                    // Bridge up to current offset
                    if let Some(mut reader) = self.readers.remove(&prev_offset) {
                        reader.cache_exact(bridge_len as usize).await?;
                        let mut buf = vec![0u8; size as usize];
                        reader.read_exact(&mut buf).await?;
                        self.readers.insert(offset + size, reader);
                        return Ok(buf);
                    }
                }
            } else if let Some(mut reader) = self.readers.remove(&prev_offset) {
                reader.cache_exact(bridge_len as usize).await?;
                let mut buf = vec![0u8; size as usize];
                reader.read_exact(&mut buf).await?;
                self.readers.insert(offset + size, reader);
                return Ok(buf);
            }
        }
        // If we can't bridge, just do read_data instead
        self.read_data(offset, size).await
    }

    async fn cache_data(&mut self, offset: u64, size: u64) -> Result<(), CacheHandlerError> {
        if self.item.file_size > offset {
            return Ok(());
        }
        if let Some(mut reader) = self.readers.remove(&offset) {
            reader.cache_exact(size as usize).await?;

            self.readers.insert(offset + size, reader);
        } else {
            let mut reader = self
                .drive_access
                .cache_handler
                .open_file(
                    self.item.access_id.clone(),
                    self.item.data_id.clone(),
                    self.item.file_size,
                    offset,
                    true,
                )
                .await?;

            reader.cache_exact(size as usize).await?;

            self.readers.insert(offset + size, reader);
        }

        Ok(())
    }

    async fn cache_data_bridged(
        &mut self,
        _offset: u64,
        _size: u64,
        _max_bridge_len: Option<u64>,
    ) -> Result<(), CacheHandlerError> {
        todo!()
    }

    async fn cache_data_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
        // Ignore any other readers - make a new one, and go from start to offset
        let mut reader = self
            .drive_access
            .cache_handler
            .open_file(
                self.item.access_id.clone(),
                self.item.data_id.clone(),
                self.item.file_size,
                0,
                true,
            )
            .await?;
        reader.cache_exact(offset as usize).await?;

        // Throw into readers, just in case we read from that section later
        self.readers.entry(offset).or_insert(reader);

        Ok(())
    }

    async fn cache_data_fully(&mut self) -> Result<(), CacheHandlerError> {
        let mut reader = self
            .drive_access
            .cache_handler
            .open_file(
                self.item.access_id.clone(),
                self.item.data_id.clone(),
                self.item.file_size,
                0,
                true,
            )
            .await?;
        reader.cache_exact(self.item.file_size as usize).await?;

        Ok(())
    }

    async fn save(&mut self) {
    }
}

pub struct HardCacheDownloader {
    item: Box<dyn HcCacherItem + Send + Sync>,
    file_item: HardCacheItem,
}

impl HardCacheDownloader {
    async fn new(
        item: Box<dyn HcCacherItem + Send + Sync>,
        file_item: HardCacheItem,
    ) -> HardCacheDownloader {
        HardCacheDownloader { item, file_item }
    }

    /// Download `size` bytes of data from `offset` and return it here.
    /// Any data that is read will also be cached.
    pub async fn read_data(
        &mut self,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, CacheHandlerError> {
        if size == 0 {
            return Ok(vec![]);
        }

        self.item.read_data(offset, size).await
    }

    //pub async fn read_data_tokio_io(&mut self, offset: u64, size: u64) -> Result<Vec<u8>>

    /// Download `size` bytes of data from `offset` and return it here, bridging from a previous call.
    /// If a read_data or cache_data call was performed at a previous offset, cache all data between it and this call.
    /// Only bridge data if there's less than `max_bridge_len` bytes between the start of this call and the end of
    /// the previous-offset call (or unlimited if `max_bridge_len` is None).
    pub async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<Vec<u8>, CacheHandlerError> {
        self.item
            .read_data_bridged(offset, size, max_bridge_len)
            .await
    }

    /// Download `size` bytes of data from `offset`, but do not return it here.
    pub async fn cache_data(&mut self, offset: u64, size: u64) -> Result<(), CacheHandlerError> {
        self.item.cache_data(offset, size).await
    }

    /// Download all data from the beginning of the file up to `offset`, but do not return it here.
    pub async fn cache_data_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
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
    ) -> Result<(), CacheHandlerError> {
        self.item
            .cache_data_bridged(offset, size, max_bridge_len)
            .await
    }

    pub async fn cache_data_fully(&mut self) -> Result<(), CacheHandlerError> {
        self.item.cache_data_fully().await
    }

    /// Save all uncached data to disk.
    async fn save(&mut self) {
        self.item.save().await
    }

    pub fn reader(&mut self, offset: u64) -> HardCacheReader<'_> {
        let size = self.file_item.file_size;
        HardCacheReader {
            dl_ptr: self,
            dl: self,
            offset,
            size,
            last_fut: None,
        }
    }
}

type LastFut<T> = Option<Pin<Box<dyn Future<Output = T> + Send + 'static>>>;

pub struct HardCacheReader<'a> {
    dl_ptr: *mut HardCacheDownloader,
    dl: &'a mut HardCacheDownloader,
    size: u64,
    offset: u64,
    last_fut: LastFut<Result<Vec<u8>, CacheHandlerError>>, // references HardCacheDownloader, must not escape
}

unsafe impl<'a> Send for HardCacheReader<'a> {
}
unsafe impl<'a> Sync for HardCacheReader<'a> {
}

impl<'a> AsyncRead for HardCacheReader<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        let dl = self.dl_ptr;
        let offset = self.offset;
        let last_fut = self.last_fut.get_or_insert_with(|| {
            // SAFETY: As long as last_fut is not leaked outside of HardCacheReader, this should be safe.
            unsafe { &mut *dl }
                .read_data(offset, buf.remaining() as u64)
                .boxed()
        });

        // Get the ready value of the Poll, otherwise return Pending
        let x = ready!(last_fut.poll_unpin(cx));
        match x {
            Ok(x) => {
                buf.put_slice(x.as_slice());
                self.last_fut = None;
                self.offset += x.len() as u64;
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e.into_tokio_io_error())),
        }
    }
}

impl<'a> AsyncSeek for HardCacheReader<'a> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        match position {
            SeekFrom::Start(val) => {
                self.offset = min(val, self.size);
            }
            SeekFrom::End(val) => {
                let res_offset = (self.size as i64).saturating_sub(val) as u64;
                self.offset = min(res_offset, self.size);
            }
            SeekFrom::Current(val) => {
                let res_offset = (self.offset as i64).saturating_add(val) as u64;
                self.offset = min(res_offset, self.size);
            }
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.offset))
    }
}

impl<'a> HardCacheReader<'a> {
    async fn cache_bytes(&mut self, size: u64) -> Result<(), CacheHandlerError> {
        self.dl.cache_data(self.offset, size).await?;
        self.offset += size;

        Ok(())
    }

    async fn cache_bytes_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
        let size = offset - self.offset;
        self.dl.cache_data(self.offset, size).await?;
        self.offset += size;

        Ok(())
    }

    fn seek(&mut self, offset: u64) {
        self.offset = min(offset, self.size);
    }

    fn tell(&self) -> u64 {
        self.offset
    }
}

fn hard_cache_item_to_file_spec(item: &HardCacheItem) -> FileSpec {
    FileSpec {
        name: item.file_name.clone(),
        size: item.file_size,
    }
}
