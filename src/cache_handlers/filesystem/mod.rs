// Filesystem cache handler

use std::{
    cmp::min,
    collections::HashMap,
    io::SeekFrom,
    mem,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use byte_ranger::GetRange;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
    time::sleep,
};

use self::{
    lru::Lru,
    open_file::{Cache, DownloadHandle, DownloadStatus, Receiver},
};
use super::{CacheDriveHandler, CacheFileHandler, CacheHandlerError, Page};
use crate::{
    db::types::DataIdentifier,
    downloaders::{DownloaderDrive, DownloaderError},
    hard_cache::HardCacheMetadata,
};

mod open_file;
use open_file::OpenFile;
use tracing::instrument;

pub mod lru;

/// The largest size of chunk file allowed within cache directories.
/// When this limit is reached, create a new adjacent chunk file.
const MAX_CHUNK_SIZE: u64 = 100_000_000;

/// Filesystem-based CacheDriveHandler.
pub struct FilesystemCacheHandler {
    drive_id: String,
    lru: Option<Lru>,
    hard_cache_root: PathBuf,
    soft_cache_root: PathBuf,
    open_files: Mutex<HashMap<String, Weak<Mutex<OpenFile>>>>,
    downloader_drive: Arc<dyn DownloaderDrive>,
}

impl std::fmt::Debug for FilesystemCacheHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilesystemCacheHandler")
            .field("drive_id", &self.drive_id)
            .finish()
    }
}

#[derive(Debug)]
struct ReadData {
    /// Chunk file to read from.
    file: File,
    /// End offset of chunk on disk.
    /// If downloading, this will need to be updated from the OpenFile.
    end_offset: u64,
    /// Whether we're reading/downloading from hard_cache or soft_cache.
    is_hard_cache: bool,
    /// Start offset of currently reading/downloading chunk.
    chunk_start_offset: u64,
}

#[derive(Debug)]
enum CurrentChunk {
    /// Currently downloading data from download provider.
    /// This can also be used to "download" from soft_cache to hard_cache.
    Downloading {
        read_data: ReadData,
        /// Handle containing the downloader and write file handle.
        /// Not directly accessible in the threads, and only
        /// used for garbage collection when all threads are closed.
        _dl: DownloadHandle,
    },
    /// Currently reading data from disk.
    /// This is used if we haven't reached the end_offset, but nobody else is
    /// currently downloading.
    Reading { read_data: ReadData },
}

#[derive(Debug)]
/// Values that can be returned by chunk readers.
enum Reader {
    /// Regular data has been returned.
    Data(usize),
    /// Regular data has been returned, but current chunk needs to be reset.
    LastData(usize),
    /// DownloadStatus in chunk disappeared but there's some data left,
    /// so downgrade to a reader until we get to the next chunk.
    Downgrade,
    /// Continue downloading a new chunk from one that reached MAX_CHUNK_SIZE.
    ContinueDownloading(Arc<Mutex<Option<DownloadStatus>>>),
    /// No data could be found, but we're not at EOF, so current chunk needs
    /// to be reset.
    NeedsNewChunk,
}

impl FilesystemCacheHandler {
    pub fn new(
        drive_id: &str,
        lru: Option<Lru>,
        hard_cache_root: &Path,
        soft_cache_root: &Path,
        downloader_drive: Arc<dyn DownloaderDrive>,
    ) -> FilesystemCacheHandler {
        let open_files = Mutex::new(HashMap::new());
        let hard_cache_root = hard_cache_root.to_path_buf();
        let soft_cache_root = soft_cache_root.to_path_buf();
        FilesystemCacheHandler {
            drive_id: drive_id.to_owned(),
            lru,
            hard_cache_root,
            soft_cache_root,
            open_files,
            downloader_drive,
        }
    }

    async fn get_open_handle(
        &self,
        file_id: &str,
        data_id: &DataIdentifier,
    ) -> Arc<Mutex<OpenFile>> {
        let mut files = self.open_files.lock().await;
        if let Some(file) = files.get(file_id) {
            if let Some(file_arc) = file.upgrade() {
                // Existing file, use it
                return file_arc;
            } else {
                // Existing file, but last handle died, so create new one
            }
        }

        // No file, creating new one
        let new_file = Arc::new(Mutex::new(self.create_open_handle(file_id, data_id).await));

        // Store a weak pointer to the open file, so we don't have to do our own reference counting
        files.insert(file_id.to_owned(), Arc::downgrade(&new_file));
        new_file
    }

    async fn create_open_handle(&self, file_id: &str, data_id: &DataIdentifier) -> OpenFile {
        let downloader_drive = self.downloader_drive.clone();
        OpenFile::new(
            self.lru.clone(),
            &self.hard_cache_root,
            &self.soft_cache_root,
            file_id,
            data_id,
            downloader_drive,
        )
        .await
    }
}

#[async_trait]
impl CacheDriveHandler for FilesystemCacheHandler {
    /// Passthrough method for DownloaderDrive's scan_pages method.
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin> {
        Box::new(
            self.downloader_drive
                .scan_pages(last_page_token, last_modified_date),
        )
    }

    fn watch_changes(
        &self,
    ) -> Box<
        dyn Stream<Item = Result<Vec<crate::downloaders::Change>, DownloaderError>>
            + Send
            + Sync
            + Unpin,
    > {
        Box::new(self.downloader_drive.watch_changes())
    }

    async fn open_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
        size: u64,
        offset: u64,
        write_hard_cache: bool,
    ) -> Result<Box<dyn CacheFileHandler>, CacheHandlerError> {
        let handle = self.get_open_handle(&file_id, &data_id).await;
        let hard_cache_file_root = get_file_cache_path(&self.hard_cache_root, &data_id);
        let soft_cache_file_root = get_file_cache_path(&self.soft_cache_root, &data_id);

        Ok(Box::new(FilesystemCacheFileHandler {
            handle,
            data_id: data_id.clone(),
            lru: self.lru.clone(),
            size,
            offset,
            write_hard_cache,
            hard_cache_file_root,
            soft_cache_file_root,
            current_chunk: None,
        }))
    }

    fn get_drive_id(&self) -> String {
        self.drive_id.clone()
    }

    fn get_drive_type(&self) -> &'static str {
        self.downloader_drive.get_downloader_type()
    }

    async fn clear_cache_item(&self, data_id: DataIdentifier) {
        let hard_cache_file_root = get_file_cache_path(&self.hard_cache_root, &data_id);
        let soft_cache_file_root = get_file_cache_path(&self.soft_cache_root, &data_id);
        let _ = fs::remove_dir_all(&hard_cache_file_root).await;
        let _ = fs::remove_dir_all(&soft_cache_file_root).await;
    }

    async fn get_cache_metadata(
        &self,
        data_id: DataIdentifier,
    ) -> Result<HardCacheMetadata, CacheHandlerError> {
        let hard_cache_path = get_file_cache_path(&self.hard_cache_root, &data_id);
        let hcm_path = hard_cache_path.join("metadata.json");

        if !hard_cache_path.exists() {
            tokio::fs::create_dir_all(hard_cache_path).await?;
        }
        let meta_bytes = tokio::fs::read(hcm_path).await?;
        let meta = serde_json::from_slice(&meta_bytes)?;

        Ok(meta)
    }

    async fn set_cache_metadata(
        &self,
        data_id: DataIdentifier,
        meta: HardCacheMetadata,
    ) -> Result<(), CacheHandlerError> {
        let hard_cache_path = get_file_cache_path(&self.hard_cache_root, &data_id);
        let hcm_path = hard_cache_path.join("metadata.json");

        if !hard_cache_path.exists() {
            tokio::fs::create_dir_all(hard_cache_path).await?;
        }
        let mut hcm_file = File::create(hcm_path).await?;

        let meta_bytes = serde_json::to_vec_pretty(&meta)?;
        hcm_file.write_all(&meta_bytes).await?;

        Ok(())
    }

    async fn unlink_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
    ) -> Result<(), CacheHandlerError> {
        self.downloader_drive.delete_file(file_id).await?;
        self.clear_cache_item(data_id).await;

        Ok(())
    }

    #[instrument]
    async fn rename_file(
        &self,
        file_id: String,
        old_new_parent_ids: Option<(String, String)>,
        new_file_name: Option<String>,
    ) -> Result<(), CacheHandlerError> {
        self.downloader_drive
            .move_file(file_id, old_new_parent_ids, new_file_name)
            .await?;

        Ok(())
    }
}

/// Handle to an open file, to be read and seeked.
struct FilesystemCacheFileHandler {
    /// Structure used for synchronizing between all readers.
    handle: Arc<Mutex<OpenFile>>,
    /// ID for accessing data in cache
    data_id: DataIdentifier,
    /// LRU for handling soft_cache space limiting
    lru: Option<Lru>,
    size: u64,
    offset: u64,
    hard_cache_file_root: PathBuf,
    soft_cache_file_root: PathBuf,
    write_hard_cache: bool,
    /// Handle to cache file on disk (a "chunk") in
    /// soft_cache or hard_cache directory.
    current_chunk: Option<CurrentChunk>,
}

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> Result<usize, CacheHandlerError> {
        self.handle_read_into(buf.len(), Some(buf)).await
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, CacheHandlerError> {
        // Call read_into repeatedly until correct length, or EOF
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.read_into(&mut buf[total_bytes_read..]).await?;
            if bytes_read == 0 {
                return Ok(total_bytes_read);
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == buf.len() {
                    return Ok(total_bytes_read);
                }
            }
        }
    }

    async fn cache_data(&mut self, len: usize) -> Result<usize, CacheHandlerError> {
        self.handle_read_into(len, None).await
    }

    async fn cache_exact(&mut self, len: usize) -> Result<usize, CacheHandlerError> {
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.cache_data(len - total_bytes_read).await?;
            if bytes_read == 0 {
                return Ok(total_bytes_read);
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == len {
                    return Ok(total_bytes_read);
                }
            }
        }
    }

    async fn seek_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
        if offset == self.offset {
            return Ok(());
        }
        // Close all downloads and files, set offset
        self.current_chunk = None;
        self.offset = min(offset, self.size);

        Ok(())
    }
}

impl FilesystemCacheFileHandler {
    /// Handles both the read_into and cache_data methods
    async fn handle_read_into(
        &mut self,
        len: usize,
        mut buf: Option<&mut [u8]>,
    ) -> Result<usize, CacheHandlerError> {
        // EOF short-circuit
        if self.offset == self.size || len == 0 {
            return Ok(0);
        }

        if self.current_chunk.is_none() {
            // We're not in the middle of reading a chunk, so get a new one
            let new_chunk = self.new_current_chunk(None).await?;
            self.current_chunk = Some(new_chunk);
        }


        for attempt in 0..40 {
            if attempt > 20 {
                sleep(Duration::from_millis(50 * attempt)).await;
            }

            // Attempt to read from (or download to) a chunk file
            let handle_chunk_res = self.handle_chunk(len, &mut buf).await?;
            match handle_chunk_res {
                Reader::Data(data_read) => {
                    // We received the data we wanted, continue as normal.
                    return Ok(data_read);
                }
                Reader::LastData(data_read) => {
                    // We received the data we wanted, but reached the end of the chunk.
                    // On next read, try to read from the next adjacent chunk, or download
                    // a new chunk.
                    self.current_chunk = None;
                    return Ok(data_read);
                }
                Reader::NeedsNewChunk => {
                    // We received no data from the current chunk, which means we were either
                    // at the end of the chunk, or no chunk was found at this offset.
                    // Create a new chunk.
                    let new_chunk = self.new_current_chunk(None).await?;
                    self.current_chunk = Some(new_chunk);
                }
                Reader::Downgrade => {
                    // The chunk has finished downloading, but there's more we can read.
                    // This is usually returned when a different process was further ahead
                    // in the chunk than we were.
                    let mut new_chunk = None;
                    mem::swap(&mut new_chunk, &mut self.current_chunk);
                    if let Some(CurrentChunk::Downloading { read_data, _dl }) = new_chunk {
                        self.current_chunk = Some(CurrentChunk::Reading { read_data });
                    } else {
                        // We aren't a downloader, so swap back.
                        mem::swap(&mut new_chunk, &mut self.current_chunk);
                    };
                }
                Reader::ContinueDownloading(prev_download_status) => {
                    // We downloaded a chunk to its maximum size (100MB by default), and we need to
                    // make a new chunk while still keeping the same download thread.
                    // Create a new chunk using the old chunk's DownloadStatus struct.
                    let mut dl_status = prev_download_status.lock().await;
                    let mut new_dl_status = None;
                    mem::swap(&mut *dl_status, &mut new_dl_status);
                    let new_chunk = self.new_current_chunk(new_dl_status).await?;
                    self.current_chunk = Some(new_chunk);
                }
            }
        }

        // Return an error up to the FUSE mount if 40 attempts to read a chunk failed.
        // This only appears to happen if a downloader has a download quota reached,
        // or if there is trouble with internet or downloader service access.
        Err(CacheHandlerError::HandleInto)
    }

    /// Handle reading from, or downloading to, the current cache chunk file.
    /// Make every attempt to read from a chunk file without locking the OpenFile struct.
    /// 
    /// Expects self.current_chunk to be Some(), otherwise it will panic.
    async fn handle_chunk(
        &mut self,
        len: usize,
        buf: &mut Option<&mut [u8]>,
    ) -> Result<Reader, CacheHandlerError> {
        if let Some(ref mut current_chunk) = self.current_chunk {
            let data_read = match current_chunk {
                CurrentChunk::Downloading { read_data, _dl } => {
                    // Chunk is in downloading state, so we might need to lock
                    if self.offset < read_data.end_offset {
                        // If we have any data before end_offset, read that first
                        let max_read_len = min(len, (read_data.end_offset - self.offset) as usize);
                        let sr_res =
                            Self::simple_read(&mut read_data.file, buf, max_read_len).await?;
                        Reader::Data(sr_res)
                    } else {
                        // Otherwise, lock the OpenFile and try to read/download from there
                        let mut of = self.handle.lock().await;
                        Self::downloader_read(read_data, &mut of, buf, len, self.offset).await?
                    }
                }
                CurrentChunk::Reading { read_data } => {
                    // Chunk is in reading state, which means we won't have to lock until
                    // we reach the end of it.
                    if self.offset < read_data.end_offset {
                        // If we have any data before end_offset, read that first
                        let max_read_len = min(len, (read_data.end_offset - self.offset) as usize);
                        let sr_res =
                            Self::simple_read(&mut read_data.file, buf, max_read_len).await?;
                        if self.offset + (sr_res as u64) == read_data.end_offset {
                            Reader::LastData(sr_res)
                        } else {
                            Reader::Data(sr_res)
                        }
                    } else {
                        // return None here, to get the next chunk
                        Reader::NeedsNewChunk
                    }
                }
            };

            if let Reader::Data(data_read) | Reader::LastData(data_read) = data_read {
                self.offset += data_read as u64;
            }
            Ok(data_read)
        } else {
            unreachable!("self.current_chunk cannot be None here");
        }
    }

    /// Figure out the current chunk based on our offset.
    async fn new_current_chunk(
        &mut self,
        prev_download_status: Option<DownloadStatus>,
    ) -> Result<CurrentChunk, CacheHandlerError> {
        let mut of = self.handle.lock().await;
        // TODO: could probably switch Cache::Any to Cache::Hard when self.write_hard_cache is true
        // then simplify this function a bunch
        let (chunk, is_hard_cache) = of.get_chunk_at(self.offset, Cache::Any);

        // Where are we?
        match chunk {
            // We're at the start of, or in the middle of, a chunk
            GetRange::Data {
                start_offset,
                size,
                data,
            } => {
                let chunk_start_offset = data.chunk_start_offset;
                if self.write_hard_cache && !is_hard_cache {
                    // We're writing to hard_cache and data exists in soft_cache
                    // Copy from soft_cache to hard_cache
                    self.start_copying(&mut of, chunk_start_offset, start_offset, size)
                        .await
                } else {
                    // Check if there's an active downloader
                    if let Some(download_status) = data.download_status.upgrade() {
                        self.start_reading(
                            chunk_start_offset,
                            start_offset,
                            size,
                            is_hard_cache,
                            Some(download_status.clone()),
                        )
                        .await
                    } else {
                        self.start_reading(
                            chunk_start_offset,
                            start_offset,
                            size,
                            is_hard_cache,
                            None,
                        )
                        .await
                    }
                }
            }
            // We're somewhere in between two chunks
            GetRange::Gap {
                start_offset,
                size: _,
                prev_range,
            } => {
                if let Some(prev_range) = prev_range {
                    if self.write_hard_cache && !is_hard_cache {
                        // If we're writing to hard_cache and this gap exists in the soft_cache,
                        // explicitly check the hard_cache to see if there's a gap, as we
                        // might be able to append to a hard_cache chunk.
                        // Otherwise, start downloading a new chunk.
                        let (hc_chunk, _) = of.get_chunk_at(self.offset, Cache::Hard);
                        match hc_chunk {
                            GetRange::Gap {
                                start_offset,
                                size: _,
                                prev_range,
                            } => {
                                if self.offset == start_offset {
                                    if let Some(prev_range) = prev_range {
                                        let chunk_start_offset = prev_range.data.chunk_start_offset;
                                        if let Some(prev_download_status) = prev_download_status {
                                            return self
                                                .transfer_dl(&mut of, prev_download_status)
                                                .await;
                                        } else {
                                            let dl_status =
                                                prev_range.data.download_status.upgrade();
                                            return self
                                                .start_append_dl(
                                                    &mut of,
                                                    chunk_start_offset,
                                                    dl_status,
                                                )
                                                .await;
                                        }
                                    }
                                }
                            }
                            GetRange::PastFinalRange {
                                start_offset,
                                size,
                                data,
                            } => {
                                if self.offset == start_offset + size {
                                    if let Some(prev_download_status) = prev_download_status {
                                        return self
                                            .transfer_dl(&mut of, prev_download_status)
                                            .await;
                                    } else {
                                        let dl_status = data.download_status.upgrade();
                                        return self
                                            .start_append_dl(&mut of, start_offset, dl_status)
                                            .await;
                                    }
                                }
                            }
                            _ => {}
                        }
                        self.start_new_dl(&mut of).await
                    } else if self.offset == start_offset && self.write_hard_cache == is_hard_cache
                    {
                        // We're at the end of the previous chunk and have enough space to write
                        // And the previous chunk matches the cache type
                        // Download and append data to chunk
                        let chunk_start_offset = prev_range.data.chunk_start_offset;
                        if let Some(prev_download_status) = prev_download_status {
                            return self.transfer_dl(&mut of, prev_download_status).await;
                        } else {
                            let dl_status = prev_range.data.download_status.upgrade();
                            self.start_append_dl(&mut of, chunk_start_offset, dl_status)
                                .await
                        }
                    } else {
                        // We're not at the end of the previous chunk, but have space to write
                        // Start downloading new chunk
                        self.start_new_dl(&mut of).await
                    }
                } else {
                    // There is no previous range, so we have to start new
                    self.start_new_dl(&mut of).await
                }
            }
            // We're somewhere after the end of the final chunk
            GetRange::PastFinalRange {
                start_offset,
                size,
                data,
            } => {
                if self.write_hard_cache && !is_hard_cache {
                    // If we're writing to hard_cache and this gap exists in the soft_cache,
                    // explicitly check the hard_cache to see if there's a gap, as we
                    // might be able to append to a hard_cache chunk.
                    // Otherwise, start downloading a new chunk.
                    let (hc_chunk, _) = of.get_chunk_at(self.offset, Cache::Hard);
                    match hc_chunk {
                        GetRange::Gap {
                            start_offset,
                            size: _,
                            prev_range,
                        } => {
                            if self.offset == start_offset {
                                if let Some(prev_range) = prev_range {
                                    let chunk_start_offset = prev_range.data.chunk_start_offset;

                                    if let Some(prev_download_status) = prev_download_status {
                                        return self
                                            .transfer_dl(&mut of, prev_download_status)
                                            .await;
                                    } else {
                                        let dl_status = prev_range.data.download_status.upgrade();
                                        return self
                                            .start_append_dl(&mut of, chunk_start_offset, dl_status)
                                            .await;
                                    }
                                }
                            }
                        }
                        GetRange::PastFinalRange {
                            start_offset,
                            size,
                            data,
                        } => {
                            if self.offset == start_offset + size {
                                if let Some(prev_download_status) = prev_download_status {
                                    return self.transfer_dl(&mut of, prev_download_status).await;
                                } else {
                                    let dl_status = data.download_status.upgrade();
                                    return self
                                        .start_append_dl(&mut of, start_offset, dl_status)
                                        .await;
                                }
                            }
                        }
                        _ => {}
                    }
                    self.start_new_dl(&mut of).await
                } else if self.offset == start_offset + size
                    && self.write_hard_cache == is_hard_cache
                {
                    // We're at the end of the previous chunk
                    // Download and append data to chunk
                    let chunk_start_offset = data.chunk_start_offset;
                    if let Some(prev_download_status) = prev_download_status {
                        return self.transfer_dl(&mut of, prev_download_status).await;
                    } else {
                        let dl_status = data.download_status.upgrade();
                        self.start_append_dl(&mut of, chunk_start_offset, dl_status)
                            .await
                    }
                } else {
                    // We're not at the end of the previous chunk, but have space to write
                    // Start downloading new chunk
                    self.start_new_dl(&mut of).await
                }
            }
            // There are no chunks in the file, so start downloading new chunk
            GetRange::Empty => self.start_new_dl(&mut of).await,
        }
    }

    /// Start a download, creating a new chunk.
    async fn start_new_dl(&self, of: &mut OpenFile) -> Result<CurrentChunk, CacheHandlerError> {
        let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);

        let (file, dl) = of
            .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
            .await?;

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset: self.offset,
            is_hard_cache: self.write_hard_cache,
        };

        Ok(CurrentChunk::Downloading { read_data, _dl: dl })
    }

    /// Create a new chunk from previous download, if possible.
    /// If previous chunk's DownloadStatus is none, just create a new download.
    async fn transfer_dl(
        &self,
        of: &mut OpenFile,
        prev_download_status: DownloadStatus,
    ) -> Result<CurrentChunk, CacheHandlerError> {
        let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);

        let (file, dl) = of
            .start_transfer_chunk(
                self.offset,
                prev_download_status,
                self.write_hard_cache,
                &file_path,
            )
            .await?;

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset: self.offset,
            is_hard_cache: self.write_hard_cache,
        };

        Ok(CurrentChunk::Downloading { read_data, _dl: dl })
    }

    /// Start a download, appending to an existing chunk.
    /// Use an existing downloader if possible.
    ///
    /// chunk_start_offset: Where the previous chunk starts
    async fn start_append_dl(
        &self,
        of: &mut OpenFile,
        chunk_start_offset: u64,
        download_status: Option<Arc<Mutex<Option<DownloadStatus>>>>,
    ) -> Result<CurrentChunk, CacheHandlerError> {
        let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, chunk_start_offset);

        if let Some(download_status) = download_status {
            let dl_status_arc = download_status.clone();
            if let Some(download_status) = &*download_status.lock().await {
                if let Receiver::Downloader(_) = download_status.receiver {
                    let mut file = File::open(&file_path).await?;
                    if let Some(lru) = &self.lru {
                        if !self.write_hard_cache {
                            lru.touch_file(&self.data_id, chunk_start_offset).await;
                        }
                    }
                    file.seek(SeekFrom::Start(self.offset - chunk_start_offset))
                        .await?;

                    let read_data = ReadData {
                        file,
                        end_offset: self.offset,
                        chunk_start_offset,
                        is_hard_cache: self.write_hard_cache,
                    };

                    let dl = DownloadHandle {
                        dl_handle: dl_status_arc,
                    };
                    return Ok(CurrentChunk::Downloading { read_data, _dl: dl });
                }
            }
        }

        let (file, dl) = of
            .append_download_chunk(
                chunk_start_offset,
                self.offset,
                self.write_hard_cache,
                &file_path,
            )
            .await?;

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset,
            is_hard_cache: self.write_hard_cache,
        };

        Ok(CurrentChunk::Downloading { read_data, _dl: dl })
    }

    /// Start copying an existing chunk from soft_cache to hard_cache.
    async fn start_copying(
        &self,
        of: &mut OpenFile,
        chunk_start_offset: u64,
        start_offset: u64,
        size: u64,
    ) -> Result<CurrentChunk, CacheHandlerError> {
        let source_file_path = self.get_file_cache_chunk_path(false, chunk_start_offset);
        let source_file_offset = self.offset - start_offset;
        let source_file_end_offset = source_file_offset + size;

        let hc_file_path = self.get_file_cache_chunk_path(true, self.offset);
        let (file, dl) = of
            .start_copy_chunk(
                self.offset,
                &source_file_path,
                source_file_offset,
                source_file_end_offset,
                &hc_file_path,
            )
            .await?;

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset: self.offset,
            is_hard_cache: self.write_hard_cache,
        };

        Ok(CurrentChunk::Downloading { read_data, _dl: dl })
    }

    /// Start reading an existing chunk.
    /// Act as a downloader if one exists.
    async fn start_reading(
        &self,
        chunk_start_offset: u64,
        start_offset: u64,
        size: u64,
        is_hard_cache: bool,
        download_status: Option<Arc<Mutex<Option<DownloadStatus>>>>,
    ) -> Result<CurrentChunk, CacheHandlerError> {
        let file_path = self.get_file_cache_chunk_path(is_hard_cache, chunk_start_offset);

        let mut file = File::open(&file_path).await?;
        if let Some(lru) = &self.lru {
            if !is_hard_cache {
                lru.touch_file(&self.data_id, chunk_start_offset).await;
            }
        }

        file.seek(SeekFrom::Start(self.offset - start_offset))
            .await?;

        let end_offset = start_offset + size;

        let read_data = ReadData {
            file,
            end_offset,
            is_hard_cache,
            chunk_start_offset,
        };

        Ok(if let Some(download_status) = download_status {
            let dl = DownloadHandle {
                dl_handle: download_status,
            };
            CurrentChunk::Downloading { read_data, _dl: dl }
        } else {
            CurrentChunk::Reading { read_data }
        })
    }

    /// Read data from file, or ignore if we're caching.
    async fn simple_read(
        file: &mut File,
        buf: &mut Option<&mut [u8]>,
        len: usize,
    ) -> Result<usize, CacheHandlerError> {
        Ok(if let Some(buf) = buf {
            file.read(&mut buf[..len]).await?
        } else {
            // Since there's no point reading to nothing, just seek instead
            file.seek(SeekFrom::Current(len as i64)).await?;
            len
        })
    }

    /// Read data from cache file or downloader. Returns bytes read, as well as the current end_offset.
    /// This download read may either:
    /// 1. Extend the end_offset and read from disk (if another thread downloaded)
    /// 2. Read from last_bytes
    /// 3. Grab a new bytes chunk and read from there
    ///
    /// If the receiver is a cache file, just read straight from it.
    async fn downloader_read(
        read_data: &mut ReadData,
        of: &mut OpenFile,
        buf: &mut Option<&mut [u8]>,
        len: usize,
        current_offset: u64,
    ) -> Result<Reader, CacheHandlerError> {
        let (end_offset, chunk_mutex) =
            of.get_download_status(read_data.is_hard_cache, read_data.chunk_start_offset);
        if let Some(chunk) = chunk_mutex {
            let mut chunk = chunk.lock().await;
            if let Some(DownloadStatus {
                receiver,
                last_bytes,
                write_file,
                split_offset,
                last_revision,
            }) = chunk.as_mut()
            {
                // Update local end_offset
                read_data.end_offset = end_offset.load(Ordering::Acquire);
                let res = if current_offset < read_data.end_offset {
                    // If we're not beyond DlStatus's end_offset, just read from disk.
                    // This happens if a different thread downloaded or read from last_bytes.
                    let max_read_len = min(len, (read_data.end_offset - current_offset) as usize);
                    let sr_res = Self::simple_read(&mut read_data.file, buf, max_read_len).await?;
                    Reader::Data(sr_res)
                } else if !last_bytes.is_empty() {
                    // We're beyond end_offset, so if last_bytes is not empty, read from there.
                    Self::read_from_last_bytes(
                        read_data,
                        of,
                        last_bytes,
                        buf,
                        len,
                        write_file,
                        end_offset,
                        split_offset,
                        *last_revision,
                    )
                    .await?
                } else {
                    // We're at end_offset and last_bytes is empty, so download a new last_bytes.
                    match receiver {
                        Receiver::Downloader(downloader) => {
                            let next_chunk: Result<Option<Bytes>, CacheHandlerError> =
                                downloader.next().await.transpose().map_err(|e| e.into());
                            if let Ok(Some(next_chunk)) = next_chunk {
                                if next_chunk.is_empty() {
                                    Reader::NeedsNewChunk
                                } else {
                                    *last_bytes = next_chunk;
                                    Self::read_from_last_bytes(
                                        read_data,
                                        of,
                                        last_bytes,
                                        buf,
                                        len,
                                        write_file,
                                        end_offset,
                                        split_offset,
                                        *last_revision,
                                    )
                                    .await?
                                }
                            } else {
                                Reader::NeedsNewChunk
                            }
                        }
                        Receiver::CacheReader(cache_reader, cache_end_offset) => {
                            // Terribly inefficient way of doing this, causing two allocations.
                            // Luckily this code isn't called very often, but there's definitely a
                            // cleaner way to do this.
                            let mut temp_buf = vec![0u8; 65536];
                            let read_len = cache_reader.read(&mut temp_buf).await?;
                            if read_len == 0 {
                                Reader::NeedsNewChunk
                            } else {
                                let mut next_chunk = BytesMut::with_capacity(65536);
                                next_chunk.extend_from_slice(&temp_buf[..read_len]);
                                *last_bytes = next_chunk.freeze();
                                if let Reader::Data(lb_res) = Self::read_from_last_bytes(
                                    read_data,
                                    of,
                                    last_bytes,
                                    buf,
                                    len,
                                    write_file,
                                    end_offset,
                                    split_offset,
                                    *last_revision,
                                )
                                .await?
                                {
                                    if current_offset + (lb_res as u64) == *cache_end_offset {
                                        Reader::LastData(lb_res)
                                    } else {
                                        Reader::Data(lb_res)
                                    }
                                } else {
                                    Reader::NeedsNewChunk
                                }
                            }
                        }
                    }
                };
                return Ok(res);
            }
        }
        // There is no DlStatus in this chunk, which means we're not downloading.
        // This most likely means the chunk was split, and the DlStatus is in the next chunk.
        let cur_end_offset = end_offset.load(Ordering::Acquire);
        Ok(if cur_end_offset > read_data.end_offset {
            // This chunk has finished downloading, but there's more we can read.
            read_data.end_offset = cur_end_offset;
            Reader::Downgrade
        } else {
            // We need to get a new chunk.
            Reader::NeedsNewChunk
        })
    }

    /// Read data from the last_bytes object in the downloader.
    #[allow(clippy::too_many_arguments)]
    async fn read_from_last_bytes(
        // Read details stored in the OpenFile.
        read_data: &mut ReadData,
        // The OpenFile itself, locked by a mutex.
        of: &mut OpenFile,
        // The last_bytes object to read/split from.
        last_bytes: &mut Bytes,
        // The read_into output_buffer.
        buf: &mut Option<&mut [u8]>,
        // How many bytes to read.
        len: usize,
        // Cache output file.
        write_file: &mut File,
        // The current end offset of the write_file (which gets updated as more gets written).
        end_offset: Arc<AtomicU64>,
        // The offset we're able to write to before we have to split the chunk, either due to
        // MAX_CHUNK_SIZE, or another chunk.
        split_offset: &mut u64,
        last_revision: u64,
    ) -> Result<Reader, CacheHandlerError> {
        let cur_end_offset = end_offset.load(Ordering::Acquire);
        if let Some(so) =
            of.get_split_offset(read_data.is_hard_cache, cur_end_offset, last_revision)
        {
            let chunk_end_offset = read_data.chunk_start_offset + MAX_CHUNK_SIZE;
            *split_offset = min(chunk_end_offset, so);
        }

        if cur_end_offset == *split_offset {
            if cur_end_offset == read_data.chunk_start_offset + MAX_CHUNK_SIZE {
                if let (_, Some(prev_download_status)) =
                    of.get_download_status(read_data.is_hard_cache, read_data.chunk_start_offset)
                {
                    return Ok(Reader::ContinueDownloading(prev_download_status));
                }
            } else {
                return Ok(Reader::NeedsNewChunk);
            }
        }

        let split_read = *split_offset - cur_end_offset;
        let split_len = min(split_read as usize, len);
        let bytes_split_len = min(split_len, last_bytes.len());
        let from_last_bytes = last_bytes.split_to(bytes_split_len);

        // Write data to cache file.
        write_file.write_all(&from_last_bytes).await?;
        write_file.flush().await?;

        // Update end_offset.
        read_data.end_offset += from_last_bytes.len() as u64;
        end_offset.store(read_data.end_offset, Ordering::Release);

        // If reading and not caching, also write data out to buf.
        if let Some(buf) = buf {
            buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
        }

        // Advance our reader file handle.
        read_data
            .file
            .seek(SeekFrom::Current(from_last_bytes.len() as i64))
            .await?;

        // Extend byte ranger to new length, so new threads know about
        // the newly-added data.
        of.update_chunk_size(
            read_data.is_hard_cache,
            read_data.chunk_start_offset,
            read_data.end_offset - read_data.chunk_start_offset,
        )
        .await;

        Ok(Reader::Data(from_last_bytes.len()))
    }

    fn get_file_cache_chunk_path(&self, hard_cache: bool, offset: u64) -> PathBuf {
        if hard_cache {
            &self.hard_cache_file_root
        } else {
            &self.soft_cache_file_root
        }
        .join(format!("chunk_{}", offset))
    }
}

/// Get the hard cache and soft cache paths, given an identifier.
fn get_file_cache_path(cache_root: &Path, data_id: &DataIdentifier) -> PathBuf {
    match data_id {
        DataIdentifier::GlobalMd5(ref md5) => {
            let hex_md5 = hex::encode(md5);
            let dir_1 = hex_md5.get(0..2).unwrap();
            let dir_2 = hex_md5.get(2..4).unwrap();
            cache_root
                .join("global_md5")
                .join(dir_1)
                .join(dir_2)
                .join(hex_md5)
        }
        #[cfg(feature = "sctest")]
        DataIdentifier::None => PathBuf::new(),
    }
}

pub fn get_file_cache_chunk_path(
    cache_root: &Path,
    data_id: &DataIdentifier,
    offset: u64,
) -> PathBuf {
    get_file_cache_path(cache_root, data_id).join(format!("chunk_{}", offset))
}

/// CacheFileHandler uses proptest for testing.
/// 
/// The general strategy is to open one file with three handles, and
/// have each handle perform random seeks, reads, and caches.
/// Occasionally, they may also reopen the file.
/// 
/// When a file is opened, it may be opened to write to hard_cache,
/// or it may be opened "normally" (like if a FUSE mount opened it).
/// 
/// Additionally, when a read occurs, timecode is used to ensure
/// the data was read at the proper offset.
/// 
/// By default, the timecode drive is used for testing and is done
/// locally, however this does not support CryptPassthrough.
/// 
/// In order to test crypt, a timecode.bin file needs to be uploaded
/// to a google drive via an rclone drive+crypt drive, and
/// environment variables at the start of tester_crypt() must be set
/// to point to that file.
/// 
/// Some of the test methods have a `keep_path` variable that's set to
/// false by default. If set to true, the cache directories will be kept
/// which can allow for further debugging.
#[cfg(test)]
mod test {
    use std::env;

    use proptest::{
        arbitrary::any,
        collection,
        prelude::ProptestConfig,
        prop_oneof, proptest,
        strategy::{Just, Strategy},
    };
    use tokio::runtime::Runtime;

    use super::*;
    use crate::{
        cache_handlers::{
            crypt_context::CryptContext, crypt_passthrough::CryptPassthrough,
            filesystem::FilesystemCacheHandler,
        },
        downloaders::{gdrive::GDriveClient, timecode::TimecodeDrive, DownloaderClient},
    };

    const MAX_FILE_SIZE: u64 = 1024u64.pow(2) * 100; // 100MB
    const MAX_OFFSET: u64 = MAX_FILE_SIZE - 11;
    const NUM_ACTIONS: usize = 10; // Number of actions to run per test
    const MAX_READ_LEN: u64 = 65535;
    const MAX_READ_EXACT_LEN: u64 = 65535;
    const MAX_CACHE_LEN: u64 = 65535;

    /// Limit the number of file handles for tests to only three.
    /// Using an enum is probably a bit nicer than generating a random number.
    #[derive(Debug, Clone)]
    enum TestFile {
        File0,
        File1,
        File2,
    }
    use TestFile::*;

    #[derive(Debug, Clone)]
    enum TestAction {
        ReopenAt {
            file: TestFile,
            offset: u64,
            hard_cache: bool,
        },
        ReadData {
            file: TestFile,
            len: u64,
        },
        ReadExact {
            file: TestFile,
            len: u64,
        },
        CacheData {
            file: TestFile,
            len: u64,
        },
        SeekTo {
            file: TestFile,
            offset: u64,
        },
    }

    fn gen_fileid() -> impl Strategy<Value = TestFile> {
        prop_oneof![Just(File0), Just(File1), Just(File2),]
    }

    fn gen_action() -> impl Strategy<Value = TestAction> {
        prop_oneof![
            (gen_fileid(), 0..MAX_OFFSET, any::<bool>()).prop_map(|(f, o, h)| {
                TestAction::ReopenAt {
                    file: f,
                    offset: o,
                    hard_cache: h,
                }
            }),
            (gen_fileid(), 0..MAX_READ_LEN)
                .prop_map(|(f, l)| { TestAction::ReadData { file: f, len: l } }),
            (gen_fileid(), 0..MAX_READ_EXACT_LEN)
                .prop_map(|(f, l)| { TestAction::ReadExact { file: f, len: l } }),
            (gen_fileid(), 0..MAX_CACHE_LEN)
                .prop_map(|(f, l)| { TestAction::CacheData { file: f, len: l } }),
            (gen_fileid(), 0..MAX_OFFSET)
                .prop_map(|(f, o)| { TestAction::SeekTo { file: f, offset: o } }),
        ]
    }

    fn gen_vec_of_test_action() -> impl Strategy<Value = Vec<TestAction>> {
        collection::vec(gen_action(), NUM_ACTIONS)
    }

    fn gen_init_offsets() -> impl Strategy<Value = Vec<u64>> {
        collection::vec(0..MAX_OFFSET, 3)
    }

    fn gen_init_hard_caches() -> impl Strategy<Value = Vec<bool>> {
        collection::vec(any::<bool>(), 3)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            max_shrink_iters: 0,
            fork: false,
            .. ProptestConfig::default()
        })]
        #[test]
        fn prop_test_actions(init_offsets in gen_init_offsets(), init_hard_caches in gen_init_hard_caches(), actions in gen_vec_of_test_action()) {
            Runtime::new().unwrap().block_on(tester(&init_offsets, &init_hard_caches, actions))
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            max_shrink_iters: 0,
            fork: false,
            .. ProptestConfig::default()
        })]
        #[test]
        fn prop_test_crypt_actions(init_offsets in gen_init_offsets(), init_hard_caches in gen_init_hard_caches(), actions in gen_vec_of_test_action()) {
            Runtime::new().unwrap().block_on(tester_crypt(&init_offsets, &init_hard_caches, actions))
        }
    }

    async fn tester(init_offsets: &[u64], init_hard_cache: &[bool], actions: Vec<TestAction>) {
        let d = Box::new(TimecodeDrive {
            root_name: "test".to_string(),
        });

        let file_id = r#"{"bytes_len": 65535}"#;
        let data_id = DataIdentifier::GlobalMd5(vec![0, 0, 0, 0]);
        let file_size = MAX_FILE_SIZE;

        tester_inner(
            init_offsets,
            init_hard_cache,
            actions,
            d,
            file_id,
            data_id,
            file_size,
            None,
        )
        .await
    }

    async fn tester_crypt(
        init_offsets: &[u64],
        init_hard_cache: &[bool],
        actions: Vec<TestAction>,
    ) {
        if env::var("TEST_CRYPT").is_err() {
            println!("TEST_CRYPT environment variable not test, skipping.");
            return
        }
        let client_id = env::var("TEST_CLIENT_ID").unwrap();
        let client_secret = env::var("TEST_CLIENT_SECRET").unwrap();
        let refresh_token = env::var("TEST_REFRESH_TOKEN").unwrap();
        let drive_id = env::var("TEST_DRIVE_ID").unwrap();
        let file_id = env::var("TEST_FILE_ID").unwrap();
        let data_id_hex = env::var("TEST_DATA_ID").unwrap();
        let password1 = env::var("TEST_PASSWORD").unwrap();
        let password2 = env::var("TEST_PASSWORD2").unwrap();
        let data_id = DataIdentifier::GlobalMd5(hex::decode(data_id_hex).unwrap());
        let file_size = 1073741824;

        let crypt_context = CryptContext::new(&password1, &password2).unwrap();

        let c = GDriveClient::new(&client_id, &client_secret, &refresh_token, &[])
            .await
            .unwrap();

        let d = c.open_drive(&drive_id);

        tester_inner(
            init_offsets,
            init_hard_cache,
            actions,
            d,
            &file_id,
            data_id,
            file_size,
            Some(crypt_context),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn tester_inner(
        init_offsets: &[u64],
        init_hard_cache: &[bool],
        actions: Vec<TestAction>,
        d: Box<dyn DownloaderDrive>,
        file_id: &str,
        data_id: DataIdentifier,
        file_size: u64,
        crypt_context: Option<CryptContext>,
    ) {
        let keep_path = false;
        let test_path = tempfile::tempdir().unwrap();
        let (hard_cache_root, soft_cache_root) = if keep_path {
            let test_path = test_path.into_path();
            (test_path.join("hard_cache"), test_path.join("soft_cache"))
        } else {
            (
                test_path.path().join("hard_cache"),
                test_path.path().join("soft_cache"),
            )
        };

        let fsch = FilesystemCacheHandler::new(
            "main",
            None,
            &hard_cache_root,
            &soft_cache_root,
            Arc::from(d),
        );

        let mut offsets = init_offsets.to_vec();
        let mut files = vec![];
        for f in 0..3 {
            if let Some(ctx) = &crypt_context {
                let reader = fsch
                    .open_file(
                        file_id.to_owned(),
                        data_id.clone(),
                        file_size,
                        0,
                        init_hard_cache[f],
                    )
                    .await
                    .unwrap();
                let file: Box<dyn CacheFileHandler> = Box::new(
                    CryptPassthrough::new(ctx, offsets[f], reader)
                        .await
                        .unwrap(),
                );
                files.push(file)
            } else {
                files.push(
                    fsch.open_file(
                        file_id.to_owned(),
                        data_id.clone(),
                        file_size,
                        offsets[f],
                        init_hard_cache[f],
                    )
                    .await
                    .unwrap(),
                )
            }
        }

        dbg!(&init_offsets);
        dbg!(&init_hard_cache);

        println!("BEGIN TEST");
        for action in actions {
            println!("\n\n\n\n\n\nACTION: {:?}", action);
            match action {
                TestAction::ReopenAt {
                    file,
                    offset,
                    hard_cache,
                } => {
                    let f = match file {
                        File0 => 0,
                        File1 => 1,
                        File2 => 2,
                    };
                    files[f] = if let Some(ctx) = &crypt_context {
                        let reader = fsch
                            .open_file(
                                file_id.to_owned(),
                                data_id.clone(),
                                file_size,
                                0,
                                hard_cache,
                            )
                            .await
                            .unwrap();

                        Box::new(CryptPassthrough::new(ctx, offset, reader).await.unwrap())
                    } else {
                        fsch.open_file(
                            file_id.to_owned(),
                            data_id.clone(),
                            file_size,
                            offset,
                            hard_cache,
                        )
                        .await
                        .unwrap()
                    };
                    offsets[f] = offset;
                }
                TestAction::ReadData { file, len } => {
                    let f = match file {
                        File0 => 0,
                        File1 => 1,
                        File2 => 2,
                    };
                    let mut out = vec![0u8; len as usize];
                    let out_len = files[f].read_into(&mut out).await.unwrap();
                    if out_len >= 11 {
                        assert!(timecode_rs::validate_offset(offsets[f], &out[..out_len]))
                    }
                    offsets[f] += out_len as u64;
                }
                TestAction::ReadExact { file, len } => {
                    let f = match file {
                        File0 => 0,
                        File1 => 1,
                        File2 => 2,
                    };
                    let mut out = vec![0u8; len as usize];
                    let out_len = files[f].read_exact(&mut out).await.unwrap();
                    if out_len >= 11 {
                        assert!(timecode_rs::validate_offset(offsets[f], &out[..out_len]))
                    }
                    offsets[f] += out_len as u64;
                }
                TestAction::CacheData { file, len } => {
                    let f = match file {
                        File0 => 0,
                        File1 => 1,
                        File2 => 2,
                    };
                    let out_len = files[f].cache_data(len as usize).await.unwrap();
                    offsets[f] += out_len as u64;
                }
                TestAction::SeekTo { file, offset } => {
                    let f = match file {
                        File0 => 0,
                        File1 => 1,
                        File2 => 2,
                    };
                    files[f].seek_to(offset).await.unwrap();
                    offsets[f] = offset;
                }
            }
        }
        println!("END TEST");
    }

    /// Test cache chunk splitting with download
    #[tokio::test]
    async fn test_splitting() {
        let d = Arc::new(TimecodeDrive {
            root_name: "test".to_string(),
        });

        let file_id = r#"{"bytes_len": 65535}"#;
        let data_id = DataIdentifier::GlobalMd5(vec![0, 0, 0, 0]);
        let file_size = 1024u64.pow(3) * 10; // 10 GB

        let keep_path = false;
        let test_path = tempfile::tempdir().unwrap();
        let (hard_cache_root, soft_cache_root) = if keep_path {
            let test_path = test_path.into_path();
            println!("Path: {}", test_path.display());
            (test_path.join("hard_cache"), test_path.join("soft_cache"))
        } else {
            (
                test_path.path().join("hard_cache"),
                test_path.path().join("soft_cache"),
            )
        };

        let fsch = FilesystemCacheHandler::new("main", None, &hard_cache_root, &soft_cache_root, d);

        let mut f = fsch
            .open_file(file_id.to_owned(), data_id.clone(), file_size, 0, false)
            .await
            .unwrap();

        f.cache_exact(MAX_CHUNK_SIZE as usize * 3).await.unwrap();
    }

    /// Test that a download thread is dropped when a file handle
    /// is closed
    #[tokio::test]
    async fn test_dl_drop() {
        let d = Arc::new(TimecodeDrive {
            root_name: "test".to_string(),
        });

        let file_id = r#"{"bytes_len": 65535}"#;
        let data_id = DataIdentifier::GlobalMd5(vec![0, 0, 0, 0]);
        let file_size = 1024u64.pow(3) * 10; // 10 GB

        let keep_path = false;
        let test_path = tempfile::tempdir().unwrap();
        let (hard_cache_root, soft_cache_root) = if keep_path {
            let test_path = test_path.into_path();
            println!("Path: {}", test_path.display());
            (test_path.join("hard_cache"), test_path.join("soft_cache"))
        } else {
            (
                test_path.path().join("hard_cache"),
                test_path.path().join("soft_cache"),
            )
        };

        let fsch = FilesystemCacheHandler::new("main", None, &hard_cache_root, &soft_cache_root, d);

        {
            let mut f = fsch
                .open_file(file_id.to_owned(), data_id.clone(), file_size, 0, false)
                .await
                .unwrap();

            f.cache_exact(100).await.unwrap();
            f.cache_exact(50).await.unwrap();

            f.seek_to(200).await.unwrap();
            f.cache_exact(100).await.unwrap();

            {
                let mut f2 = fsch
                    .open_file(file_id.to_owned(), data_id.clone(), file_size, 300, false)
                    .await
                    .unwrap();

                f2.cache_exact(200).await.unwrap();
                f2.cache_exact(200).await.unwrap();
            }
        }
    }
}
