use bytes::Bytes;
use futures::Stream;
use std::{fmt, io::SeekFrom, path::Path, sync::Arc};
use tokio::{fs::DirEntry, fs::{read_dir, File, OpenOptions}, stream::StreamExt, sync::Mutex};

use crate::{
    cache_handlers::CacheHandlerError,
    downloaders::{DownloaderDrive, DownloaderError},
    types::DataIdentifier,
};
use byte_ranger::{ByteRanger, GetRange};

use super::{MAX_CHUNK_SIZE, get_file_cache_path};

/// OpenFile is a struct representing a single open file within the cache. It is a requirement
/// that, for any file, only one of these structs exist and that it is protected by a mutex.
/// This includes scanning a file to populate the hard cache.
///
/// When reading a file, the reader must first check the hard cache, then the soft cache, and if
/// neither cache contains data, it must download the data and simultaneously stream it to the
/// reader, and to the filesystem. The write_hard_cache flag will specify whether downloaded data
/// gets written to the hard cache or soft cache.
///
/// The caches are made up of chunk files, named by their offsets in the original file. If a chunk
/// file that the reader needs does not exist, it will download the data needed until either the
/// reader closes or the chunk file reaches 100MB, at which point a new chunk file will be created
/// and the download will continue.
///
/// When a reader reaches the end of a chunk, it must download new data and append to the chunk
/// until it becomes 100MB, or it reaches the beginning of the next chunk, whichever comes first.
///
/// When an appended download occurs, the downloader must update the structure to keep track of how
/// much data has been written to disk. This is in order to ensure other readers can read data chunks
/// without having to re-lock the mutex on each read operation.
pub struct OpenFile {
    file_id: String,
    downloader_drive: Arc<dyn DownloaderDrive>,
    pub chunks: ByteRanger<ChunkData>,
}

impl OpenFile {
    pub async fn new(
        hard_cache_root: &Path,
        soft_cache_root: &Path,
        file_id: &str,
        data_id: &DataIdentifier,
        downloader_drive: Arc<dyn DownloaderDrive>,
    ) -> OpenFile {
        // Scan for existing cache files
        let chunks = Self::get_cache_files(&[(hard_cache_root, true), (soft_cache_root, false)], data_id).await;

        let file_id = file_id.to_string();

        dbg!(&chunks);
        OpenFile {
            file_id,
            downloader_drive,
            chunks,
        }
    }

    pub async fn get_chunk_at<'a>(
        &'a self,
        offset: u64,
    ) -> GetRange<&'a ChunkData> {
        self.chunks.get_range_at(offset)
    }

    /// Use this to start a new chunk by downloading it.
    ///
    /// offset: Where to start the new download chunk.
    /// 
    /// write_hard_cache: Whether or not this is writing to the hard cache.
    /// 
    /// file_path: Path to write file.
    pub async fn start_download_chunk(
        &mut self,
        offset: u64,
        write_hard_cache: bool,
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("START DL");

        let parent = file_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let write_file = File::create(file_path).await?;
        
        let downloader = self
            .downloader_drive
            .open_file(self.file_id.clone(), offset, write_hard_cache)
            .await
            .unwrap();

        let end_offset = offset;
        let split_offset = offset + MAX_CHUNK_SIZE;

        let dl_handle = Arc::new(());

        let download_status = DownloadStatus::new(downloader, write_file, end_offset, split_offset, dl_handle.clone());

        self.chunks.add_range(
            offset,
            0,
            ChunkData {
                is_hard_cache: write_hard_cache,
                chunk_start_offset: offset,
                download_status: Arc::new(Mutex::new(Some(download_status))),
            },
        );

        let read_file = File::open(file_path).await?;
        let download_handle = DownloadHandle { dl_handle };

        Ok((read_file, download_handle))
    }

    /// Use this to write to an existing chunk.
    /// Errors out if existing chunk is the opposite cache,
    /// e.g. when writing to hard cache, the chunk is located in the soft cache.
    ///
    /// start_offset: Where the current chunk starts, i.e. listed in the chunk file name.
    ///
    /// end_offset: The file offset at the end of the chunk. This is where we start appending.
    ///
    /// file_path: Path to write file.
    pub async fn append_download_chunk(
        &mut self,
        chunk_start_offset: u64,
        end_offset: u64,
        write_hard_cache: bool,
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("APPEND DL");

        // Check that the chunk actually exists, and is in the correct cache
        if let Some(mut cache_data) = self.chunks.get_data_mut(chunk_start_offset) {
            if cache_data.is_hard_cache != write_hard_cache {
                return Err(CacheHandlerError::CacheMismatchError{ write_hard_cache, is_hard_cache: cache_data.is_hard_cache, start_offset: chunk_start_offset });
            }

            let mut write_file = OpenOptions::new().append(true).open(file_path).await?;
            // Seek to proper offset, relative to chunk's start offset.
            write_file.seek(SeekFrom::Start(end_offset - chunk_start_offset))
                    .await
                    .unwrap();

            // Create a downloader
            let downloader = self
                .downloader_drive
                .open_file(self.file_id.clone(), chunk_start_offset, write_hard_cache)
                .await
                .unwrap();

            
            let split_offset = chunk_start_offset + MAX_CHUNK_SIZE;

            let dl_handle = Arc::new(());

            let download_status = DownloadStatus::new(downloader, write_file, end_offset, split_offset, dl_handle.clone());

            cache_data.download_status = Arc::new(Mutex::new(Some(download_status)));

            let read_file = File::open(file_path).await?;
            let download_handle = DownloadHandle { dl_handle };

            Ok((read_file, download_handle))
        } else {
            Err(CacheHandlerError::AppendChunkError { start_offset: chunk_start_offset })
        }
    }

    /// Use this to copy from soft_cache to hard_cache. Creates a new hard_cache chunk.
    pub async fn new_copy_chunk(&mut self, file_path: &Path) -> Result<File, CacheHandlerError> {
        println!("COPY CHUNK");

        // dbg!(file_path.parent());
        let parent = file_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = File::create(file_path).await?;

        Ok(file)
    }

    /// Set a new chunk size as the chunk gets downloaded.
    pub fn update_chunk_size(&mut self, start_offset: u64, new_size: u64) {
        println!("UPDATE CHUNK {} {}", start_offset, new_size);
        self.chunks.extend_range(start_offset, new_size);
    }

    /// Get the DownloadStatus of a particular chunk.
    pub fn get_download_status(&self, chunk_start_offset: u64) -> Arc<Mutex<Option<DownloadStatus>>> {
        let chunk = self.chunks.get_data(chunk_start_offset).unwrap();
        chunk.download_status.clone()
    }

    /// Get all cache files in a list of directories.
    /// Earlier cache roots get priority over later cache roots.
    async fn get_cache_files(
        cache_roots: &[(&Path, bool)],
        data_id: &DataIdentifier,
    ) -> ByteRanger<ChunkData> {
        let mut br = ByteRanger::new();

        for (cache_root, is_hard_cache) in cache_roots {
            let cache_path = get_file_cache_path(cache_root, data_id);
            if cache_path.exists() {
                let mut dirs = read_dir(cache_path).await.unwrap();
                while let Some(dir_entry) = dirs.next().await {
                    if let Some((offset, len)) = Self::direntry_to_namelen(dir_entry.unwrap()).await {
                        println!("add_range {} {}", offset, len);
                        if len == 0 {
                            // Ignore zero-length chunks
                            continue;
                        }
                        br.add_range(
                            offset,
                            len,
                            ChunkData {
                                is_hard_cache: *is_hard_cache,
                                chunk_start_offset: offset,
                                download_status: Arc::new(Mutex::new(None)),
                            },
                        );
                    }
                }
            }
        }
        
        br
    }

    /// Converts a Tokio DirEntry to a two-item tuple.
    /// The first item identifies the cache file's offset in the original file.
    /// The second item identifies the length of this cache file.
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
}

#[derive(Clone, Debug)]
pub struct ChunkData {
    /// Flag if this chunk is in hard_cache.
    pub is_hard_cache: bool,
    /// Start offset of this chunk.
    /// Used as an index to this chunk download status.
    pub chunk_start_offset: u64,
    /// Status, if a chunk is in the middle of downloading.
    // The Mutex is needed here due to DownloadStatus not being clone-able.
    // The Option is inner because we want the same data between byte-ranger range splits.
    pub download_status: Arc<Mutex<Option<DownloadStatus>>>,
}

/// Structure describing the current download status of a chunk.
pub struct DownloadStatus {
    /// Downloader.
    pub downloader: Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Sync + Unpin>,
    /// Last received bytes from downloader.
    pub last_bytes: Bytes,
    /// File handle used to write to disk.
    pub write_file: File,
    /// How far we've written downloaded bytes to disk.
    pub end_offset: u64,
    /// How far we can write before we need to split the chunk file into a new one.
    pub split_offset: u64,
    /// Reference count of how many threads are using this downloader.
    pub dl_handle: Arc<()>,
}

impl fmt::Debug for DownloadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: fix to have more info
        write!(f, "DownloadStatus(end_offset: {}, split_offset: {})",
            self.end_offset,
            self.split_offset
        )
    }
}

impl DownloadStatus {
    pub fn new(downloader: Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Sync + Unpin>, write_file: File, end_offset: u64, split_offset: u64, dl_handle: Arc<()>) -> DownloadStatus {
        let last_bytes = Bytes::new();
        DownloadStatus { 
            downloader,
            last_bytes,
            write_file,
            end_offset,
            split_offset,
            dl_handle,
        }
    }
}

/// Handle that holds Arc pointer to nothing.
/// Used to close downloaders when all threads are finished.
pub struct DownloadHandle {
    dl_handle: Arc<()>,
}

// /// When a download is started, give this handle to the reader.
// pub struct DownloadHandle {
//     progress_channel: broadcast::Sender<u64>,
//     downloader: Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Unpin>,
// }

// impl DownloadHandle {
//     pub async fn get_next_bytes(&mut self) -> Result<Option<Bytes>, CacheHandlerError> {
//         self.downloader
//             .next()
//             .await
//             .transpose()
//             .map_err(|e| e.into())
//     }
// }
