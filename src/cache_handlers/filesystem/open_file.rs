use bytes::Bytes;
use futures::Stream;
use std::{io::SeekFrom, path::Path, sync::Arc};
use tokio::{
    fs::DirEntry,
    fs::{read_dir, File, OpenOptions},
    stream::StreamExt,
    sync::broadcast,
};

use crate::{
    cache_handlers::CacheHandlerError,
    downloaders::{DownloaderDrive, DownloaderError},
    types::DataIdentifier,
};
use byte_ranger::{ByteRanger, GetRange};

use super::get_file_cache_path;

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

    // Use this to start a new chunk by downloading it.
    pub async fn start_download_chunk(
        &mut self,
        offset: u64,
        write_hard_cache: bool,
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("START DL");
        let downloader = self
            .downloader_drive
            .open_file(self.file_id.clone(), offset, write_hard_cache)
            .await
            .unwrap();
        // TODO: eventually change this to something that doesn't require a sender to create new receivers
        let (progress_channel, _) = broadcast::channel(1);
        let download_handle = DownloadHandle {
            progress_channel: progress_channel.clone(),
            downloader,
        };

        let download_status = DownloadStatus {
            current_offset: offset,
            progress_channel,
        };

        self.chunks.add_range(
            offset,
            0,
            ChunkData {
                is_hard_cache: write_hard_cache,
                start_offset: offset,
                download_status: Some(Arc::new(download_status)),
            },
        );

        // dbg!(file_path.parent());
        let parent = file_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = File::create(file_path).await?;

        Ok((file, download_handle))
    }

    /// Use this to write to an existing chunk.
    /// Errors out if existing chunk is the opposite cache,
    /// e.g. when writing to hard cache, the chunk is located in the soft cache.
    pub async fn append_download_chunk(
        &mut self,
        start_offset: u64,
        current_offset: u64,
        write_hard_cache: bool,
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("APPEND DL");

        if let Some(mut cache_data) = self.chunks.get_data_mut(start_offset) {
            if cache_data.is_hard_cache != write_hard_cache {
                return Err(CacheHandlerError::CacheMismatchError{ write_hard_cache, is_hard_cache: cache_data.is_hard_cache, start_offset });
            }

            let downloader = self
                .downloader_drive
                .open_file(self.file_id.clone(), start_offset, write_hard_cache)
                .await
                .unwrap();
            let (progress_channel, _) = broadcast::channel(1);
            let download_handle = DownloadHandle {
                progress_channel: progress_channel.clone(),
                downloader
            };

            let download_status = Arc::new(DownloadStatus {
                current_offset,
                progress_channel,
            });

            cache_data.download_status = Some(download_status);

            let mut file = OpenOptions::new().append(true).open(file_path).await?;
            file.seek(SeekFrom::Start(current_offset - start_offset))
                .await
                .unwrap();

            Ok((file, download_handle))
        } else {
            Err(CacheHandlerError::AppendChunkError { start_offset })
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

    pub async fn update_chunk_size(&mut self, start_offset: u64, new_size: u64) {
        println!("UPDATE CHUNK {} {}", start_offset, new_size);
        self.chunks.extend_range(start_offset, new_size);
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
                                start_offset: offset,
                                download_status: None,
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
    pub start_offset: u64,
    /// Status, if a chunk is in the middle of downloading.
    pub download_status: Option<Arc<DownloadStatus>>,
}

#[derive(Debug)]
/// Structure describing the current download status of a chunk.
pub struct DownloadStatus {
    /// Offset of data that has been flushed to disk. Anything before
    /// this offset can be safely read, before waiting on progress_channel
    /// is required.
    current_offset: u64,
    /// When waiting for new data to show up in a chunk, readers can use
    /// this
    progress_channel: broadcast::Sender<u64>,
}

/// When a download is started, give this handle to the reader.
pub struct DownloadHandle {
    progress_channel: broadcast::Sender<u64>,
    downloader: Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Unpin>,
}

impl DownloadHandle {
    pub async fn get_next_bytes(&mut self) -> Result<Option<Bytes>, CacheHandlerError> {
        self.downloader
            .next()
            .await
            .transpose()
            .map_err(|e| e.into())
    }
}
