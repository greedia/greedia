use std::{
    cmp::min,
    fmt,
    io::SeekFrom,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
};

use byte_ranger::{ByteRanger, GetRange};
use bytes::Bytes;
use camino::Utf8Path;
use futures::Stream;
use tokio::{
    fs::{read_dir, DirEntry, File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
    task,
};
use tracing::trace;

use super::{get_file_cache_path, lru::Lru, MAX_CHUNK_SIZE};
use crate::{
    cache_handlers::CacheHandlerError,
    db::types::DataIdentifier,
    downloaders::{DownloaderDrive, DownloaderError},
};

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
/// reader closes, the next chunk is reached, or the chunk file reaches 100MB, at which point a
/// new chunk file will be created and the download will continue.
///
/// When a reader reaches the end of a chunk, it must download new data and append to the chunk
/// until it becomes 100MB, or it reaches the beginning of the next chunk, whichever comes first.
///
/// When an appended download occurs, the downloader must update the structure to keep track of how
/// much data has been written to disk. This is in order to ensure other readers can read data chunks
/// directly from the filesystem without having to re-lock the mutex on each read operation.
pub struct OpenFile {
    /// ID used within the downloader.
    file_id: String,
    /// Data identifier used on-disk.
    data_id: DataIdentifier,
    /// Downloader this file is attached to.
    downloader_drive: Arc<dyn DownloaderDrive>,
    /// List of chunks in the hard cache.
    hc_chunks: ByteRanger<ChunkData>,
    /// List of chunks in the soft cache.
    sc_chunks: ByteRanger<ChunkData>,
    /// Handle to the soft_cache LRU handler.
    lru: Option<Lru>,
    /// Number of changes made to either cache. This is so downloaders don't need to repeatedly check
    /// the hard and soft cache for changes if none have been made.
    revision: u64,
}

impl OpenFile {
    pub async fn new(
        lru: Option<Lru>,
        hard_cache_root: &Utf8Path,
        soft_cache_root: &Utf8Path,
        file_id: &str,
        data_id: &DataIdentifier,
        downloader_drive: Arc<dyn DownloaderDrive>,
    ) -> OpenFile {
        // Scan for existing cache files
        let hc_chunks = Self::get_cache_files(hard_cache_root, data_id).await;
        let sc_chunks = Self::get_cache_files(soft_cache_root, data_id).await;

        let file_id = file_id.to_string();
        let revision = 0;

        if let Some(lru) = &lru {
            lru.open_file(data_id).await;
        }

        OpenFile {
            file_id,
            data_id: data_id.clone(),
            downloader_drive,
            hc_chunks,
            sc_chunks,
            lru,
            revision,
        }
    }

    /// Get the range or gap at a specific offset.
    ///
    /// cache: Which cache to get chunk from. If Any, check hard_cache then soft_cache.
    ///
    /// Returns chunk data, as well as a bool that's true if the chunk is in hard_cache, or false if in soft_cache.
    pub fn get_chunk_at(&self, offset: u64, cache: Cache) -> (GetRange<&'_ ChunkData>, bool) {
        match cache {
            Cache::Hard => (self.hc_chunks.get_range_at(offset), true),
            Cache::Any => {
                let hc_chunk = self.hc_chunks.get_range_at(offset);
                if let GetRange::Gap {
                    start_offset: _,
                    size: _,
                    prev_range: _,
                }
                | GetRange::PastFinalRange {
                    start_offset: _,
                    size: _,
                    data: _,
                }
                | GetRange::Empty = hc_chunk
                {
                    (self.sc_chunks.get_range_at(offset), false)
                } else {
                    (hc_chunk, true)
                }
            }
        }
    }

    /// Find the offset of the next chunk.
    ///
    /// If Cache::Any is used, find the smallest next offset.
    pub fn get_next_chunk(&self, offset: u64, cache: Cache) -> Option<u64> {
        match cache {
            Cache::Hard => {
                if let Some(GetRange::Data {
                    start_offset,
                    size: _,
                    data: _,
                }) = self.hc_chunks.get_next_range(offset)
                {
                    Some(start_offset)
                } else {
                    None
                }
            }
            Cache::Any => {
                let hc = if let Some(GetRange::Data {
                    start_offset,
                    size: _,
                    data: _,
                }) = self.hc_chunks.get_next_range(offset)
                {
                    Some(start_offset)
                } else {
                    None
                };
                let sc = if let Some(GetRange::Data {
                    start_offset,
                    size: _,
                    data: _,
                }) = self.sc_chunks.get_next_range(offset)
                {
                    Some(start_offset)
                } else {
                    None
                };
                match (hc, sc) {
                    (None, None) => None,
                    (None, Some(offset)) | (Some(offset), None) => Some(offset),
                    (Some(hc_offset), Some(sc_offset)) => Some(min(hc_offset, sc_offset)),
                }
            }
        }
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
        file_path: &Utf8Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        let next_chunk = self.get_next_chunk(
            offset,
            if write_hard_cache {
                Cache::Hard
            } else {
                Cache::Any
            },
        );

        // Stop downloading at MAX_CHUNK_SIZE or next chunk offset, whichever comes first
        let split_offset = if let Some(next_chunk) = next_chunk {
            min(next_chunk, offset + MAX_CHUNK_SIZE)
        } else {
            offset + MAX_CHUNK_SIZE
        };
        let end_offset = Arc::new(AtomicU64::new(offset));

        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        let mut write_file = File::create(file_path).await?;
        write_file.flush().await?;
        if let Some(lru) = &self.lru {
            if !write_hard_cache {
                lru.touch_file(&self.data_id, offset).await;
            }
        }

        let downloader = self
            .downloader_drive
            .open_file(self.file_id.clone(), offset, write_hard_cache)
            .await?;

        let receiver = Receiver::Downloader(downloader);

        self.revision += 1;
        let download_status = Arc::new(Mutex::new(Some(DownloadStatus::new(
            receiver,
            write_file,
            split_offset,
            self.revision,
        ))));

        if write_hard_cache {
            &mut self.hc_chunks
        } else {
            &mut self.sc_chunks
        }
        .add_range(
            offset,
            0,
            ChunkData {
                chunk_start_offset: offset,
                end_offset,
                download_status: Arc::downgrade(&download_status),
            },
        );

        // let read_file = File::open(file_path).await?;
        let read_file = File::open(file_path).await?;
        let download_handle = DownloadHandle {
            dl_handle: download_status,
        };

        Ok((read_file, download_handle))
    }

    /// Use this to start a new chunk by stealing the downloader from the previous chunk, if possible.
    /// If previous chunk is not adjacent or has no downloader, start a new download instead.
    ///
    /// offset: Where to start the new download chunk.
    ///
    /// write_hard_cache: Whether or not this is writing to the hard cache.
    ///
    /// file_path: Path to write file.
    pub async fn start_transfer_chunk(
        &mut self,
        offset: u64,
        prev_download_status: DownloadStatus,
        write_hard_cache: bool,
        file_path: &Utf8Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        let next_chunk = self.get_next_chunk(
            offset,
            if write_hard_cache {
                Cache::Hard
            } else {
                Cache::Any
            },
        );

        // Stop downloading at MAX_CHUNK_SIZE or next chunk offset, whichever comes first
        let split_offset = if let Some(next_chunk) = next_chunk {
            min(next_chunk, offset + MAX_CHUNK_SIZE)
        } else {
            offset + MAX_CHUNK_SIZE
        };
        let end_offset = Arc::new(AtomicU64::new(offset));

        let parent = file_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut write_file = File::create(file_path).await?;
        write_file.flush().await?;
        if let Some(lru) = &self.lru {
            if !write_hard_cache {
                lru.touch_file(&self.data_id, offset).await;
            }
        }

        self.revision += 1;

        let download_status = Arc::new(Mutex::new(Some(DownloadStatus::chain_from_prev(
            prev_download_status,
            write_file,
            split_offset,
            self.revision,
        ))));

        if write_hard_cache {
            &mut self.hc_chunks
        } else {
            &mut self.sc_chunks
        }
        .add_range(
            offset,
            0,
            ChunkData {
                chunk_start_offset: offset,
                end_offset,
                download_status: Arc::downgrade(&download_status),
            },
        );

        let read_file = File::open(file_path).await?;

        let download_handle = DownloadHandle {
            dl_handle: download_status,
        };

        Ok((read_file, download_handle))
    }

    /// Use this to write to an existing chunk.
    /// Errors out if existing chunk is the opposite cache,
    /// e.g. when writing to hard cache, the chunk is located in the soft cache.
    ///
    /// chunk_start_offset: Where the previous chunk starts, i.e. listed in the chunk file name.
    ///
    /// start_offset: The file offset at the end of the previous chunk. This is where we start appending.
    ///
    /// file_path: Path to write file.
    pub async fn append_download_chunk(
        &mut self,
        chunk_start_offset: u64,
        start_offset: u64,
        write_hard_cache: bool,
        file_path: &Utf8Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        trace!(
            "append_download_chunk {} {}",
            cso = chunk_start_offset,
            so = start_offset
        );
        let next_chunk = self.get_next_chunk(
            start_offset,
            if write_hard_cache {
                Cache::Hard
            } else {
                Cache::Any
            },
        );

        // Stop downloading at MAX_CHUNK_SIZE or next chunk offset, whichever comes first
        let split_offset = if let Some(next_chunk) = next_chunk {
            min(next_chunk, chunk_start_offset + MAX_CHUNK_SIZE)
        } else {
            chunk_start_offset + MAX_CHUNK_SIZE
        };

        let chunks = if write_hard_cache {
            &mut self.hc_chunks
        } else {
            &mut self.sc_chunks
        };

        // Check that the chunk actually exists
        if let Some(mut cache_data) = chunks.get_data_mut(chunk_start_offset) {
            let mut write_file = OpenOptions::new().append(true).open(file_path).await?;
            if let Some(lru) = &self.lru {
                if !write_hard_cache {
                    lru.touch_file(&self.data_id, chunk_start_offset).await;
                }
            }

            // Seek to proper offset, relative to chunk's start offset.
            write_file.seek(SeekFrom::End(0)).await?;

            // Create a downloader
            let downloader = self
                .downloader_drive
                .open_file(self.file_id.clone(), start_offset, write_hard_cache)
                .await?;

            let receiver = Receiver::Downloader(downloader);

            let download_status = Arc::new(Mutex::new(Some(DownloadStatus::new(
                receiver,
                write_file,
                split_offset,
                self.revision,
            ))));

            assert_eq!(start_offset, cache_data.end_offset.load(Ordering::Acquire));
            cache_data.download_status = Arc::downgrade(&download_status);

            let mut read_file = File::open(file_path).await?;

            read_file.seek(SeekFrom::End(0)).await?;

            let download_handle = DownloadHandle {
                dl_handle: download_status,
            };

            Ok((read_file, download_handle))
        } else {
            Err(CacheHandlerError::AppendChunk {
                start_offset: chunk_start_offset,
            })
        }
    }

    /// Use this to copy from soft_cache to hard_cache. Creates a new hard_cache chunk.
    pub async fn start_copy_chunk(
        &mut self,
        offset: u64,
        source_file_path: &Utf8Path,
        source_file_offset: u64,
        source_file_end_offset: u64,
        file_path: &Utf8Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        let next_chunk = self.get_next_chunk(offset, Cache::Hard);

        // Stop downloading at MAX_CHUNK_SIZE or next chunk offset, whichever comes first
        let split_offset = if let Some(next_chunk) = next_chunk {
            min(next_chunk, offset + MAX_CHUNK_SIZE)
        } else {
            offset + MAX_CHUNK_SIZE
        };

        let parent = file_path.parent().unwrap();
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut write_file = File::create(file_path).await?;
        write_file.flush().await?;

        let mut cache_file = File::open(source_file_path).await?;

        cache_file.seek(SeekFrom::Start(source_file_offset)).await?;

        let receiver = Receiver::CacheReader(cache_file, source_file_end_offset);

        let end_offset = Arc::new(AtomicU64::new(offset));

        self.revision += 1;
        let download_status = Arc::new(Mutex::new(Some(DownloadStatus::new(
            receiver,
            write_file,
            split_offset,
            self.revision,
        ))));

        self.hc_chunks.add_range(
            offset,
            0,
            ChunkData {
                chunk_start_offset: offset,
                end_offset,
                download_status: Arc::downgrade(&download_status),
            },
        );

        let read_file = File::open(file_path).await?;
        let download_handle = DownloadHandle {
            dl_handle: download_status,
        };

        Ok((read_file, download_handle))
    }

    /// Set a new chunk size as the chunk gets downloaded.
    pub async fn update_chunk_size(&mut self, hard_cache: bool, start_offset: u64, new_size: u64) {
        let old_size = if hard_cache {
            &mut self.hc_chunks
        } else {
            &mut self.sc_chunks
        }
        .extend_range(start_offset, new_size);
        self.revision += 1;

        if let Some(lru) = &self.lru {
            if !hard_cache {
                lru.add_space_usage(new_size.saturating_sub(old_size.unwrap_or(0)))
                    .await;
            }
        }
    }

    /// Get the end_offset and DownloadStatus of a particular chunk.
    #[allow(clippy::type_complexity)]
    pub fn get_download_status(
        &self,
        hard_cache: bool,
        chunk_start_offset: u64,
    ) -> (Arc<AtomicU64>, Option<Arc<Mutex<Option<DownloadStatus>>>>) {
        let chunk = if hard_cache {
            &self.hc_chunks
        } else {
            &self.sc_chunks
        }
        .get_data(chunk_start_offset)
        .unwrap();

        (chunk.end_offset.clone(), chunk.download_status.upgrade())
    }

    /// Get a new split_offset value, if last_revision does not match OpenFile's revision.
    pub fn get_split_offset(
        &mut self,
        hard_cache: bool,
        start_offset: u64,
        last_revision: u64,
    ) -> Option<u64> {
        if last_revision < self.revision {
            if let Some(next_chunk) = self.get_next_chunk(
                start_offset,
                if hard_cache { Cache::Hard } else { Cache::Any },
            ) {
                Some(min(next_chunk, start_offset + MAX_CHUNK_SIZE))
            } else {
                Some(start_offset + MAX_CHUNK_SIZE)
            }
        } else {
            None
        }
    }

    /// Get all cache files in a list of directories.
    /// Earlier cache roots get priority over later cache roots.
    async fn get_cache_files(
        cache_root: &Utf8Path,
        data_id: &DataIdentifier,
    ) -> ByteRanger<ChunkData> {
        let mut br = ByteRanger::new();

        let cache_path = get_file_cache_path(cache_root, data_id).unwrap();
        if cache_path.exists() {
            let mut dirs = read_dir(cache_path).await.unwrap();
            while let Some(dir_entry) = dirs.next_entry().await.unwrap() {
                if let Some((offset, len)) = Self::direntry_to_namelen(dir_entry).await {
                    if len == 0 {
                        // Ignore zero-length chunks
                        continue;
                    }
                    let end_offset = Arc::new(AtomicU64::new(offset + len));
                    br.add_range(
                        offset,
                        len,
                        ChunkData {
                            chunk_start_offset: offset,
                            end_offset,
                            download_status: Weak::new(),
                        },
                    );
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

impl Drop for OpenFile {
    fn drop(&mut self) {
        if let Some(lru) = &self.lru {
            let lru = lru.clone();
            let data_id = self.data_id.clone();
            task::spawn(lru.close_file(data_id));
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkData {
    /// Start offset of this chunk.
    /// Used as an index to this chunk download status.
    ///
    /// This is necessary because byte_ranger can potentially split chunks.
    pub chunk_start_offset: u64,
    /// How far we've written downloaded bytes to disk.
    pub end_offset: Arc<AtomicU64>,
    /// Status, if a chunk is in the middle of downloading.
    // The Mutex is needed here due to DownloadStatus not being clone-able.
    // The Option is inner because we want the same data between byte-ranger range splits.
    pub download_status: Weak<Mutex<Option<DownloadStatus>>>,
}

pub enum Receiver {
    Downloader(Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Sync + Unpin>),
    CacheReader(File, u64),
}

pub enum Cache {
    Hard,
    Any,
}

/// Structure describing the current download status of a chunk.
pub struct DownloadStatus {
    /// Downloader.
    pub receiver: Receiver,
    /// Last received bytes from downloader.
    pub last_bytes: Bytes,
    /// File handle used to write to disk.
    pub write_file: File,
    /// How far we can write before we need to split the chunk file into a new one.
    pub split_offset: u64,
    /// Last revision that split_offset has been updated.
    pub last_revision: u64,
}

impl fmt::Debug for DownloadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: fix to have more info
        write!(f, "DownloadStatus(split_offset: {})", self.split_offset)
    }
}

impl DownloadStatus {
    /// Create new DownloadStatus from a receiver.
    ///
    /// The Receiver can be a downloader or a soft cache reader.
    pub fn new(
        receiver: Receiver,
        write_file: File,
        split_offset: u64,
        current_revision: u64,
    ) -> DownloadStatus {
        let last_bytes = Bytes::new();
        DownloadStatus {
            receiver,
            last_bytes,
            write_file,
            split_offset,
            last_revision: current_revision,
        }
    }

    /// Create a new DownloadStatus, chained from a previous one.
    /// This is used when we want to use the same receiver,
    /// but need to write to a new file.
    pub fn chain_from_prev(
        prev: DownloadStatus,
        write_file: File,
        split_offset: u64,
        current_revision: u64,
    ) -> DownloadStatus {
        DownloadStatus {
            receiver: prev.receiver,
            last_bytes: prev.last_bytes,
            write_file,
            split_offset,
            last_revision: current_revision,
        }
    }
}

/// Handle that holds Arc pointer to nothing.
/// Used to close downloaders when all threads are finished.
#[derive(Debug)]
pub struct DownloadHandle {
    pub dl_handle: Arc<Mutex<Option<DownloadStatus>>>,
}
