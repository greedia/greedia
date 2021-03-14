use bytes::Bytes;
use futures::Stream;
use std::{
    cmp::min,
    fmt,
    io::SeekFrom,
    path::Path,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::{
    fs::DirEntry,
    fs::{read_dir, File, OpenOptions},
    stream::StreamExt,
    sync::Mutex,
};

use crate::{
    cache_handlers::CacheHandlerError,
    downloaders::{DownloaderDrive, DownloaderError},
    types::DataIdentifier,
};
use byte_ranger::{ByteRanger, GetRange};

use super::{get_file_cache_path, MAX_CHUNK_SIZE};

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
/// without having to re-lock the mutex on each read operation.
pub struct OpenFile {
    /// ID used within the downloader.
    file_id: String,
    /// Downloader this file is attached to.
    downloader_drive: Arc<dyn DownloaderDrive>,
    /// List of chunks in the hard cache.
    hc_chunks: ByteRanger<ChunkData>,
    /// List of chunks in the soft cache.
    sc_chunks: ByteRanger<ChunkData>,
    /// Number of changes made to either cache. This is so downloaders don't need to repeatedly check
    /// the hard and soft cache for changes if none have been made.
    revision: u64,
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
        let hc_chunks = Self::get_cache_files(hard_cache_root, data_id).await;
        let sc_chunks = Self::get_cache_files(soft_cache_root, data_id).await;

        let file_id = file_id.to_string();
        let revision = 0;

        OpenFile {
            file_id,
            downloader_drive,
            hc_chunks,
            sc_chunks,
            revision,
        }
    }

    /// Get the range or gap at a specific offset.
    ///
    /// cache: Which cache to get chunk from. If AnyCache, check hard_cache then soft_cache.
    ///
    /// Returns chunk data, as well as a bool that's true if the chunk is in hard_cache, or false if in soft_cache.
    pub fn get_chunk_at<'a>(
        &'a self,
        offset: u64,
        cache: Cache,
    ) -> (GetRange<&'a ChunkData>, bool) {
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
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("START DL {}", offset);

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
        let end_offset = offset;

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

        let receiver = Receiver::Downloader(downloader);
        let dl_handle = Arc::new(());

        self.revision += 1;
        let download_status = DownloadStatus::new(
            receiver,
            write_file,
            end_offset,
            split_offset,
            self.revision,
            dl_handle.clone(),
        );

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
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("APPEND DL");

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

            // Seek to proper offset, relative to chunk's start offset.
            let off1 = write_file.seek(SeekFrom::End(0)).await?;

            // Create a downloader
            let downloader = self
                .downloader_drive
                .open_file(self.file_id.clone(), start_offset, write_hard_cache)
                .await
                .unwrap();

            let receiver = Receiver::Downloader(downloader);

            let dl_handle = Arc::new(());

            let download_status = DownloadStatus::new(
                receiver,
                write_file,
                start_offset,
                split_offset,
                self.revision,
                dl_handle.clone(),
            );

            cache_data.download_status = Arc::new(Mutex::new(Some(download_status)));

            let mut read_file = File::open(file_path).await?;

            let off2 = read_file.seek(SeekFrom::End(0)).await?;

            let download_handle = DownloadHandle { dl_handle };

            Ok((read_file, download_handle))
        } else {
            Err(CacheHandlerError::AppendChunkError {
                start_offset: chunk_start_offset,
            })
        }
    }

    /// Use this to copy from soft_cache to hard_cache. Creates a new hard_cache chunk.
    pub async fn start_copy_chunk(
        &mut self,
        offset: u64,
        source_file_path: &Path,
        source_file_offset: u64,
        source_file_end_offset: u64,
        file_path: &Path,
    ) -> Result<(File, DownloadHandle), CacheHandlerError> {
        println!("START COPY");

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

        let write_file = File::create(file_path).await?;

        let mut cache_file = File::open(source_file_path).await?;
        cache_file.seek(SeekFrom::Start(source_file_offset)).await?;

        let receiver = Receiver::CacheReader(cache_file, source_file_end_offset);

        let end_offset = offset;

        let dl_handle = Arc::new(());

        self.revision += 1;
        let download_status = DownloadStatus::new(
            receiver,
            write_file,
            end_offset,
            split_offset,
            self.revision,
            dl_handle.clone(),
        );

        self.hc_chunks.add_range(
            offset,
            0,
            ChunkData {
                chunk_start_offset: offset,
                download_status: Arc::new(Mutex::new(Some(download_status))),
            },
        );

        let read_file = File::open(file_path).await?;
        let download_handle = DownloadHandle { dl_handle };

        Ok((read_file, download_handle))
    }

    /// Set a new chunk size as the chunk gets downloaded.
    pub fn update_chunk_size(&mut self, hard_cache: bool, start_offset: u64, new_size: u64) {
        if hard_cache {
            &mut self.hc_chunks
        } else {
            &mut self.sc_chunks
        }
        .extend_range(start_offset, new_size);
        self.revision += 1;
    }

    /// Get the DownloadStatus of a particular chunk.
    pub fn get_download_status(
        &self,
        hard_cache: bool,
        chunk_start_offset: u64,
    ) -> Arc<Mutex<Option<DownloadStatus>>> {
        let chunk = if hard_cache {
            &self.hc_chunks
        } else {
            &self.sc_chunks
        }
        .get_data(chunk_start_offset)
        .unwrap();

        chunk.download_status.clone()
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
    async fn get_cache_files(cache_root: &Path, data_id: &DataIdentifier) -> ByteRanger<ChunkData> {
        let mut br = ByteRanger::new();

        let cache_path = get_file_cache_path(cache_root, data_id);
        if cache_path.exists() {
            let mut dirs = read_dir(cache_path).await.unwrap();
            while let Some(dir_entry) = dirs.next().await {
                if let Some((offset, len)) = Self::direntry_to_namelen(dir_entry.unwrap()).await {
                    // println!("add_range {} {}", offset, len);
                    if len == 0 {
                        // Ignore zero-length chunks
                        continue;
                    }
                    br.add_range(
                        offset,
                        len,
                        ChunkData {
                            chunk_start_offset: offset,
                            download_status: Arc::new(Mutex::new(None)),
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

#[derive(Clone, Debug)]
pub struct ChunkData {
    /// Start offset of this chunk.
    /// Used as an index to this chunk download status.
    ///
    /// This is necessary because byte_ranger can potentially split chunks.
    pub chunk_start_offset: u64,
    /// Status, if a chunk is in the middle of downloading.
    // The Mutex is needed here due to DownloadStatus not being clone-able.
    // The Option is inner because we want the same data between byte-ranger range splits.
    pub download_status: Arc<Mutex<Option<DownloadStatus>>>,
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
    /// How far we've written downloaded bytes to disk.
    pub end_offset: u64,
    /// How far we can write before we need to split the chunk file into a new one.
    pub split_offset: u64,
    /// Last revision that split_offset has been updated.
    pub last_revision: u64,
    /// Reference count of how many threads are using this downloader.
    pub dl_handle: Arc<()>,
}

impl fmt::Debug for DownloadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: fix to have more info
        write!(
            f,
            "DownloadStatus(end_offset: {}, split_offset: {})",
            self.end_offset, self.split_offset
        )
    }
}

impl DownloadStatus {
    pub fn new(
        receiver: Receiver,
        write_file: File,
        end_offset: u64,
        split_offset: u64,
        current_revision: u64,
        dl_handle: Arc<()>,
    ) -> DownloadStatus {
        let last_bytes = Bytes::new();
        DownloadStatus {
            receiver,
            last_bytes,
            write_file,
            end_offset,
            split_offset,
            last_revision: current_revision,
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
