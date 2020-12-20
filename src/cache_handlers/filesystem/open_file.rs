use std::{sync::Arc, path::Path, path::PathBuf};
use bytes::Bytes;
use futures::Stream;
use tokio::{fs::DirEntry, sync::broadcast, fs::read_dir, stream::StreamExt};

use byte_ranger::{ByteRanger, Scan};
use crate::{downloaders::{DownloaderDrive, DownloaderError, DownloaderFile}, types::DataIdentifier};

/// OpenFile is a struct representing a single open file within the cache. It is a requirement
/// that, for any file, only one of these structs exist and that it is protected by a mutex.
/// This includes scanning a file to populate the hard cache.
///
/// When reading a file, the reader must first check the hard cache, then the soft cache, and if
/// neither cache contains data, it must download the data and simultaneously stream it to the
/// reader, and to the filesystem. A flag (TODO WHICH FLAG?) will specify whether downloaded data
/// gets written to the hard cache or soft cache.
///
/// The caches are made up of chunk files, named by their offsets in the original file. If a chunk
/// file does not exist that the reader needs, it will download the data needed until either the
/// reader closes, or the chunk file reaches 100MB, at which point it will create a new chunk file
/// and continue downloading.
///
/// When a reader reaches the end of a chunk, it must download new data and append to the chunk
/// until it becomes 100MB, or it reaches the beginning of the next chunk, whichever comes first.
///
/// When an appended download occurs, the downloader must update the structure to keep track of how
/// much data has been written to disk. This is in order to ensure other readers can read data chunks
/// without having to re-lock the mutex on each read operation.
pub struct OpenFile {
    file_id: String,
    data_id: DataIdentifier,
    downloader_drive: Arc<dyn DownloaderDrive>,
    pub hard_cache: ByteRanger<ChunkStatus>,
    pub soft_cache: ByteRanger<ChunkStatus>,
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
        let hard_cache = Self::get_cache_files(hard_cache_root, data_id).await;
        let soft_cache = Self::get_cache_files(soft_cache_root, data_id).await;

        let file_id = file_id.to_string();
        let data_id = data_id.clone();
        let downloader_drive = downloader_drive.into();

        dbg!(&hard_cache);
        dbg!(&soft_cache);
        OpenFile {
            file_id,
            data_id,
            downloader_drive,
            hard_cache,
            soft_cache,
        }
    }

    pub async fn get_chunk_at<'a>(&'a self, offset: u64) -> Option<Scan<&'a ChunkStatus>> {
        let hcs = self.hard_cache.get_range_at(offset);
        if hcs.is_data() {
            Some(hcs)
        } else {
            let scs = self.soft_cache.get_range_at(offset);
            if scs.is_data() {
                Some(scs)
            } else {
                None
            }
        }
    }

    // Use this to start a new chunk by downloading it.
    pub async fn download_chunk(&mut self, offset: u64, hard_cache: bool) -> DownloadHandle {
        let downloader = self.downloader_drive.open_file(self.file_id.clone(), offset, hard_cache).await.unwrap();
        // TODO: eventually change this to something that doesn't require a sender to create new receivers
        let (progress_channel, _) = broadcast::channel(1);
        let download_handle = DownloadHandle {
            progress_channel: progress_channel.clone(),
            downloader,
            max_file_size: 200_000_000, // TODO: not hardcode this
        };

        let download_status = DownloadStatus {
            current_offset: offset,
            progress_channel,
        };

        let cache = if hard_cache {
            &mut self.hard_cache
        } else {
            &mut self.soft_cache
        };

        cache.add_range(offset, 0, ChunkStatus {
            download_status: Some(Arc::new(download_status))
        });

        download_handle
    }

    // TODO
    pub async fn append_download_chunk() {
        
    }

    /// Get the hard cache and soft cache paths, given an identifier.
    fn get_file_cache_path(cache_root: &Path, data_id: &DataIdentifier) -> PathBuf {
        match data_id {
            DataIdentifier::GlobalMd5(ref md5) => {
                let hex_md5 = hex::encode(md5);
                let dir_1 = hex_md5.get(0..2).unwrap();
                let dir_2 = hex_md5.get(2..4).unwrap();
                cache_root.join(dir_1).join(dir_2).join(hex_md5)
            }
        }
    }

    /// Get all cache files in a hard/soft cache directory.
    /// This assumes no other Greedia instance has this file open.
    async fn get_cache_files(cache_root: &Path, data_id: &DataIdentifier) -> ByteRanger<ChunkStatus> {
        let path = Self::get_file_cache_path(cache_root, data_id);

        if path.exists() {
            let mut dirs = read_dir(path).await.unwrap();
            let mut br = ByteRanger::new();
            while let Some(dir_entry) = dirs.next().await {
                if let Some((offset, len)) = Self::direntry_to_namelen(dir_entry.unwrap()).await {
                    br.add_range(offset, len, ChunkStatus { download_status: None });
                }
            }
            br
        } else {
            ByteRanger::new()
        }
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
pub struct ChunkStatus {
    /// Status, if a chunk is in the middle of downloading.
    download_status: Option<Arc<DownloadStatus>>
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
    downloader: Box<dyn Stream<Item = Result<Bytes, DownloaderError>>>,
    max_file_size: u64,
}

impl DownloadHandle {
    pub fn get_next_bytes(&mut self) -> Bytes {
        //self.downloader;
        todo!()
    }
}