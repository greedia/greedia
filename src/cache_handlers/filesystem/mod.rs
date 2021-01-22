// Filesystem cache handler

use std::{
    cmp::min, collections::HashMap, io::SeekFrom, path::Path, path::PathBuf, sync::Arc,
    sync::Weak,
};

use super::{CacheDriveHandler, CacheFileHandler, CacheHandlerError};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use byte_ranger::Scan;
use bytes::Bytes;
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};

mod open_file;
use open_file::{DownloadHandle, OpenFile};

struct FilesystemCacheHandler {
    drive_id: String,
    hard_cache_root: PathBuf,
    soft_cache_root: PathBuf,
    open_files: Mutex<HashMap<String, Weak<Mutex<OpenFile>>>>,
    downloader_drive: Arc<dyn DownloaderDrive>,
}

// TODO: create OpenFileStruct that contains gc_count for garbage collection
// put whole OpenFileStruct within Mutex

impl FilesystemCacheHandler {
    fn new(
        drive_id: String,
        hard_cache_root: &Path,
        soft_cache_root: &Path,
        downloader_drive: Arc<dyn DownloaderDrive>,
    ) -> Box<dyn CacheDriveHandler> {
        let open_files = Mutex::new(HashMap::new());
        let hard_cache_root = hard_cache_root.to_path_buf();
        let soft_cache_root = soft_cache_root.to_path_buf();
        Box::new(FilesystemCacheHandler {
            drive_id,
            hard_cache_root,
            soft_cache_root,
            open_files,
            downloader_drive,
        })
    }

    async fn get_open_handle(
        &self,
        file_id: &str,
        data_id: &DataIdentifier,
    ) -> Arc<Mutex<OpenFile>> {
        let mut files = self.open_files.lock().await;
        if let Some(file) = files.get(file_id) {
            if let Some(file_arc) = file.upgrade() {
                println!("Existing file");
                return file_arc;
            } else {
                println!("Existing file, but last handle died");
            }
        }

        println!("No file, creating new");

        let new_file = Arc::new(Mutex::new(self.create_open_handle(file_id, data_id).await));
        // Store a weak pointer to the open file, so we don't have to do our own reference counting
        files.insert(file_id.to_owned(), Arc::downgrade(&new_file));
        new_file
    }

    async fn create_open_handle(&self, file_id: &str, data_id: &DataIdentifier) -> OpenFile {
        let downloader_drive = self.downloader_drive.clone();
        OpenFile::new(
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
            file_id,
            handle,
            size,
            offset,
            write_hard_cache,
            hard_cache_file_root,
            soft_cache_file_root,
            current_chunk: None,
        }))
    }
}

struct FilesystemCacheFileHandler {
    file_id: String,
    handle: Arc<Mutex<OpenFile>>,
    size: u64,
    offset: u64,
    hard_cache_file_root: PathBuf,
    soft_cache_file_root: PathBuf,
    write_hard_cache: bool,
    current_chunk: Option<CurrentChunk>,
}

enum CurrentChunk {
    Downloading {
        /// Chunk file to write to
        file: File,
        /// Offset of the start of the chunk file
        start_offset: u64,
        /// Handle for downloading from provider
        dl: DownloadHandle,
        /// Cache of the last Bytes object
        last_bytes: Bytes,
        /// Where to stop writing (to keep a maximum cache file size)
        max_offset: u64,
    },
    Reading {
        /// Chunk file to write to
        file: File,
        /// How far we can safely read before starting or waiting for a downloader
        end_offset: u64,
    },
}

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        self.handle_into(buf.len(), Some(buf)).await
    }

    async fn cache_data(&mut self, len: usize) -> usize {
        self.handle_into(len, None).await
    }

    async fn read_bytes(&mut self, len: usize) -> Bytes {
        todo!()
    }

    async fn read_exact(&mut self, len: usize) -> Bytes {
        // Call read_into repeatedly until correct length, or EOF
        todo!()
    }

    async fn seek_to(&mut self, offset: u64) {
        // Close all downloads and files, set offset
        // TODO: short-circuit offset == self.offset
        self.current_chunk = None;
        self.offset = min(offset, self.size);
    }
}

impl FilesystemCacheFileHandler {
    /// Set the new current chunk, so the next step of read_into can 
    async fn set_new_current_chunk(&mut self) {
        let mut of = self.handle.lock().await;
        let chunk = of.get_chunk_at(self.offset, self.write_hard_cache).await;
        dbg!(&chunk);
        match chunk {
            Some(Scan::Data {
                start_offset,
                size,
                data, // TODO
            }) => {
                dbg!(start_offset, size, self.offset);
                // We're within a data chunk, start reading from it
                let file_path =
                    self.get_file_cache_chunk_path(self.write_hard_cache, start_offset);

                if size == 0 {
                    let (file, dl) = of
                        .append_download_chunk(
                            start_offset,
                            start_offset + size,
                            self.write_hard_cache,
                            &file_path,
                        )
                        .await
                        .unwrap();

                    self.current_chunk = Some(CurrentChunk::Downloading {
                        file,
                        start_offset,
                        dl,
                        last_bytes: Bytes::new(),
                        max_offset: 200_000_000, // TODO: not hardcode
                    });
                } else {
                    let mut file = File::open(file_path).await.unwrap();
                    file.seek(SeekFrom::Start(self.offset - start_offset))
                        .await
                        .unwrap();

                    let end_offset = start_offset + size;
                    self.current_chunk = Some(CurrentChunk::Reading { file, end_offset });    
                }
            }
            Some(Scan::FinalRange {
                start_offset,
                size,
            }) => {
                // We're somewhere after the end of the final chunk
                if self.offset == start_offset + size /* && TODO size < max_size */ {
                    // We're at the end of the final chunk and have enough space to write
                    // Download and append data to chunk
                    let file_path =
                        self.get_file_cache_chunk_path(self.write_hard_cache, start_offset);

                    let (file, dl) = of
                        .append_download_chunk(
                            start_offset,
                            start_offset + size,
                            self.write_hard_cache,
                            &file_path,
                        )
                        .await
                        .unwrap();

                    self.current_chunk = Some(CurrentChunk::Downloading {
                        file,
                        start_offset,
                        dl,
                        last_bytes: Bytes::new(),
                        max_offset: 200_000_000, // TODO: not hardcode
                    });
                } else /* && TODO size < max_size */ {
                    // We're not at the end of the final chunk, but have space to write
                    // Start downloading new chunk
                    let file_path =
                        self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);

                    let (file, dl) = of
                        .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
                        .await
                        .unwrap();

                    self.current_chunk = Some(CurrentChunk::Downloading {
                        file,
                        start_offset,
                        dl,
                        last_bytes: Bytes::new(),
                        max_offset: 200_000_000, // TODO: not hardcode
                    });
                }
            }
            Some(Scan::Gap {
                start_offset,
                size,
                prev_range_start_offset,
            }) => {
                // We're somewhere in between two chunks
                if self.offset == start_offset /* && TODO size < max_size */ {
                    // We're at the end of the previous chunk and have enough space to write
                    // Download and append data to chunk
                    let file_path =
                        self.get_file_cache_chunk_path(self.write_hard_cache, prev_range_start_offset);

                    let (file, dl) = of
                        .append_download_chunk(
                            start_offset,
                            start_offset + size,
                            self.write_hard_cache,
                            &file_path,
                        )
                        .await
                        .unwrap();

                    self.current_chunk = Some(CurrentChunk::Downloading {
                        file,
                        start_offset,
                        dl,
                        last_bytes: Bytes::new(),
                        max_offset: 200_000_000, // TODO: not hardcode
                    });
                } else /* && TODO size < max_size */ {
                    // We're not at the end of the previous chunk, but have space to write
                    // Start downloading new chunk
                    let file_path =
                        self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);

                    let (file, dl) = of
                        .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
                        .await
                        .unwrap();

                    self.current_chunk = Some(CurrentChunk::Downloading {
                        file,
                        start_offset,
                        dl,
                        last_bytes: Bytes::new(),
                        max_offset: 200_000_000, // TODO: not hardcode
                    });
                }
            }
            None | Some(Scan::Empty) => {
                // There are no chunks in the file
                // Start downloading new chunk
                let start_offset = self.offset;

                let file_path =
                    self.get_file_cache_chunk_path(self.write_hard_cache, start_offset);

                let (file, dl) = of
                    .start_download_chunk(start_offset, self.write_hard_cache, &file_path)
                    .await
                    .unwrap();

                self.current_chunk = Some(CurrentChunk::Downloading {
                    file,
                    start_offset,
                    dl,
                    last_bytes: Bytes::new(),
                    max_offset: 200_000_000, // TODO: not hardcode
                });
            }
        }
    }

    /// Handle the read_into and cache_data methods
    async fn handle_into(&mut self, len: usize, mut buf: Option<&mut [u8]>) -> usize {
        if self.offset == self.size {
            return 0;
        }
        println!("handle_into len {} offset {}", len, self.offset);

        if self.current_chunk.is_none() {
            // Set the new current chunk
            // Try to lock self.handle as little as possible
            self.set_new_current_chunk().await;
        }

        match &mut self.current_chunk {
            Some(CurrentChunk::Downloading {
                file,
                start_offset,
                dl,
                last_bytes,
                max_offset: _,
            }) => {
                // Write and fill up buf with rest of last_bytes, if possible

                // TODO: fix this when bytes fixes #468
                let split_len = min(len, last_bytes.len());
                let from_last_bytes = last_bytes.split_to(split_len);
                if from_last_bytes.len() != 0 {
                    {
                        let mut of = self.handle.lock().await;
                        file.write_all(&from_last_bytes).await.unwrap();

                        of.update_chunk_size(self.write_hard_cache, *start_offset, self.offset + from_last_bytes.len() as u64).await;
                    }
                    if let Some(ref mut buf) = buf {
                        buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
                    }
                }

                self.offset += from_last_bytes.len() as u64;

                if from_last_bytes.len() == len {
                    return from_last_bytes.len();
                }

                // Download one more Bytes object if we can
                if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
                    // TODO: fix this when bytes fixes #468
                    let split_len = min(len - from_last_bytes.len(), b.len());
                    let to_output = b.split_to(split_len);

                    {
                        let mut of = self.handle.lock().await;
                        file.write_all(&to_output).await.unwrap();
                        of.update_chunk_size(self.write_hard_cache, *start_offset, self.offset + to_output.len() as u64).await;
                    }
                    if let Some(ref mut buf) = buf {
                        buf[from_last_bytes.len()..to_output.len()].copy_from_slice(&to_output);
                    }
                    *last_bytes = b;
                    self.offset += to_output.len() as u64;
                    return from_last_bytes.len() + to_output.len();
                } else {
                    // Either EOF, or some network issue
                    todo!("EOF?");
                }
            }
            Some(CurrentChunk::Reading { file, end_offset }) => {
                let max_data_to_read = *end_offset - self.offset;
                let data_to_read = min(len as u64, max_data_to_read) as usize;

                let bytes_read = if let Some(buf) = buf {
                    file.read(&mut buf[..data_to_read]).await.unwrap()
                } else {
                    // Since there's no point reading to nothing, just seek instead
                    file.seek(SeekFrom::Current(data_to_read as i64)).await.unwrap();
                    data_to_read
                };

                if *end_offset == self.offset {
                    self.current_chunk = None;
                }

                self.offset += bytes_read as u64;
                return bytes_read;
            }
            None => {
                // We weren't able to set current_chunk?
                unreachable!("set_new_current_chunk() was unable to set a new chunk for some reason")
            }
        }
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
            cache_root.join(dir_1).join(dir_2).join(hex_md5)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cache_handlers::filesystem::FilesystemCacheHandler;
    use crate::downloaders::gdrive::GDriveClient;
    use crate::downloaders::DownloaderClient;

    #[tokio::test]
    async fn do_2_stuff() {
        let client_id =
            "***REMOVED***".to_string();
        let client_secret = "***REMOVED***".to_string();
        let refresh_token = "***REMOVED***".to_string();
        let drive_id = "***REMOVED***".to_string();

        let c = GDriveClient::new(client_id, client_secret, refresh_token)
            .await
            .unwrap();

        let d = c.open_drive(drive_id);

        let file_id = "***REMOVED***".to_string();
        let data_id =
            DataIdentifier::GlobalMd5(hex::decode("***REMOVED***").unwrap());
        let file_size = 1073741824;

        let hard_cache_root = PathBuf::from("/root/greed_test/hard_cache");
        let soft_cache_root = PathBuf::from("/root/greed_test/soft_cache");

        let fsch = FilesystemCacheHandler::new(
            "main".to_string(),
            &hard_cache_root,
            &soft_cache_root,
            d.into(),
        );
        let mut f = fsch
            .open_file(file_id, data_id, file_size, 0, false)
            .await
            .unwrap();

        let mut buf = [0u8; 65536];

        for _ in 0..10000 {
            let x = f.read_into(&mut buf).await;
        dbg!(&x);
        }
    }
}
