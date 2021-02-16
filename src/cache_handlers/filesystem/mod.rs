// Filesystem cache handler

use std::{
    cmp::min, collections::HashMap, io::SeekFrom, path::Path, path::PathBuf, sync::Arc, sync::Weak,
};

use self::open_file::{DownloadHandle, DownloadStatus};

use super::{CacheDriveHandler, CacheFileHandler, CacheHandlerError};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use byte_ranger::GetRange;
use bytes::Bytes;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

mod open_file;
use open_file::OpenFile;

const MAX_CHUNK_SIZE: u64 = 100_000_000;

struct FilesystemCacheHandler {
    drive_id: String,
    hard_cache_root: PathBuf,
    soft_cache_root: PathBuf,
    open_files: Mutex<HashMap<String, Weak<Mutex<OpenFile>>>>,
    downloader_drive: Arc<dyn DownloaderDrive>,
}

enum CurrentChunk {
    /// Currently downloading data from download provider.
    Downloading {
        /// Chunk file to read from.
        file: File,
        /// Where to stop reading before more downloading is needed.
        end_offset: u64,
        /// Start offset of currently downloading chunk.
        chunk_start_offset: u64,
        /// Handle containing the downloader and write file handle.
        /// Not directly accessible in the threads, and only
        /// used for garbage collection when all threads are closed.
        _dl: DownloadHandle,
    },
    /// Currently reading data from disk.
    /// This is used if we haven't reached the end_offset, but nobody else is
    /// currently downloading.
    Reading {
        /// Chunk file to read from.
        file: File,
        /// How far we can safely read before starting or waiting for a downloader
        end_offset: u64,
    },
    /// Reading data from soft_cache into hard_cache.
    Copying {
        /// Chunk file to read from (soft_cache).
        file: File,
        /// Chunk file to write to (hard_cache).
        to_file: File,
        /// Offset of the start of the output chunk file.
        // TODO: output or input?
        chunk_start_offset: u64,
        /// How far we can safely read before starting or waiting for a downloader.
        end_offset: u64,
    },
}

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
    handle: Arc<Mutex<OpenFile>>,
    size: u64,
    offset: u64,
    hard_cache_file_root: PathBuf,
    soft_cache_file_root: PathBuf,
    write_hard_cache: bool,
    current_chunk: Option<CurrentChunk>,
}

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        self.handle_read_into(buf.len(), Some(buf)).await
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> usize {
        // Call read_into repeatedly until correct length, or EOF
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.read_into(&mut buf[total_bytes_read..]).await;
            if bytes_read == 0 {
                return total_bytes_read;
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == buf.len() {
                    return total_bytes_read;
                }
            }
        }
    }

    async fn cache_data(&mut self, len: usize) -> usize {
        self.handle_read_into(len, None).await
    }

    async fn cache_exact(&mut self, len: usize) -> usize {
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.cache_data(len - total_bytes_read).await;
            if bytes_read == 0 {
                return total_bytes_read;
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == len {
                    return total_bytes_read;
                }
            }
        }
    }

    async fn seek_to(&mut self, offset: u64) {
        if offset == self.offset {
            return;
        }
        // Close all downloads and files, set offset
        self.current_chunk = None;
        self.offset = min(offset, self.size);
    }
}

impl FilesystemCacheFileHandler {
    /// Handle the read_into and cache_data methods
    async fn handle_read_into(&mut self, len: usize, buf: Option<&mut [u8]>) -> usize {
        if self.offset == self.size {
            return 0;
        }
        println!("handle_into len {} offset {}", len, self.offset);

        if self.current_chunk.is_none() {
            let new_chunk = self.new_current_chunk().await;
            self.current_chunk = Some(new_chunk);
        }

        if let Some(ref mut current_chunk) = self.current_chunk {
            let data_read = match current_chunk {
                CurrentChunk::Downloading {
                    file,
                    end_offset,
                    chunk_start_offset,
                    _dl,
                } => {
                    println!("DL at {}, len {}", self.offset, len);
                    if self.offset < *end_offset {
                        // If we have any data before end_offset, read that first
                        let max_read_len = min(len, (self.offset - *end_offset) as usize);
                        Self::simple_read(file, buf, max_read_len).await
                    } else {
                        // Otherwise, lock the OpenFile and try to read/download from there
                        let mut of = self.handle.lock().await;
                        let (data_read, new_end_offset) = Self::download_read(
                            &mut of,
                            file,
                            buf,
                            len,
                            *chunk_start_offset,
                            self.offset,
                        )
                        .await;
                        data_read
                    }
                }
                CurrentChunk::Reading { file, end_offset } => {
                    todo!()
                }
                CurrentChunk::Copying {
                    file,
                    to_file,
                    chunk_start_offset: start_offset,
                    end_offset,
                } => {
                    todo!()
                }
            };
            self.offset += data_read as u64;
            data_read
        } else {
            unreachable!("self.current_chunk cannot be None here");
        }

        // Get current chunk status
        /*match &mut self.current_chunk {
            // We have an open download stream
            Some(CurrentChunk::Downloading {
                file,
                end_offset,
            }) => {
                println!("DL start_offset {}", start_offset);
                // If anything's left in last_bytes, write that out first
                let split_len = min(len, last_bytes.len());
                let from_last_bytes = last_bytes.split_to(split_len);

                self.offset += from_last_bytes.len() as u64;

                if from_last_bytes.len() != 0 {
                    {
                        // Try to lock self.handle as little as possible
                        let mut of = self.handle.lock().await;
                        file.write_all(&from_last_bytes).await.unwrap();

                        of.update_chunk_size(*start_offset, self.offset - *start_offset).await;
                    }
                    if let Some(ref mut buf) = buf {
                        buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
                    }
                }

                // Stop downloading if we go past end_offset
                // It's okay if we overrun a little bit
                if self.offset >= *end_offset {
                    self.current_chunk = None;
                    return from_last_bytes.len();
                }

                if from_last_bytes.len() == len {
                    return from_last_bytes.len();
                }

                // Download one more Bytes object if we can
                if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
                    // TODO: fix this when tokio-rs/bytes fixes #468
                    let split_len = min(len - from_last_bytes.len(), b.len());
                    let to_output = b.split_to(split_len);

                    self.offset += to_output.len() as u64;

                    {
                        let mut of = self.handle.lock().await;
                        file.write_all(&to_output).await.unwrap();
                        // TODO: double-check this is correct
                        of.update_chunk_size(*start_offset, self.offset - *start_offset).await;
                    }
                    if let Some(ref mut buf) = buf {
                        buf[from_last_bytes.len()..to_output.len()].copy_from_slice(&to_output);
                    }
                    *last_bytes = b;

                    if self.offset >= *end_offset {
                        self.current_chunk = None;
                    }

                    return from_last_bytes.len() + to_output.len();
                } else {
                    // Either EOF, or some network issue
                    todo!("EOF?");
                }
            }
            // We're in the middle of reading an open file.
            // It's safe to read up to end_offset, after which
            // we need to either check if another thread is downloading
            // this file, or we need to start a new download ourselves.
            Some(CurrentChunk::Reading { file, end_offset }) => {
                println!("READ");
                let max_data_to_read = *end_offset - self.offset;
                let data_to_read = min(len as u64, max_data_to_read) as usize;
                // dbg!(*end_offset, self.offset, max_data_to_read, data_to_read, len);

                let bytes_read = if let Some(buf) = buf {
                    file.read(&mut buf[..data_to_read]).await.unwrap()
                } else {
                    // Since there's no point reading to nothing, just seek instead
                    file.seek(SeekFrom::Current(data_to_read as i64)).await.unwrap();
                    data_to_read
                };

                self.offset += bytes_read as u64;

                if *end_offset == self.offset {
                    self.current_chunk = None;
                }

                return bytes_read;
            }
            Some(CurrentChunk::Copying { file, to_file, start_offset, end_offset }) => {
                println!("COPY");
                let mut inner_buf = [0; 16384];
                let max_data_to_read = min(*end_offset - self.offset, inner_buf.len() as u64);
                let data_to_read = min(len as u64, max_data_to_read) as usize;

                // Read from soft_cache
                let bytes_read = file.read(&mut inner_buf[..data_to_read]).await.unwrap();

                self.offset += bytes_read as u64;

                // Write to hard_cache
                {
                    let mut of = self.handle.lock().await;
                    to_file.write_all(&inner_buf[..bytes_read]).await.unwrap();

                    of.update_chunk_size(*start_offset, self.offset - *start_offset).await;
                }

                // If using read_into, also write out to buf
                if let Some(buf) = buf {
                    &mut buf[..bytes_read].copy_from_slice(&mut inner_buf[..bytes_read]);
                };

                return bytes_read;
            }
            // This should be impossible to reach, as set_new_current_chunk should never return None.
            // Perhaps this could be validated more statically.
            None => {
                // We weren't able to set current_chunk?
                unreachable!("set_new_current_chunk() was unable to set a new chunk for some reason")
            }
        } */
    }

    /// Figure out the current chunk based on our offset.
    async fn new_current_chunk(&mut self) -> CurrentChunk {
        let mut of = self.handle.lock().await;
        let chunk = of.get_chunk_at(self.offset).await;
        dbg!(&chunk);
        // Where are we?
        match chunk {
            // We're in the middle of a data chunk
            GetRange::Data {
                start_offset,
                size,
                data,
            } => {
                let chunk_start_offset = data.chunk_start_offset;
                let is_hard_cache = data.is_hard_cache;
                if self.write_hard_cache && data.is_hard_cache == false {
                    // We're writing to hard_cache and data exists in soft_cache
                    // Copy from soft_cache to hard_cache
                    self.start_copying(
                        &mut of,
                        chunk_start_offset,
                        start_offset,
                        size,
                        is_hard_cache,
                    )
                    .await
                } else {
                    // Don't copy, and just read data instead
                    self.start_reading(chunk_start_offset, start_offset, size, is_hard_cache)
                        .await
                    // TODO: handle opening existing downloads here
                }
            }
            // We're somewhere in between two chunks
            GetRange::Gap {
                start_offset,
                size,
                prev_range_start_offset: _,
                prev_range_data,
            } => {
                if self.offset == start_offset
                    && self.write_hard_cache == prev_range_data.is_hard_cache
                {
                    // We're at the end of the previous chunk and have enough space to write
                    // Download and append data to chunk
                    let chunk_start_offset = prev_range_data.chunk_start_offset;
                    self.start_append_dl(&mut of, start_offset, size, chunk_start_offset)
                        .await
                } else {
                    // We're not at the end of the previous chunk, but have space to write
                    // Start downloading new chunk
                    self.start_new_dl(&mut of).await
                }
            }
            // We're somewhere after the end of the final chunk
            GetRange::FinalRange {
                start_offset,
                size,
                data,
            } => {
                if self.offset == start_offset + size && self.write_hard_cache == data.is_hard_cache
                {
                    // We're at the end of the previous chunk and have enough space to write
                    // Download and append data to chunk
                    let chunk_start_offset = data.chunk_start_offset;
                    self.start_append_dl(&mut of, start_offset, size, chunk_start_offset)
                        .await
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
    async fn start_new_dl(&self, of: &mut OpenFile) -> CurrentChunk {
        println!("start_new_dl");
        let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);

        let (file, dl) = of
            .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
            .await
            .unwrap();

        CurrentChunk::Downloading {
            file,
            end_offset: self.offset + MAX_CHUNK_SIZE,
            chunk_start_offset: self.offset,
            _dl: dl,
        }
    }

    /// Start a download, appending to an existing chunk.
    async fn start_append_dl(
        &self,
        of: &mut OpenFile,
        start_offset: u64,
        size: u64,
        chunk_start_offset: u64,
    ) -> CurrentChunk {
        println!("start_append_dl");
        let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, chunk_start_offset);

        let end_offset = start_offset + size;

        let (file, dl) = of
            .append_download_chunk(
                chunk_start_offset,
                end_offset,
                self.write_hard_cache,
                &file_path,
            )
            .await
            .unwrap();

        CurrentChunk::Downloading {
            file,
            end_offset: min(start_offset + MAX_CHUNK_SIZE, start_offset + size),
            chunk_start_offset,
            _dl: dl,
        }
    }

    /// Start copying an existing chunk from soft_cache to hard_cache.
    async fn start_copying(
        &self,
        of: &mut OpenFile,
        chunk_start_offset: u64,
        start_offset: u64,
        size: u64,
        is_hard_cache: bool,
    ) -> CurrentChunk {
        let file_path = self.get_file_cache_chunk_path(is_hard_cache, chunk_start_offset);

        let mut file = File::open(file_path).await.unwrap();
        file.seek(SeekFrom::Start(self.offset - start_offset))
            .await
            .unwrap();

        let end_offset = start_offset + size;

        let start_offset = self.offset;
        let hc_file_path = self.get_file_cache_chunk_path(true, start_offset);
        let to_file = of.new_copy_chunk(&hc_file_path).await.unwrap();

        CurrentChunk::Copying {
            file,
            to_file,
            chunk_start_offset: start_offset,
            end_offset,
        }
    }

    /// Start reading an existing chunk.
    async fn start_reading(
        &self,
        chunk_start_offset: u64,
        start_offset: u64,
        size: u64,
        is_hard_cache: bool,
    ) -> CurrentChunk {
        let file_path = self.get_file_cache_chunk_path(is_hard_cache, chunk_start_offset);

        let mut file = File::open(file_path).await.unwrap();
        file.seek(SeekFrom::Start(self.offset - start_offset))
            .await
            .unwrap();

        let end_offset = start_offset + size;

        CurrentChunk::Reading { file, end_offset }
    }

    /// Read data from file, or ignore if we're caching.
    async fn simple_read(file: &mut File, buf: Option<&mut [u8]>, len: usize) -> usize {
        if let Some(buf) = buf {
            file.read(&mut buf[..len]).await.unwrap()
        } else {
            // Since there's no point reading to nothing, just seek instead
            file.seek(SeekFrom::Current(len as i64)).await.unwrap();
            len
        }
    }

    /// Read data from downloader. Returns bytes read, as well as the current end_offset.
    async fn download_read(
        of: &mut OpenFile,
        file: &mut File,
        buf: Option<&mut [u8]>,
        len: usize,
        chunk_start_offset: u64,
        current_offset: u64,
    ) -> (usize, u64) {
        let chunk_mutex = of.get_download_status(chunk_start_offset);
        let mut chunk = chunk_mutex.lock().await;
        if let Some(DownloadStatus {
            downloader,
            last_bytes,
            write_file,
            end_offset,
            split_offset,
            dl_handle,
        }) = chunk.as_mut()
        {
            if current_offset < *end_offset {
                // First, check if we're beyond end_offset
                let max_read_len = min(len, (current_offset - *end_offset) as usize);
                let data_read = Self::simple_read(file, buf, max_read_len).await;
                (data_read, *end_offset)
            } else if !last_bytes.is_empty() {
                // Second, check if data exists in last_bytes
                let split_len = min(len, last_bytes.len());
                let from_last_bytes = last_bytes.split_to(split_len);
                write_file.write_all(&from_last_bytes).await.unwrap();
                if let Some(buf) = buf {
                    buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
                }
                (from_last_bytes.len(), *end_offset)
            } else {
                // Third, download a new last_bytes and use that
                todo!()
            }
        } else {
            todo!()
            // TODO: Probably return zero here?
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

        // Read a bit of hard cache
        println!("HARD CACHE");
        let mut f = fsch
            .open_file(file_id.clone(), data_id.clone(), file_size, 0, true)
            .await
            .unwrap();

        let mut buf = [0u8; 65536];

        let x = f.read_exact(&mut buf).await;
        dbg!(&x);

        f.seek_to(1_000_000).await;

        let x = f.read_exact(&mut buf).await;
        dbg!(&x);

        println!("SOFT CACHE");
        // Read more soft cache
        let mut f = fsch
            .open_file(file_id, data_id, file_size, 65500, false)
            .await
            .unwrap();

        let mut buf = [0u8; 65536];

        let x = f.read_exact(&mut buf).await;
        dbg!(&x);

        f.seek_to(999_000).await;

        let x = f.read_exact(&mut buf).await;
        dbg!(&x);
    }
}
