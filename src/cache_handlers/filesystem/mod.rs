// Filesystem cache handler

use std::{
    cmp::min, collections::HashMap, io::SeekFrom, path::Path, path::PathBuf, sync::Arc, sync::Weak,
};

use self::open_file::{DownloadHandle, DownloadStatus, Receiver};

use super::{CacheDriveHandler, CacheFileHandler, CacheHandlerError};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use byte_ranger::GetRange;
use bytes::{Bytes, BytesMut};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    stream::StreamExt,
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

struct ReadData {
    /// Chunk file to read from.
    file: File,
    /// End offset of chunk on disk.
    /// If downloading, this will need to be updated from the OpenFile.
    end_offset: u64,
    /// Start offset of currently downloading chunk.
    chunk_start_offset: u64,
}

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
    /// No data could be found, but we're not at EOF, so current chunk needs
    /// to be reset.
    NeedsNewChunk,
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
            dbg!(&bytes_read);
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
    async fn handle_read_into(&mut self, len: usize, mut buf: Option<&mut [u8]>) -> usize {
        // EOF short-circuit
        if self.offset == self.size {
            return 0;
        }

        println!("handle_into len {} offset {}", len, self.offset);

        if self.current_chunk.is_none() {
            dbg!();
            // We're not in the middle of reading a chunk, so get a new one
            let new_chunk = self.new_current_chunk().await;
            self.current_chunk = Some(new_chunk);
        }

        // TODO: limit this and error if too many loops occur
        loop {
            match self.handle_chunk(len, &mut buf).await {
                
                Reader::Data(data_read) => {
                    return data_read;
                }
                Reader::LastData(data_read) => {
                    self.current_chunk = None;
                    return data_read;
                }
                Reader::NeedsNewChunk => {
                    let new_chunk = self.new_current_chunk().await;
                    self.current_chunk = Some(new_chunk);
                }
                Reader::Eof => {
                    return 0;
                }
            }
            // if let Some(data_read) = self.handle_chunk(len, &mut buf).await {
            //     return data_read
            // } else {
            //     dbg!();
            //     let new_chunk = self.new_current_chunk().await;
            //     self.current_chunk = Some(new_chunk);
            // }
        }
    }

    async fn handle_chunk(&mut self, len: usize, buf: &mut Option<&mut [u8]>) -> Reader {
        if let Some(ref mut current_chunk) = self.current_chunk {
            let data_read = match current_chunk {
                CurrentChunk::Downloading { read_data, _dl } => {
                    dbg!();
                    println!("DL at {}, len {}, end_offset {}", self.offset, len, read_data.end_offset);
                    if self.offset < read_data.end_offset {
                        // If we have any data before end_offset, read that first
                        println!("simple_read");
                        let max_read_len = min(len, (read_data.end_offset - self.offset) as usize);
                        let sr_res = Self::simple_read(&mut read_data.file, buf, max_read_len).await;
                        if self.offset + (sr_res as u64) == read_data.end_offset {
                            Reader::LastData(sr_res)
                        } else {
                            Reader::Data(sr_res)
                        }
                    } else {
                        // Otherwise, lock the OpenFile and try to read/download from there
                        let mut of = self.handle.lock().await;
                        println!("download_read");
                        Self::downloader_read(read_data, &mut of, buf, len, self.offset).await
                    }
                }
                CurrentChunk::Reading { read_data } => {
                    dbg!();
                    if self.offset < read_data.end_offset {
                        // If we have any data before end_offset, read that first
                        println!("simple_read");
                        let max_read_len = min(len, (read_data.end_offset - self.offset) as usize);
                        let sr_res = Self::simple_read(&mut read_data.file, buf, max_read_len).await;
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
            dbg!(&data_read);
            data_read
        } else {
            unreachable!("self.current_chunk cannot be None here");
        }
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
                prev_range,
            } => {
                if let Some(prev_range) = prev_range {
                    if self.offset == start_offset
                        && self.write_hard_cache == prev_range.data.is_hard_cache
                    {
                        // We're at the end of the previous chunk and have enough space to write
                        // And the previous chunk matches the cache type
                        // Download and append data to chunk
                        let chunk_start_offset = prev_range.data.chunk_start_offset;
                        self.start_append_dl(&mut of, start_offset, size, chunk_start_offset)
                            .await
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

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset: self.offset,
        };

        CurrentChunk::Downloading { read_data, _dl: dl }
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

        let read_data = ReadData {
            file,
            end_offset,
            chunk_start_offset,
        };

        CurrentChunk::Downloading { read_data, _dl: dl }
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
        let source_file_path = self.get_file_cache_chunk_path(is_hard_cache, chunk_start_offset);
        let source_file_offset = self.offset - start_offset;
        let source_file_end_offset = source_file_offset + size;

        let hc_file_path = self.get_file_cache_chunk_path(true, self.offset);
        let (file, dl) = of.start_copy_chunk(self.offset, &source_file_path, source_file_offset, source_file_end_offset, &hc_file_path).await.unwrap();

        let read_data = ReadData {
            file,
            end_offset: self.offset,
            chunk_start_offset: self.offset,
        };

        CurrentChunk::Downloading { read_data, _dl: dl }
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

        let read_data = ReadData {
            file,
            end_offset,
            chunk_start_offset,
        };

        CurrentChunk::Reading { read_data }
    }

    /// Read data from file, or ignore if we're caching.
    async fn simple_read(file: &mut File, buf: &mut Option<&mut [u8]>, len: usize) -> usize {
        dbg!(&len);
        if let Some(buf) = buf {
            file.read(&mut buf[..len]).await.unwrap()
        } else {
            // Since there's no point reading to nothing, just seek instead
            file.seek(SeekFrom::Current(len as i64)).await.unwrap();
            len
        }
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
    ) -> Reader {
        let chunk_mutex = of.get_receiver_status(read_data.chunk_start_offset);
        let mut chunk = chunk_mutex.lock().await;
        if let Some(DownloadStatus {
            receiver,
            last_bytes,
            write_file,
            end_offset,
            split_offset,
            dl_handle,
        }) = chunk.as_mut()
        {
                // Update local end_offset
                // TODO: make sure end_offset and read_data.end_offset are properly handled everywhere
                read_data.end_offset = *end_offset;
                if current_offset < read_data.end_offset {
                    // If we're not beyond DlStatus's end_offset, just read from disk.
                    // This happens if a different thread downloaded or read from last_bytes.
                    let max_read_len = min(len, (current_offset - read_data.end_offset) as usize);
                    let sr_res = Self::simple_read(&mut read_data.file, buf, max_read_len).await;
                    if current_offset + (sr_res as u64) == read_data.end_offset {
                        Reader::LastData(sr_res)
                    } else {
                        Reader::Data(sr_res)
                    }
                } else if !last_bytes.is_empty() {
                    // We're beyond end_offset, so if last_bytes is not empty, read from there.
                    Reader::Data(Self::read_from_last_bytes(read_data, of, last_bytes, buf, len, write_file, end_offset).await)
                } else {
                    // We're at end_offset and last_bytes is empty, so download a new last_bytes.
                    match receiver {
                        Receiver::Downloader(downloader) => {
                            let next_chunk: Result<Option<Bytes>, CacheHandlerError> =
                                downloader.next().await.transpose().map_err(|e| e.into());
                            if let Some(next_chunk) = next_chunk.unwrap() {
                                println!("nclen: {}", next_chunk.len());
                                if next_chunk.len() == 0 {
                                    Reader::NeedsNewChunk
                                } else {
                                    *last_bytes = next_chunk;
                                    println!("rflb1");
                                    Reader::Data(Self::read_from_last_bytes(read_data, of, last_bytes, buf, len, write_file, end_offset).await)
                                }
                            } else {
                                // KTODO: failure case?
                                Reader::NeedsNewChunk
                            }
                        }
                        Receiver::CacheReader(cache_reader, cache_end_offset) => {
                            // Terribly inefficient way of doing this, causing two allocations.
                            // Luckily this code isn't called very often, but there's definitely a
                            // cleaner way to do this.
                            let mut temp_buf = vec![0u8; 65536];
                            let read_len = cache_reader.read(&mut temp_buf).await.unwrap();
                            if read_len == 0 {
                                Reader::NeedsNewChunk
                            } else {
                                let mut next_chunk = BytesMut::with_capacity(65536);
                                next_chunk.extend_from_slice(&temp_buf[..read_len]);
                                *last_bytes = next_chunk.freeze();
                                println!("rflb2");
                                dbg!(&last_bytes.len());
                                let lb_res = Self::read_from_last_bytes(read_data, of, last_bytes, buf, len, write_file, end_offset).await;
                                if current_offset + (lb_res as u64) == *cache_end_offset {
                                    Reader::LastData(lb_res)
                                } else {
                                    Reader::Data(lb_res)
                                }
                            }
                        }
                    }
                }
        } else {
            // There is no DlStatus in this chunk, which means we're not downloading.
            // This most likely means the chunk was split, and the DlStatus is in the next chunk.

            // We need to get a new chunk.
            Reader::NeedsNewChunk
        }
    }

    async fn read_from_last_bytes(
        read_data: &mut ReadData,
        of: &mut OpenFile,
        last_bytes: &mut Bytes,
        buf: &mut Option<&mut [u8]>,
        len: usize,
        write_file: &mut File,
        end_offset: &mut u64,
    ) -> usize {
        // Read up to len in last_bytes, leaving the rest in last_bytes.
        let split_len = min(len, last_bytes.len());
        let from_last_bytes = last_bytes.split_to(split_len);

        // Write data to cache file.
        write_file.write_all(&from_last_bytes).await.unwrap();
        read_data.end_offset += from_last_bytes.len() as u64;
        *end_offset = read_data.end_offset;

        // If reading and not caching, also write data out to buf.
        if let Some(buf) = buf {
            buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
        }

        // Extend byte ranger to new length, so new threads know about
        // the newly-added data.
        of.update_chunk_size(read_data.chunk_start_offset, read_data.end_offset - read_data.chunk_start_offset);

        from_last_bytes.len()
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
    use timecode_rs::read_offset;

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

        let hard_cache_root = PathBuf::from("/home/main/greed_test/hard_cache");
        let soft_cache_root = PathBuf::from("/home/main/greed_test/soft_cache");

        let fsch = FilesystemCacheHandler::new(
            "main".to_string(),
            &hard_cache_root,
            &soft_cache_root,
            d.into(),
        );

        println!("SOFT CACHE");

        let mut offset = 0;

        let mut f = fsch
            .open_file(file_id.clone(), data_id.clone(), file_size, offset, false)
            .await
            .unwrap();

        loop {
            let mut buf = [0u8; 65535];
            let l = f.read_into(&mut buf).await;
            if l == 0 {
                break;
            }
            if l < 11 {
                offset += l as u64;
                continue;
            }
            let off = read_offset(&buf);
            dbg!(l);
            dbg!(&off);
            dbg!(&buf[..11]);
            assert_eq!(off.unwrap(), offset);
            offset += l as u64;
        }
    }
}
