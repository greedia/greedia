// Filesystem cache handler

use std::{
    cmp::min, collections::HashMap, io::SeekFrom, mem, path::Path, path::PathBuf, sync::Arc,
    sync::Weak,
};

use super::{CacheDriveHandler, CacheFileHandler, CacheHandlerError};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use byte_ranger::Scan;
use bytes::Bytes;
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}, sync::{Mutex, MutexGuard}};

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
        /// File to write to
        file: File,
        /// Handle for downloading from provider
        dl: DownloadHandle,
        /// Cache of the last Bytes object
        last_bytes: Bytes,
        /// Where to stop writing (to keep a maximum cache file size)
        max_offset: u64,
    },
    Reading {
        file: File,
        end_offset: u64,
    },
}

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        // TODO: short-circuit EOF read
        println!("read bytes len {} offset {}", buf.len(), self.offset);

        match &mut self.current_chunk {
            Some(CurrentChunk::Downloading {
                file,
                dl,
                last_bytes,
                max_offset,
            }) => {
                // Write and fill up buf with rest of last_bytes, if possible
                let from_last_bytes = last_bytes.split_to(buf.len());
                // TODO: swap write_all with write (or even better, a buffer)
                file.write_all(&from_last_bytes).await.unwrap();
                buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);

                if from_last_bytes.len() == buf.len() {
                    return from_last_bytes.len();
                }

                // TODO: short-circuit EOF read

                // Download one more Bytes object if we can
                if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
                    let to_output = b.split_to(buf.len() - from_last_bytes.len());

                    // TODO: swap write_all with write (or even better, a buffer)
                    file.write_all(&to_output).await.unwrap();
                    buf[from_last_bytes.len()..to_output.len()].copy_from_slice(&to_output);
                    *last_bytes = b;
                    self.offset += to_output.len() as u64;
                    return to_output.len();
                } else {
                    todo!("EOF?");
                }
            }
            Some(CurrentChunk::Reading { file, end_offset }) => {
                // TODO: check data chunkstatus if chunk is being downloaded
                let max_data_to_read = *end_offset - self.offset;
                let data_to_read = min(buf.len() as u64, max_data_to_read) as usize;

                let bytes_read = file.read(&mut buf[..data_to_read]).await.unwrap();
                self.offset += bytes_read as u64;

                if *end_offset == self.offset {
                    self.current_chunk = None;
                }

                return bytes_read;
            }
            None => {
                let mut of = self.handle.lock().await;
                let chunk = of.get_chunk_at(self.offset, self.write_hard_cache).await;
                dbg!(&chunk);
                match chunk {
                    Some(Scan::Data {
                        start_offset,
                        size,
                        data,
                    }) => {
                        // TODO: check data chunkstatus if chunk is being downloaded
                        let file_path =
                            self.get_file_cache_chunk_path(self.write_hard_cache, start_offset);

                        dbg!(&self.offset, start_offset, size);

                        // Chunk file exists and there's data we can read from it
                        let chunk_relative_offset = self.offset - start_offset;
                        let max_data_to_read = size - chunk_relative_offset;
                        let data_to_read = min(buf.len() as u64, max_data_to_read) as usize;

                        // Actually read the data
                        dbg!(&file_path);
                        let mut file = File::open(file_path).await.unwrap();
                        file.seek(SeekFrom::Start(chunk_relative_offset))
                            .await
                            .unwrap();
                        let bytes_read = file.read(&mut buf[..data_to_read]).await.unwrap();
                        self.offset += bytes_read as u64;

                        // Save chunk status
                        let end_offset = start_offset + size;
                        if end_offset != self.offset {
                            self.current_chunk =
                                Some(CurrentChunk::Reading { file, end_offset });
                        }

                        return bytes_read;

                        // TODO: also handle when download is already in progress
                    }
                    Some(Scan::FinalRange {
                        start_offset,
                        size,
                    }) => {
                        // KTODO 1
                        // TODO: check data chunkstatus if chunk is being downloaded
                        let file_path =
                            self.get_file_cache_chunk_path(self.write_hard_cache, start_offset);

                        let (mut file, mut dl) = of
                            .append_download_chunk(
                                start_offset,
                                start_offset + size,
                                self.write_hard_cache,
                                &file_path,
                            )
                            .await
                            .unwrap();
                        if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
                            let to_output = b.split_to(buf.len());

                            file.write_all(&to_output).await.unwrap();
                            buf[..to_output.len()].copy_from_slice(&to_output);
                            let last_bytes = b;
                            let max_offset = 200_000_000; // TODO: not hardcode
                            self.current_chunk = Some(CurrentChunk::Downloading {
                                file,
                                dl,
                                last_bytes,
                                max_offset,
                            });
                            self.offset += to_output.len() as u64;
                            return to_output.len();
                        } else {
                            todo!("EOF?");
                        }

                        // max_offset is max - (start_offset + self.offset)
                    }
                    Some(Scan::Gap {
                        start_offset,
                        size
                    }) => {
                        // KTODO 2
                        // KTODO 3: reorganize this function into smaller functions
                        // Handle append_download
                        // max_offset is start_offset + size
                        todo!()
                    }
                    None | Some(Scan::Empty) => {
                        // Chunk file does not exist
                        let file_path =
                            self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);
                        let (mut file, mut dl) = of
                            .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
                            .await
                            .unwrap();

                        if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
                            let to_output = b.split_to(buf.len());

                            file.write_all(&to_output).await.unwrap();
                            buf[..to_output.len()].copy_from_slice(&to_output);
                            let last_bytes = b;
                            let max_offset = 200_000_000; // TODO: not hardcode
                            self.current_chunk = Some(CurrentChunk::Downloading {
                                file,
                                dl,
                                last_bytes,
                                max_offset,
                            });
                            self.offset += to_output.len() as u64;
                            return to_output.len();
                        } else {
                            todo!("EOF?");
                        }
                    }
                }
            }
        }
    }

    async fn cache_data(&mut self, len: usize) -> usize {
        // Copy from read_into into new function which takes Option<buf>
        // and call that from cache_data and read_into
        todo!()
    }

    async fn read_bytes(&mut self, len: usize) -> Bytes {
        // Call read_into once, return as Bytes object
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
    async fn start_new_chunk(&mut self, of: &mut MutexGuard<'_, OpenFile>, buf: &mut [u8]) -> usize {
        let file_path =
            self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);
        
        let (mut file, mut dl) = of
            .start_download_chunk(self.offset, self.write_hard_cache, &file_path)
            .await
            .unwrap();

        if let Some(mut b) = dl.get_next_bytes().await.unwrap() {
            let to_output = b.split_to(buf.len());

            file.write_all(&to_output).await.unwrap();
            buf[..to_output.len()].copy_from_slice(&to_output);
            let last_bytes = b;
            let max_offset = 200_000_000; // TODO: not hardcode
            self.current_chunk = Some(CurrentChunk::Downloading {
                file,
                dl,
                last_bytes,
                max_offset,
            });
            self.offset += to_output.len() as u64;
            return to_output.len();
        } else {
            todo!("EOF?");
        }
    }

    fn append_chunk(&mut self, buf: &mut u8) -> usize {
        todo!()
    }

    fn continue_dl(&mut self, buf: &mut u8) -> usize {
        todo!()
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
            .open_file(file_id, data_id, file_size, 64, false)
            .await
            .unwrap();

        let mut buf = [0u8; 16];
        let x = f.read_into(&mut buf).await;
        dbg!(&x, &buf[..x]);

        let x = f.read_into(&mut buf).await;
        dbg!(&x, &buf[..x]);

        let x = f.read_into(&mut buf).await;
        dbg!(&x, &buf[..x]);

        let x = f.read_into(&mut buf).await;
        dbg!(&x, &buf[..x]);

        f.seek_to(256).await;

        let x = f.read_into(&mut buf).await;
        dbg!(&x, &buf[..x]);
    }
}
