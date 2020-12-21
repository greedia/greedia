// Filesystem cache handler

use std::{collections::HashMap, path::Path, path::PathBuf, sync::Arc, sync::Weak};

use super::{CacheDriveHandler, CacheHandlerError, CacheFileHandler};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use byte_ranger::Scan;
use bytes::Bytes;
use tokio::{fs::File, sync::Mutex};

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
    Downloading { file: File, dl: DownloadHandle },
    Reading { file: File, end_offset: u64 },
    }

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        println!("read bytes len {} offset {}", buf.len(), self.offset);

        match &mut self.current_chunk {
            Some(CurrentChunk::Downloading { file, dl }) => todo!(),
            Some(CurrentChunk::Reading { file, end_offset }) => todo!(),
            None => {
                let mut of = self.handle.lock().await;
                let chunk = of.get_chunk_at(self.offset).await;
                dbg!(&chunk);
                let file_path = self.get_file_cache_chunk_path(self.write_hard_cache, self.offset);
                if let Some(Scan::Data{ start_offset, size, data }) = chunk {
                    println!("CHUNK!");
                    // TODO: look closely at this section. Here be dragons.
                    // This logic is probably incorrect.
                    // Really, we want to check if we're at the end of the chunk, rather than check if the size is 0.
                    // TODO: also handle when download is already in progress
                    if size == 0 {
                        let (file, mut dl) = of.append_download_chunk(start_offset, start_offset + size, self.write_hard_cache, &file_path).await.unwrap();
                        let bytes = dl.get_next_bytes().await.unwrap();
                }
                
                    todo!();
                } else {
                    let (file, dl) = of.start_download_chunk(self.offset, self.write_hard_cache, &file_path).await.unwrap();
                    
                    
                    todo!();
                    // TODO: this next.
            }
        }
        }

        // If current chunk exists
          // If data is left in current chunk
            // Read data
          // Else if current chunk at end
            // If more data has been downloaded
              // Update current chunk
              // Drop openfile lock
              // Read data
            // Else
              // If next chunk is sequential
                // Open next chunk as current chunk
                // Drop openfile lock
                // Read data
              // Else
                // Start downloading data
                // TODO figure this out
        // Else
          // Lock openfile
          // Get chunk at offset
          
        todo!()
    }

    async fn read_bytes(&mut self, len: usize) -> Bytes {
        todo!()
    }

    async fn read_exact(&mut self, len: usize) -> Bytes {
        // Call read_bytes repeatedly until correct length, or EOF
        todo!()
    }
}

impl FilesystemCacheFileHandler {
    fn get_file_cache_chunk_path(&self, hard_cache: bool, offset: u64) -> PathBuf {
        if hard_cache {
            &self.hard_cache_file_root
        } else {
            &self.soft_cache_file_root
        }.join(format!("chunk_{}", offset))
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

        let mut buf = [0u8; 4];
        let x = f.read_into(&mut buf).await;

        dbg!(&x);
    }
}
