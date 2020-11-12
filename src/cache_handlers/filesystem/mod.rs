// Filesystem cache handler

use std::{
    collections::HashMap, path::Path, path::PathBuf, sync::Arc, sync::Weak,
};

use super::{CacheDriveHandler, CacheError, CacheFileHandler};
use crate::downloaders::DownloaderDrive;
use crate::types::DataIdentifier;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;

mod open_file;
use open_file::OpenFile;

struct FilesystemCacheHandler {
    drive_id: String,
    hard_cache_root: PathBuf,
    soft_cache_root: PathBuf,
    open_files: Mutex<HashMap<String, Weak<Mutex<OpenFile>>>>,
}

// TODO: create OpenFileStruct that contains gc_count for garbage collection
// put whole OpenFileStruct within Mutex

impl FilesystemCacheHandler {
    fn new(
        drive_id: String,
        hard_cache_root: &Path,
        soft_cache_root: &Path,
        downloader: Box<dyn DownloaderDrive>,
    ) -> Box<dyn CacheDriveHandler> {
        let open_files = Mutex::new(HashMap::new());
        let hard_cache_root = hard_cache_root.to_path_buf();
        let soft_cache_root = soft_cache_root.to_path_buf();
        Box::new(FilesystemCacheHandler {
            drive_id,
            hard_cache_root,
            soft_cache_root,
            open_files,
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
        OpenFile::new(
            &self.hard_cache_root,
            &self.soft_cache_root,
            file_id,
            data_id,
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
    ) -> Result<Box<dyn CacheFileHandler>, CacheError> {
        let handle = self.get_open_handle(&file_id, &data_id).await;

        Ok(Box::new(FilesystemCacheFileHandler {
            file_id,
            handle,
            size,
            offset,
            hard_cache: true, // TODO: not hardcode hard cache
        }))
    }
}

struct FilesystemCacheFileHandler {
    file_id: String,
    handle: Arc<Mutex<OpenFile>>,
    size: u64,
    offset: u64,
    hard_cache: bool,
}

#[async_trait]
impl CacheFileHandler for FilesystemCacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        todo!()
    }

    async fn read_bytes(&mut self, len: usize) -> Bytes {
        println!("read bytes len {} offset {}", len, self.offset);
        let of = self.handle.lock().await;
        let hsc = of.hard_cache.scan_range(self.offset, len as u64);
        dbg!(&hsc);
        todo!()
    }

    async fn read_exact(&mut self, len: usize) -> Bytes {
        todo!()
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

        let hard_cache_root = PathBuf::from("/home/main/greed_test/hard_cache");
        let soft_cache_root = PathBuf::from("/home/main/greed_test/soft_cache");

        let fsch =
            FilesystemCacheHandler::new("main".to_string(), &hard_cache_root, &soft_cache_root, d);
        let mut f = fsch
            .open_file(file_id, data_id, file_size, 0)
            .await
            .unwrap();

        let x = f.read_bytes(4).await;

        dbg!(&x);
    }
}
