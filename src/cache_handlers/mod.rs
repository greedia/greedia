mod filesystem;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use tokio::io;

use crate::{downloaders::DownloaderError, types::DataIdentifier};

#[derive(Error, Debug)]
pub enum CacheHandlerError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Downloader Error")]
    DownloaderError(#[from] DownloaderError),
}

#[async_trait]
trait CacheDriveHandler {
    async fn open_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
        size: u64,
        offset: u64,
        write_hard_cache: bool,
    ) -> Result<Box<dyn CacheFileHandler>, CacheHandlerError>;
}

#[async_trait]
trait CacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize;
    async fn read_bytes(&mut self, len: usize) -> Bytes;
    async fn read_exact(&mut self, len: usize) -> Bytes;
}
