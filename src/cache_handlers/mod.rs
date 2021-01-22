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
    /// Read data into `buf`.
    async fn read_into(&mut self, buf: &mut [u8]) -> usize;
    /// Cache data to file without reading it.
    async fn cache_data(&mut self, len: usize) -> usize;
    /// Read data into Bytes object, which may end up less than `len`.
    async fn read_bytes(&mut self, len: usize) -> Bytes;
    /// Read data into Bytes object which is exactly the size of `len` (unless an EOF occurs).
    async fn read_exact(&mut self, len: usize) -> Bytes;
    /// Seek stream to `offset`.
    async fn seek_to(&mut self, offset: u64);
}
