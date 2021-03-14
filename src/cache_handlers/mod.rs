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
    #[error("Cache mismatch - write_hard_cache: {write_hard_cache} but is_hard_cache: {is_hard_cache} for start_offset: {start_offset}")]
    CacheMismatchError {
        write_hard_cache: bool,
        is_hard_cache: bool,
        start_offset: u64,
    },
    #[error("AppendChunkError at start_offset: {start_offset}")]
    AppendChunkError { start_offset: u64 },
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
    /// Read data into `buf` which is exactly the size of `len` (unless an EOF occurs).
    async fn read_exact(&mut self, buf: &mut [u8]) -> usize;
    /// Cache data to file without reading it.
    async fn cache_data(&mut self, len: usize) -> usize;
    /// Cache data to file without reading it, to exactly the size of `len` (unless an EOF occurs).
    async fn cache_exact(&mut self, len: usize) -> usize;
    /// Seek stream to `offset`.
    async fn seek_to(&mut self, offset: u64);
}
