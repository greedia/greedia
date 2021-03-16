pub mod filesystem;

use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;
use tokio::io;

pub use crate::downloaders::{DownloaderError, Page, PageItem};
use crate::types::DataIdentifier;

#[derive(Error, Debug)]
pub enum CacheHandlerError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Downloader Error")]
    DownloaderError(#[from] DownloaderError),
    #[error("AppendChunkError at start_offset: {start_offset}")]
    AppendChunkError { start_offset: u64 },
}

#[async_trait]
pub trait CacheDriveHandler {
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Page, DownloaderError>>>>;
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
pub trait CacheFileHandler {
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
