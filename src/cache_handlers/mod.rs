pub mod crypt_passthrough;
pub mod filesystem;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;
use tokio::io;

pub use crate::downloaders::{DownloaderError, Page, PageItem};
use crate::{downloaders::Change, types::DataIdentifier};

#[derive(Error, Debug)]
pub enum CacheHandlerError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Downloader Error")]
    DownloaderError(#[from] DownloaderError),
    #[error("AppendChunkError at start_offset: {start_offset}")]
    AppendChunkError { start_offset: u64 },
    #[error("Could not deserialize DataID")]
    DeserializeError,
    #[error("Could not build CryptPassthrough")]
    CryptPassthroughError,
}

impl CacheHandlerError {
    pub fn into_tokio_io_error(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, Box::new(self))
    }
}

#[async_trait]
pub trait CacheDriveHandler: Send + Sync {
    fn get_drive_type(&self) -> &'static str;
    fn get_drive_id(&self) -> String;
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin>;
    fn watch_changes(
        &self,
    ) -> Box<dyn Stream<Item = Result<Vec<Change>, DownloaderError>> + Send + Sync + Unpin>;
    async fn open_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
        size: u64,
        offset: u64,
        write_hard_cache: bool,
    ) -> Result<Box<dyn CacheFileHandler>, CacheHandlerError>;
    async fn clear_cache_item(&self, data_id: DataIdentifier);
}

#[async_trait]
pub trait CacheFileHandler: Send + Sync {
    /// Read data into `buf`.
    async fn read_into(&mut self, buf: &mut [u8]) -> usize;
    /// Read data into `buf` which is exactly the size of `len` (unless an EOF occurs).
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
    /// Cache data to file without reading it.
    async fn cache_data(&mut self, len: usize) -> usize;
    /// Cache data to file without reading it, to exactly the size of `len` (unless an EOF occurs).
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
    /// Seek stream to `offset`.
    async fn seek_to(&mut self, offset: u64);
}
