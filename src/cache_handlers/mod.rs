pub mod crypt_context;
pub mod crypt_passthrough;
pub mod filesystem;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;
use tokio::io;

pub use crate::downloaders::{DownloaderError, Page, PageItem};
use crate::{db::types::DataIdentifier, downloaders::Change, hard_cache::HardCacheMetadata};

#[derive(Error, Debug)]
pub enum CacheHandlerError {
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Serde JSON Error")]
    Serde(#[from] serde_json::Error),
    #[error("Downloader Error")]
    Downloader(#[from] DownloaderError),
    #[error("AppendChunkError at start_offset: {start_offset}")]
    AppendChunk { start_offset: u64 },
    #[error("Could not deserialize DataID")]
    Deserialize,
    #[error("Could not build CryptPassthrough")]
    CryptPassthrough,
    #[error("HANDLE_INTO Error")]
    HandleInto,
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
    async fn unlink_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
    ) -> Result<(), CacheHandlerError>;
    async fn rename_file(
        &self,
        file_id: String,
        old_new_parent_ids: Option<(String, String)>,
        new_file_name: Option<String>,
    ) -> Result<(), CacheHandlerError>;
    async fn get_cache_metadata(
        &self,
        data_id: DataIdentifier,
    ) -> Result<HardCacheMetadata, CacheHandlerError>;
    async fn set_cache_metadata(
        &self,
        data_id: DataIdentifier,
        metadata: HardCacheMetadata,
    ) -> Result<(), CacheHandlerError>;
    async fn clear_cache_item(&self, data_id: DataIdentifier);
}

#[async_trait]
pub trait CacheFileHandler: Send + Sync {
    /// Read data into `buf`.
    async fn read_into(&mut self, buf: &mut [u8]) -> Result<usize, CacheHandlerError>;
    /// Read data into `buf` which is exactly the size of `len` (unless an EOF occurs).
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, CacheHandlerError> {
        // Call read_into repeatedly until correct length, or EOF
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.read_into(&mut buf[total_bytes_read..]).await?;
            if bytes_read == 0 {
                return Ok(total_bytes_read);
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == buf.len() {
                    return Ok(total_bytes_read);
                }
            }
        }
    }
    /// Cache data to file without reading it.
    async fn cache_data(&mut self, len: usize) -> Result<usize, CacheHandlerError>;
    /// Cache data to file without reading it, to exactly the size of `len` (unless an EOF occurs).
    async fn cache_exact(&mut self, len: usize) -> Result<usize, CacheHandlerError> {
        let mut total_bytes_read = 0;

        loop {
            let bytes_read = self.cache_data(len - total_bytes_read).await?;
            if bytes_read == 0 {
                return Ok(total_bytes_read);
            } else {
                total_bytes_read += bytes_read;
                if total_bytes_read == len {
                    return Ok(total_bytes_read);
                }
            }
        }
    }
    /// Seek stream to `offset`.
    async fn seek_to(&mut self, offset: u64) -> Result<(), CacheHandlerError>;
}
