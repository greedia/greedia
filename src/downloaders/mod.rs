use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;

use crate::types::DataIdentifier;

pub mod gdrive;
pub mod timecode;

#[derive(Error, Debug)]
pub enum DownloaderError {
    #[error("Reqwest error")]
    Reqwest(#[from] reqwest::Error),
    #[error("Download quota for this file has been exceeded")]
    QuotaExceeded
}

pub trait DownloaderClient {
    fn open_drive(&self, drive_id: &str) -> Box<dyn DownloaderDrive>;
}

#[async_trait]
pub trait DownloaderDrive: Sync + Send + 'static {
    /// Get the downloader type.
    fn get_downloader_type(&self) -> &'static str;

    /// Perform an initial scan of files in the drive, for at startup.
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin>;

    /// Continuously watch for file changes over time.
    fn watch_changes(
        &self,
    ) -> Box<dyn Stream<Item = Result<Vec<Change>, DownloaderError>> + Send + Sync + Unpin>;

    /// Open a download thread at a specific offset for this downloader.
    async fn open_file(
        &self,
        file_id: String,
        offset: u64,
        bg_request: bool,
    ) -> Result<
        Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Sync + Unpin>,
        DownloaderError,
    >;
}

#[derive(Debug)]
pub struct Page {
    pub items: Vec<PageItem>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PageItem {
    pub id: String,
    pub name: String,
    pub parent: String,
    pub modified_time: DateTime<Utc>,
    pub file_info: Option<FileInfo>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub data_id: DataIdentifier,
    pub size: u64,
}

#[derive(Debug)]
pub enum Change {
    Added(PageItem), // Could also represent changed item, e.g. moved directories?
    Removed(String), // File ID?
}
