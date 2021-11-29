use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use thiserror::Error;

use crate::db::types::DataIdentifier;

pub mod gdrive;
pub mod timecode;

#[derive(Error, Debug)]
pub enum DownloaderError {
    #[error("Reqwest error")]
    Reqwest(#[from] reqwest::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
    #[error("Download quota for this file has been exceeded")]
    QuotaExceeded,
    #[error("Range not satisfiable ({})", _0)]
    RangeNotSatisfiable(String),
    #[error("Could not initialize or renew access token")]
    AccessToken,
    #[error("Server error: {}", _0)]
    Server(String),
    #[error("Forbidden error")]
    Forbidden,
    #[error("Operation is unimplemented")]
    Unimplemented,
    #[error("Unknown downloader retry error")]
    Retry,
    #[error("File was not found")]
    NotFound,
}

pub trait DownloaderClient {
    fn open_drive(&self, drive_id: &str) -> Box<dyn DownloaderDrive>;
}

/// Download-side implementation of a drive.
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

    /// Move or rename a file.
    ///
    /// file_id: ID of the existing file.
    ///
    /// new_filename: New filename, if name change is desired.
    ///
    /// new_parent: New parent, if moving the file is desired.
    async fn move_file(
        &self,
        file_id: String,
        new_parent_id: Option<(String, String)>,
        new_file_name: Option<String>,
    ) -> Result<(), DownloaderError>;

    /// Delete a file.
    async fn delete_file(&self, file_id: String) -> Result<(), DownloaderError>;
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
