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
    ReqwestError(#[from] reqwest::Error),
}

pub trait DownloaderClient {
    fn open_drive(&self, drive_id: &str) -> Box<dyn DownloaderDrive>;
}

#[async_trait]
pub trait DownloaderDrive: Sync + Send + 'static {
    fn get_drive_type(&self) -> &'static str;
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin>;

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
