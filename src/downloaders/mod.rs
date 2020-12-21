use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream};
use std::pin::Pin;
use thiserror::Error;

pub mod gdrive;

#[derive(Error, Debug)]
pub enum DownloaderError {
    #[error("Reqwest error")]
    ReqwestError(#[from] reqwest::Error),
}

pub trait DownloaderClient {
    fn open_drive(&self, drive_id: String) -> Box<dyn DownloaderDrive>;
}

#[async_trait]
pub trait DownloaderDrive: Sync + Send + 'static {
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Page, DownloaderError>>>>;

    async fn open_file(
        &self,
        file_id: String,
        offset: u64,
        bg_request: bool,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Unpin + Send>, DownloaderError>;
}

#[derive(Debug)]
pub struct Page {
    items: Vec<PageItem>,
    next_page_token: Option<String>,
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
    pub md5: String,
    pub size: u64,
}
