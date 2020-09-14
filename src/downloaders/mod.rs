use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{AsyncWrite, Stream};
use std::pin::Pin;

pub mod gdrive;

#[derive(Debug)]
struct DownloaderError {}

trait DownloaderClient {
    fn open_drive(&self, drive_id: String) -> Box<dyn DownloaderDrive>;
}

#[async_trait]
trait DownloaderDrive {
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
    ) -> Result<Box<dyn DownloaderFile>, DownloaderError>;
}

#[async_trait]
trait DownloaderFile {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize;
    async fn read_bytes(&mut self, len: usize) -> Bytes;
    async fn read_exact(&mut self, len: usize) -> Bytes;
    async fn cache(&mut self, len: usize, writer: &mut (dyn AsyncWrite + Send));
    async fn remaining_data(&mut self) -> Bytes;
}

#[derive(Debug)]
struct Page {
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
