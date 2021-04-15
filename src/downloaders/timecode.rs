use std::{
    cmp::min,
    io::Read,
    pin::Pin,
    task::{Context, Poll},
};

use chrono::Utc;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};
use timecode_rs::TimecodeReader;
use crate::types::DataIdentifier;

use super::{DownloaderDrive, DownloaderError, Page, PageItem, FileInfo};

/// A test drive that returns timecode files for testing offsets.
pub struct TimecodeDrive {
    pub root_name: String,
}

#[async_trait]
impl DownloaderDrive for TimecodeDrive {
    fn scan_pages(
        &self,
        _last_page_token: Option<String>,
        _last_modified_date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin> {
        // Should we use this to list out timecode files of different sizes?
        // Perhaps the file_id should encode the file size in it. Use JSON?
        Box::new(stream::iter(vec![
            Ok(Page{
                items: vec![
                    PageItem{
                        id: r#"{"bytes_len": 65535}"#.to_string(),
                        name: "timecode.bin".to_string(),
                        parent: self.root_name.clone(),
                        modified_time: Utc::now(),
                        file_info: Some(FileInfo {
                            data_id: DataIdentifier::GlobalMd5(vec![0,0,0,0]),
                            size: 1_073_741_824
                        })
                    }
                ],
                next_page_token: None,
            })
        ]))
    }

    async fn open_file(
        &self,
        file_id: String,
        offset: u64,
        _bg_request: bool,
    ) -> Result<
        Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Send + Sync + Unpin>,
        DownloaderError,
    > {
        let timecode_file = timecode_rs::get_timecode(offset);

        let params: TimecodeParams =
            serde_json::from_str(&file_id).expect("Unable to parse file ID, should be JSON.");
        let default_file_size = 1024u64.pow(3) * 10; // 10 GB
                                                     // dbg!(&params);

        let data_left = params
            .file_size
            .unwrap_or(default_file_size)
            .saturating_sub(offset);
        let bytes_len = params.bytes_len.unwrap_or(1024);

        let stream = Box::new(TimecodeBytesStream {
            inner: timecode_file,
            bytes_len,
            data_left,
        });

        println!("TIMECODESTREAM NEW  {:p}", &*stream);

        Ok(stream)
    }

    fn get_drive_type(&self) -> &'static str {
        "timecode"
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TimecodeParams {
    bytes_len: Option<u64>,
    file_size: Option<u64>,
}

pub struct TimecodeBytesStream {
    inner: TimecodeReader,
    bytes_len: u64,
    data_left: u64,
}

impl Stream for TimecodeBytesStream {
    type Item = Result<Bytes, DownloaderError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let read_amount = min(self.bytes_len, self.data_left) as usize;
        if read_amount == 0 {
            return Poll::Ready(None);
        }

        let mut bytes_mut = BytesMut::with_capacity(read_amount);
        bytes_mut.resize(read_amount, 0);
        let r_len = self.inner.read(&mut bytes_mut[..read_amount]).unwrap();
        self.data_left -= r_len as u64;

        Poll::Ready(Some(Ok(bytes_mut.freeze())))
    }
}

impl Drop for TimecodeBytesStream {
    fn drop(&mut self) {
        println!("TIMECODESTREAM DROP {:p}", self);
    }
}
