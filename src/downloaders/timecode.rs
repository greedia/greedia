use std::{
    cmp::min,
    io::Read,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};
use timecode_rs::TimecodeReader;

use super::{DownloaderDrive, DownloaderError, Page};

/// A test drive that returns timecode files for testing offsets.
pub struct TimecodeDrive {}

#[async_trait]
impl DownloaderDrive for TimecodeDrive {
    fn scan_pages(
        &self,
        _last_page_token: Option<String>,
        _last_modified_date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Page, DownloaderError>>>> {
        // Should we use this to list out timecode files of different sizes?
        // Perhaps the file_id should encode the file size in it. Use JSON?
        Box::pin(stream::empty())
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
        dbg!(&params);

        let data_left = params.file_size.unwrap_or(default_file_size).saturating_sub(offset);
        let bytes_len = params.bytes_len.unwrap_or(1024);

        Ok(Box::new(TimecodeBytesStream {
            inner: timecode_file,
            bytes_len,
            data_left,
        }))
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
