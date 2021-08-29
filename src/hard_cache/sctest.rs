use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
    path::PathBuf,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs::File, io::AsyncSeekExt};

use super::{HardCacheItem, HcCacher, HcCacherItem};
use crate::{cache_handlers::CacheHandlerError, config::DownloadAmount};
pub struct HcTestCacher {
    pub input: PathBuf,
    pub output: PathBuf,
    pub seconds: u64,
    pub fill_byte: Option<String>,
    pub fill_random: bool,
}

#[async_trait]
impl HcCacher for HcTestCacher {
    async fn get_item(&self, item: HardCacheItem) -> Box<dyn HcCacherItem + Send + Sync> {
        if !item.access_id.is_empty() {
            panic!("BUG: sctest access_id is not empty.");
        }

        let input = File::open(&self.input).await.unwrap();
        let output = File::create(&self.output).await.unwrap();
        Box::new(HcTestCacherItem {
            input,
            input_size: item.size,
            output,
            bridge_points: BTreeSet::new(),
            ranges_to_cache: BTreeMap::new(),
            bytes_downloaded: 0,
        })
    }
    fn generic_cache_sizes(&self) -> (DownloadAmount, DownloadAmount) {
        let start = DownloadAmount {
            percent: Some(0.5),
            bytes: Some(1_000_000),
        };
        let end = DownloadAmount {
            percent: Some(0.5),
            bytes: Some(1_000_000),
        };

        (start, end)
    }
}

#[cfg(feature = "sctest")]
struct HcTestCacherItem {
    input: File,
    input_size: u64,
    output: File,
    bridge_points: BTreeSet<u64>,
    ranges_to_cache: BTreeMap<u64, u64>,
    bytes_downloaded: u64,
}

#[cfg(feature = "sctest")]
#[async_trait]
impl HcCacherItem for HcTestCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Result<Vec<u8>, CacheHandlerError> {
        self.bridge_points.insert(offset + size);
        self.input.seek(SeekFrom::Start(offset)).await.unwrap();
        let mut buf = vec![0u8; size as usize];
        let mut buf_off = 0;

        // read_exact errors if a non-exact number of bytes is read, so we can't use it.
        loop {
            let bytes_read = self.input.read(&mut buf[buf_off..]).await?;
            if bytes_read == 0 {
                break;
            } else {
                buf_off += bytes_read;
            }
        }

        buf.truncate(buf_off);

        self.output.seek(SeekFrom::Start(offset)).await.unwrap();
        self.output.write_all(&buf).await.unwrap();
        self.bytes_downloaded += buf.len() as u64;
        Ok(buf)
    }
    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<Vec<u8>, CacheHandlerError> {
        let last_bridge_point =
            if let Some(last_bridge_point) = self.bridge_points.range(..offset).rev().next() {
                *last_bridge_point
            } else {
                0
            };

        if let Some(max_bridge_len) = max_bridge_len {
            if offset - last_bridge_point <= max_bridge_len {
                self.ranges_to_cache
                    .insert(last_bridge_point, offset - last_bridge_point);
            }
        } else {
            self.ranges_to_cache
                .insert(last_bridge_point, offset - last_bridge_point);
        }

        self.read_data(offset, size).await
    }
    async fn cache_data(&mut self, offset: u64, size: u64) -> Result<(), CacheHandlerError> {
        self.bytes_downloaded += size;
        self.ranges_to_cache.insert(offset, size);

        Ok(())
    }
    async fn cache_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Result<(), CacheHandlerError> {
        let last_bridge_point =
            if let Some(last_bridge_point) = self.bridge_points.range(..offset).rev().next() {
                *last_bridge_point
            } else {
                0
            };

        if let Some(max_bridge_len) = max_bridge_len {
            if offset - last_bridge_point <= max_bridge_len {
                self.ranges_to_cache
                    .insert(last_bridge_point, offset - last_bridge_point + size);
            }
        } else {
            self.ranges_to_cache
                .insert(last_bridge_point, offset - last_bridge_point + size);
        }

        Ok(())
    }
    async fn cache_data_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
        self.ranges_to_cache.insert(0, offset);

        Ok(())
    }

    async fn cache_data_fully(&mut self) -> Result<(), CacheHandlerError> {
        self.ranges_to_cache.insert(0, self.input_size);

        Ok(())
    }

    async fn save(&mut self) {
        dbg!(&self.ranges_to_cache);
        let mut buf = vec![];
        for (offset, size) in &self.ranges_to_cache {
            buf.resize(*size as usize, 0);
            self.input
                .seek(SeekFrom::Start(*offset))
                .await
                .expect("sctest seek failed");
            self.input
                .read_exact(&mut buf)
                .await
                .expect("sctest read_exact failed");
            self.output
                .seek(SeekFrom::Start(*offset))
                .await
                .expect("sctest seek failed");
            self.output
                .write_all(&buf)
                .await
                .expect("sctest write_all failed");
        }
        // Make sure file is full length
        self.output
            .seek(SeekFrom::Start(self.input_size - 1))
            .await
            .expect("sctest seek failed");
        self.output
            .write_all(&[0])
            .await
            .expect("sctest write_all failed");

        self.output.flush().await.expect("sctest flush failed");

        println!("Downloaded size is {}", self.bytes_downloaded);
    }
}
