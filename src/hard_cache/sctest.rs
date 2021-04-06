use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
    path::PathBuf,
};
use tokio::{fs::File, io::AsyncSeekExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{HardCacheItem, HcCacher, HcCacherItem};
use crate::config::DownloadAmount;
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
}

#[cfg(feature = "sctest")]
#[async_trait]
impl HcCacherItem for HcTestCacherItem {
    async fn read_data(&mut self, offset: u64, size: u64) -> Vec<u8> {
        //println!("read_data {} {}", offset, size);
        self.bridge_points.insert(offset + size);
        self.input.seek(SeekFrom::Start(offset)).await.unwrap();
        let mut buf = vec![0u8; size as usize];
        self.input.read_exact(&mut buf).await.unwrap();
        self.output.seek(SeekFrom::Start(offset)).await.unwrap();
        self.output.write_all(&buf).await.unwrap();
        buf
    }
    async fn read_data_bridged(
        &mut self,
        offset: u64,
        size: u64,
        max_bridge_len: Option<u64>,
    ) -> Vec<u8> {
        //println!("read_data_bridged {} {} {:?}", offset, size, max_bridge_len);
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
    async fn cache_data(&mut self, offset: u64, size: u64) {
        //println!("cache_data {} {}", offset, size);
        self.ranges_to_cache.insert(offset, size);
    }
    async fn cache_data_bridged(&mut self, offset: u64, size: u64, max_bridge_len: Option<u64>) {
        // println!(
        //     "cache_data_bridged {} {} {:?}",
        //     offset, size, max_bridge_len
        // );
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
    }
    async fn cache_data_to(&mut self, offset: u64) {
        // println!("cache_data_to {}", offset);
        self.ranges_to_cache.insert(0, offset);
    }

    async fn cache_data_fully(&mut self) {
        // println!("cache_data_fully");
        self.ranges_to_cache.insert(0, self.input_size);
    }

    async fn save(&mut self) {
        dbg!(&self.ranges_to_cache);
        let mut buf = vec![];
        for (offset, size) in &self.ranges_to_cache {
            buf.resize(*size as usize, 0);
            // println!("bufsize {}, offset {}, size {}", buf.len(), offset, size);
            self.input.seek(SeekFrom::Start(*offset)).await.unwrap();
            self.input.read_exact(&mut buf).await.unwrap();
            self.output.seek(SeekFrom::Start(*offset)).await.unwrap();
            self.output.write_all(&buf).await.unwrap();
        }
        self.output.flush().await.unwrap();
    }
}
