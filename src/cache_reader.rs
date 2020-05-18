use crate::{
    cache::{Cache, CacheFiles, ReaderFileData},
    downloader::{ReturnWhen, ToDownload, Downloader},
};
use anyhow::Result;
use async_trait::async_trait;
use std::{
    cmp::min,
    collections::BTreeMap,
    io::{SeekFrom, Write},
    sync::Arc,
};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[async_trait]
pub trait CacheRead {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Vec<u8>>;
}

pub struct CurrentFile {
    file: File,
    left: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CacheType {
    HardCache,
    SoftCache,
}

pub struct CacheReader {
    // Static data
    downloader: Arc<Downloader>,
    cache: Arc<Cache>,
    file_data: ReaderFileData,
    // Dynamic data
    offset: u64,
    chunk_size: u64,
    current_file: Option<CurrentFile>,
}

impl CacheReader {
    pub fn new(downloader: Arc<Downloader>, cache: Arc<Cache>, file_data: ReaderFileData) -> CacheReader {
        CacheReader {
            downloader,
            cache,
            file_data,
            offset: 0,
            chunk_size: 1_000_000,
            current_file: None,
        }
    }

    /// Perform one actual read call.
    pub async fn one_read<W: Write>(
        &mut self,
        offset: u64,
        size: u32,
        writer: &mut W,
    ) -> Result<usize> {
        if offset >= self.file_data.size {
            return Ok(0); // We're at the end of the file - return EOF
        }
        let next_file = if let Some(ref current_file) = self.current_file {
            if offset < self.offset - 1_000_000 || offset > self.offset + 1_000_000 {
                self.chunk_size = 1_000_000; // We've moved elsewhere in the file; reset the download chunk size
                                             // This chunk size is half of what the first chunk will be, since it's doubled before download
            }
            if current_file.left == 0 {
                self.offset = offset;
                Some(self.get_next_file(offset).await?)
            } else if self.offset != offset {
                self.offset = offset;
                Some(self.get_next_file(offset).await?)
            } else {
                None
            }
        } else {
            let new_file = self.get_next_file(offset).await?;
            Some(new_file)
        };

        if next_file.is_some() {
            self.current_file = next_file;
        }

        // At this point, assume current_file is correct
        let (bytes_read, left) = if let Some(ref mut current_file) = self.current_file {
            let to_read = size;
            // TODO: use std::io::copy instead of temp vec?
            let mut data = vec![0; to_read as usize];
            let bytes_read = current_file.file.read(&mut data).await?;
            self.offset += bytes_read as u64;
            data.truncate(bytes_read);
            current_file.left -= bytes_read as u64;
            writer.write(&data)?;
            (bytes_read, current_file.left)
        } else {
            println!("EOF reached?");
            (0, 0) // Reached end of file?
        };

        if left == 0 {
            self.current_file = None;
        }

        Ok(bytes_read)
    }

    pub async fn get_next_file(&mut self, offset: u64) -> Result<CurrentFile> {
        let (hard_cache_files, soft_cache_files) =
            self.cache.get_cache_files(&self.file_data.md5).await;

        if let Some(current_file) = self
            .open_cache_file_at_offset(&hard_cache_files, offset)
            .await?
        {
            Ok(current_file)
        } else if let Some(current_file) = self
            .open_cache_file_at_offset(&soft_cache_files, offset)
            .await?
        {
            Ok(current_file)
        } else {
            self.chunk_size = min(100_000_000, self.chunk_size * 2);
            self.download_cache_file_at_offset(
                &hard_cache_files,
                &soft_cache_files,
                offset,
                self.chunk_size,
            ).await
        }
    }

    async fn open_cache_file_at_offset(
        &self,
        cache_files: &CacheFiles,
        offset: u64,
    ) -> Result<Option<CurrentFile>> {
        /*println!(
            "OF '{}' {} @ {}",
            self.file_data.name, self.file_data.md5, offset
        );*/
        let CacheFiles {
            cache_type,
            cache_files,
        } = cache_files;
        if let Some((file, offset, left)) = get_file_and_left(&cache_files, offset) {
            let path = self
                .cache
                .get_cache_dir_path(&cache_type, &self.file_data.md5)
                .join(format!("chunk_{}", file));

            if path.exists() {
                if cache_type == &CacheType::SoftCache {
                    self.cache.touch_soft_cache_file(&self.file_data.md5, file)
                }

                let mut file = File::open(&path).await?;
                file.seek(SeekFrom::Start(offset)).await?;
                Ok(Some(CurrentFile { file, left }))
            } else {
                println!("Path does not exist");
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn download_cache_file_at_offset(
        &self,
        hard_cache_files: &CacheFiles,
        soft_cache_files: &CacheFiles,
        offset: u64,
        size: u64,
    ) -> Result<CurrentFile> {
        println!(
            "DL '{}' {} @ {} ({} bytes)",
            self.file_data.info_inode, self.file_data.md5, offset, size
        );
        // Get the next file, and only download up to it
        let next_hc_file = hard_cache_files
            .cache_files
            .range(offset - 1..)
            .next()
            .map(|(file, _)| file);
        let next_sc_file = soft_cache_files
            .cache_files
            .range(offset - 1..)
            .next()
            .map(|(file, _)| file);
        let next_file = match (next_hc_file, next_sc_file) {
            (Some(hc), None) => *hc,
            (None, Some(sc)) => *sc,
            (Some(hc), Some(sc)) => *min(hc, sc),
            (None, None) => self.file_data.size - offset,
        };

        let dl_size = min(size, next_file - offset);

        let path = self
            .cache
            .get_cache_dir_path(&CacheType::SoftCache, &self.file_data.md5)
            .join(format!("chunk_{}", offset));

        self.cache
            .add_soft_cache_file(&self.file_data.md5, offset, dl_size);

        self.downloader.cache_data(
            self.file_data.file_id.clone(),
            path.clone(),
            ReturnWhen::Finished,
            ToDownload::Range(offset, offset + dl_size),
        ).await?;

        let file = File::open(&path).await?;
        let left = path.metadata().unwrap().len();
        Ok(CurrentFile { file, left })
    }
}

#[async_trait]
impl CacheRead for CacheReader {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(size as usize);
        let mut cur_offset = offset;
        let mut size_left = size;
        loop {
            let bytes_read = self.one_read(cur_offset, size_left, &mut output).await?;
            if bytes_read == 0 {
                return Ok(output);
            }
            cur_offset += bytes_read as u64;
            size_left -= bytes_read as u32;
        }
    }
}

/// Given a cache map and an offset, figure out which file to read (or None if offset isn't within a file)
/// and return the file to open, as well as the offset to seek to and the data left
fn get_file_and_left(cache_map: &BTreeMap<u64, u64>, offset: u64) -> Option<(u64, u64, u64)> {
    let first_file = cache_map.range(..=offset).next_back();
    first_file.and_then(|(file, len)| {
        if offset < file + len {
            let cache_offset = offset - file;
            let left = len - cache_offset;
            Some((*file, cache_offset, left))
        } else {
            None
        }
    })
}
