mod filesystem;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::DataIdentifier;

#[derive(Debug)]
struct CacheError {}

#[async_trait]
trait CacheDriveHandler {
    async fn open_file(
        &self,
        file_id: String,
        data_id: DataIdentifier,
        size: u64,
        offset: u64,
    ) -> Result<Box<dyn CacheFileHandler>, CacheError>;
}

#[async_trait]
trait CacheFileHandler {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize;
    async fn read_bytes(&mut self, len: usize) -> Bytes;
    async fn read_exact(&mut self, len: usize) -> Bytes;
}
