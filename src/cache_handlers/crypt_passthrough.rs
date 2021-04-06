use crate::crypt_context::CryptContext;

use super::CacheFileHandler;
use async_trait::async_trait;
use bytes::Bytes;
use rclone_crypt::decrypter::{self, Decrypter};

pub struct CryptPassthrough {
    decrypter: Decrypter,
    reader: Box<dyn CacheFileHandler>,
    inner_offset: u64,
    outer_offset: u64,
    last_block: u64,
    last_bytes: Option<Bytes>,
}

impl CryptPassthrough {
    pub async fn new(ctx: &CryptContext, mut reader: Box<dyn CacheFileHandler>) -> Option<CryptPassthrough> {
        reader.seek_to(0).await;
        let mut header = [0u8; decrypter::FILE_HEADER_SIZE];
        reader.read_exact(&mut header).await;
        let decrypter = Decrypter::new(&ctx.cipher.file_key, &header).ok()?;

        let inner_offset = 0;
        let outer_offset = 0;
        let last_block = 0;
        let last_bytes = None;

        Some(CryptPassthrough {
            decrypter,
            reader,
            inner_offset,
            outer_offset,
            last_block,
            last_bytes,
        })
    }

    async fn handle_read_into(&mut self, len: usize, mut buf: Option<&mut [u8]>) -> usize {
        todo!()
    }
}

#[async_trait]
impl CacheFileHandler for CryptPassthrough {
    async fn read_into(&mut self, buf: &mut [u8]) -> usize {
        self.handle_read_into(buf.len(), Some(buf)).await
    }

    async fn cache_data(&mut self, len: usize) -> usize {
        self.handle_read_into(len, None).await
    }

    async fn seek_to(&mut self, offset: u64) {
        // the block to start at - note the data may span more than one block
        // for simplicity, each block is read and decrypted individually
        // Ideally, we could calculate all blocks required, issue one read call,
        // and then decrypt them efficiently but this is easier to verify for now...
        let starting_block = offset / decrypter::BLOCK_DATA_SIZE as u64;

        // the 'offset' may be partially inside the block
        // to compensate for this, calculate the offset into the decrypted block that
        // data needs to be retrieved from
        // This will be reset to 0 after it has been used
        let decrypted_block_starting_offset = if offset as usize > decrypter::BLOCK_DATA_SIZE {
            offset as usize - (starting_block as usize * decrypter::BLOCK_DATA_SIZE)
        } else {
            offset as usize
        };

        self.last_block = starting_block;
        self.last_bytes = None;
        self.outer_offset = decrypted_block_starting_offset as u64;
        self.inner_offset = offset;
    }
}