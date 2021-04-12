use crate::crypt_context::CryptContext;

use super::CacheFileHandler;
use async_trait::async_trait;
use bytes::Bytes;
use rclone_crypt::decrypter::{self, Decrypter};
use std::cmp::min;

pub struct CryptPassthrough {
    decrypter: Decrypter,
    reader: Box<dyn CacheFileHandler>,
    last_block: u64,
    /// The last block of decrypted bytes, if any.
    last_bytes: Option<Bytes>,
}

impl CryptPassthrough {
    pub async fn new(
        ctx: &CryptContext,
        mut reader: Box<dyn CacheFileHandler>,
    ) -> Option<CryptPassthrough> {
        reader.seek_to(0).await;
        let mut header = [0u8; decrypter::FILE_HEADER_SIZE];
        reader.read_exact(&mut header).await;
        let decrypter = Decrypter::new(&ctx.cipher.file_key, &header).ok()?;

        let last_block = 0;
        let last_bytes = None;

        Some(CryptPassthrough {
            decrypter,
            reader,
            last_block,
            last_bytes,
        })
    }

    async fn handle_read_into(&mut self, len: usize, buf: Option<&mut [u8]>) -> usize {
        // If data exists in last_bytes, read from there first
        if let Some(last_bytes) = &mut self.last_bytes {
            let bytes_split_len = min(len, last_bytes.len());
            let from_last_bytes = last_bytes.split_to(bytes_split_len);
            if let Some(buf) = buf {
                buf[..from_last_bytes.len()].copy_from_slice(&from_last_bytes);
            }
            if last_bytes.is_empty() {
                self.last_bytes = None;
            }
            return bytes_split_len;
        }

        // Otherwise, load up a new last_bytes.
        let mut block_buf = [0u8; decrypter::BLOCK_SIZE];
        let encrypted_block_len = self.reader.read_exact(&mut block_buf).await;

        if encrypted_block_len < decrypter::BLOCK_SIZE {
            return 0;
        }

        let decrypted_block = self
            .decrypter
            .decrypt_block(self.last_block + 1, &block_buf)
            .unwrap();

        let read_len = min(decrypted_block.len(), len);
        if let Some(buf) = buf {
            buf[..read_len].copy_from_slice(&decrypted_block[..read_len]);
        }

        self.last_block += 1;
        self.last_bytes = Some(Bytes::copy_from_slice(&decrypted_block[read_len..]));

        read_len
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
        // The block to start at - note the data may span more than one block.
        // For simplicity, each block is read and decrypted individually.
        // Ideally, we could calculate all blocks required, issue one read call,
        // and then decrypt them efficiently but this is easier to verify for now...
        let starting_block = offset / decrypter::BLOCK_DATA_SIZE as u64;

        let block_starting_offset =
            decrypter::FILE_HEADER_SIZE as u64 + (starting_block * decrypter::BLOCK_SIZE as u64);

        // The 'offset' may be partially inside the block.
        // To compensate for this, calculate the offset into the decrypted block that
        // data needs to be retrieved from.
        let decrypted_block_starting_offset = if offset as usize > decrypter::BLOCK_DATA_SIZE {
            offset as usize - (starting_block as usize * decrypter::BLOCK_DATA_SIZE)
        } else {
            offset as usize
        };

        self.last_block = starting_block;
        self.last_bytes = None;

        // Decrypt starting_block, and discard bytes up to the correct offset
        self.reader.seek_to(block_starting_offset as u64).await;

        let mut block_buf = [0u8; decrypter::BLOCK_SIZE];
        let encrypted_block_len = self.reader.read_exact(&mut block_buf).await;

        if encrypted_block_len < decrypter::BLOCK_SIZE {
            return;
        }

        let decrypted_block = self
            .decrypter
            .decrypt_block(self.last_block, &block_buf)
            .unwrap();

        self.last_bytes = Some(Bytes::copy_from_slice(
            &decrypted_block[decrypted_block_starting_offset..],
        ));
    }
}
