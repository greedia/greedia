use super::{crypt_context::CryptContext, CacheFileHandler, CacheHandlerError};
use async_trait::async_trait;
use bytes::Bytes;
use rclone_crypt::decrypter::{self, Decrypter};
use std::cmp::min;

pub struct CryptPassthrough {
    decrypter: Decrypter,
    reader: Box<dyn CacheFileHandler>,
    cur_block: u64,
    /// The last block of decrypted bytes, if any.
    last_bytes: Option<Bytes>,
}

impl CryptPassthrough {
    pub async fn new(
        ctx: &CryptContext,
        offset: u64,
        mut reader: Box<dyn CacheFileHandler>,
    ) -> Result<CryptPassthrough, CacheHandlerError> {
        reader.seek_to(0).await?;
        let mut header = [0u8; decrypter::FILE_HEADER_SIZE];
        reader.read_exact(&mut header).await?;
        let decrypter = Decrypter::new(&ctx.cipher.file_key, &header)
            .map_err(|_| CacheHandlerError::CryptPassthrough)?;

        let cur_block = 0;
        let last_bytes = None;

        let mut cpt = CryptPassthrough {
            decrypter,
            reader,
            cur_block,
            last_bytes,
        };

        cpt.seek_to(offset).await?;

        Ok(cpt)
    }

    async fn handle_read_into(
        &mut self,
        len: usize,
        mut buf: Option<&mut [u8]>,
    ) -> Result<usize, CacheHandlerError> {
        // If data exists in last_bytes, read from there first
        if let Some(last_bytes) = &mut self.last_bytes {
            if last_bytes.is_empty() {
                self.last_bytes = None
            } else {
                let bytes_split_len = min(len, last_bytes.len());
                let from_last_bytes = last_bytes.split_to(bytes_split_len);
                if let Some(buf) = buf.as_mut() {
                    buf[..bytes_split_len].copy_from_slice(&from_last_bytes);
                }
                return Ok(bytes_split_len);
            }
        }

        // Otherwise, load up a new last_bytes.
        let mut block_buf = [0u8; decrypter::BLOCK_SIZE];
        let encrypted_block_len = self.reader.read_exact(&mut block_buf).await?;

        if encrypted_block_len == 0 {
            return Ok(0);
        }

        let decrypted_block = self
            .decrypter
            .decrypt_block(self.cur_block, &block_buf[..encrypted_block_len])
            .map_err(|_| CacheHandlerError::CryptPassthrough)?;

        let read_len = min(decrypted_block.len(), len);
        if let Some(buf) = buf {
            buf[..read_len].copy_from_slice(&decrypted_block[..read_len]);
        }

        self.cur_block += 1;
        self.last_bytes = Some(Bytes::copy_from_slice(&decrypted_block[read_len..]));
        Ok(read_len)
    }
}

#[async_trait]
impl CacheFileHandler for CryptPassthrough {
    async fn read_into(&mut self, buf: &mut [u8]) -> Result<usize, CacheHandlerError> {
        self.handle_read_into(buf.len(), Some(buf)).await
    }

    async fn cache_data(&mut self, len: usize) -> Result<usize, CacheHandlerError> {
        self.handle_read_into(len, None).await
    }

    async fn seek_to(&mut self, offset: u64) -> Result<(), CacheHandlerError> {
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

        self.cur_block = starting_block;
        self.last_bytes = None;

        // Decrypt starting_block, and discard bytes up to the correct offset
        self.reader.seek_to(block_starting_offset as u64).await?;

        let mut block_buf = [0u8; decrypter::BLOCK_SIZE];
        let encrypted_block_len = self.reader.read_exact(&mut block_buf).await?;

        if encrypted_block_len == 0 {
            return Ok(());
        }

        let decrypted_block = self
            .decrypter
            .decrypt_block(self.cur_block, &block_buf[..encrypted_block_len])
            .map_err(|_| CacheHandlerError::CryptPassthrough)?;

        if let Some(decrypted_block) = decrypted_block.get(decrypted_block_starting_offset..) {
            self.last_bytes = Some(Bytes::copy_from_slice(decrypted_block));
            self.cur_block += 1;
        } else {
            self.last_bytes = None;
        }

        Ok(())
    }
}
