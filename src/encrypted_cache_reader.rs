use crate::cache_reader::{CacheRead, CacheReader};
use rclone_crypt::{
    cipher::Cipher,
    decrypter::{self, Decrypter},
};
use anyhow::Result;
use async_trait::async_trait;

pub struct EncryptedCacheReader {
    decrypter: Decrypter,
    reader: CacheReader,
}

impl EncryptedCacheReader {
    pub async fn new(cipher: &Cipher, mut reader: CacheReader) -> Result<Self> {
        let header = reader.read(0, decrypter::FILE_HEADER_SIZE as u32).await?;
        let decrypter = Decrypter::new(&cipher.file_key, &header)?;
        Ok(EncryptedCacheReader { decrypter, reader })
    }
}

#[async_trait]
impl CacheRead for EncryptedCacheReader {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Vec<u8>> {
        // NB: offset is 'file offset' into the encrypted buffer
        // dbg!("read", offset, size);
        let mut result = Vec::with_capacity(size as usize);

        // the block to start at - note the data may span more than one block
        // for simplicity, each block is read and decrypted individually
        // Ideally, we could calculate all blocks required, issue one read call,
        // and then decrypt them efficiently but this is easier to verify for now...
        let starting_block = offset / decrypter::BLOCK_DATA_SIZE as u64;

        // the 'offset' may be partially inside the block
        // to compensate for this, calculate the offset into the decrypted block that
        // data needs to be retrieved from
        // This will be reset to 0 after it has been used
        let mut decrypted_block_starting_offset = if offset as usize > decrypter::BLOCK_DATA_SIZE {
            offset as usize - (starting_block as usize * decrypter::BLOCK_DATA_SIZE)
        } else {
            offset as usize
        };

        // the amount of data remaining
        // given above, to keep it simple one block is read at a time and all data
        // within that block feeds into the data remaining.
        let mut remaining = size as usize;

        // the current block that needs to be fetched and decrypted
        // this will be decrypted and 'remaining' bytes read from it
        let mut current_block = starting_block;

        while remaining > 0 {
            // the offset into the encrypted file that the block starts at
            // each block has a header (on top of the file header) which needs to be taken into account
            // ideally this would be part of rclone-crypt :P
            let block_starting_offset =
                decrypter::FILE_HEADER_SIZE as u64 + (current_block * decrypter::BLOCK_SIZE as u64);

            let encrypted_block = self
                .reader
                .read(block_starting_offset, decrypter::BLOCK_SIZE as u32).await?;

            if encrypted_block.len() == 0 {
                return Ok(result);
            }

            let decrypted_block = self
                .decrypter
                .decrypt_block(current_block, &encrypted_block)?;

            // apply the starting_offset
            // if it's 0, this is effectively the entire block...
            if decrypted_block_starting_offset >= decrypted_block.len() {
                return Ok(result);
            }
            let decrypted_block = &decrypted_block[decrypted_block_starting_offset..];

            if remaining >= decrypted_block.len() {
                // The block contains all or a partial of the desired amount of data
                remaining -= decrypted_block.len();
                result.extend(decrypted_block);
            } else {
                // The block contains all of the desired amount of data, but only part of the block
                result.extend_from_slice(&decrypted_block[0..remaining]);
                remaining = 0;
            }

            current_block += 1;
            decrypted_block_starting_offset = 0;
        }

        Ok(result)
    }
}
