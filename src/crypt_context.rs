use anyhow::Result;
use rclone_crypt::{cipher::Cipher, decrypter, obscure};
#[derive(Clone)]
pub struct CryptContext {
    pub cipher: Cipher,
}

impl CryptContext {
    pub fn new(password1: &str, password2: &str) -> Result<CryptContext> {
        let password = obscure::reveal(password1)?;
        let salt = obscure::reveal(password2)?;
        let cipher = Cipher::new(password, salt)?;
        Ok(CryptContext { cipher })
    }

    // Get the decrypted file size for a file, given its encrypted file size.
    pub fn get_crypt_file_size(size: u64) -> u64 {
        let size_minus_header = size.saturating_sub(decrypter::FILE_HEADER_SIZE as u64);
        let last_block_size = size_minus_header % decrypter::BLOCK_SIZE as u64;
        let num_full_blocks = size_minus_header.saturating_sub(last_block_size) / decrypter::BLOCK_SIZE as u64;
        let full_blocks_size = num_full_blocks * decrypter::BLOCK_DATA_SIZE as u64;
    
        (full_blocks_size + last_block_size).saturating_sub(decrypter::BLOCK_HEADER_SIZE as u64)
    }
}
