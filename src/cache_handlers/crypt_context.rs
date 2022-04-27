use rclone_crypt::{cipher::Cipher, obscure, BLOCK_HEADER_SIZE, BLOCK_DATA_SIZE, BLOCK_SIZE, FILE_HEADER_SIZE};

use super::CacheHandlerError;

/// Handles everything related to rclone encryption.
/// Mostly a passthrough to rclone_crypt's `Cipher`,
/// but can also calculate a file's decrypted size.
#[derive(Debug, Clone)]
pub struct CryptContext {
    pub cipher: Cipher,
}

impl CryptContext {
    /// Create a CryptContext using the two rclone passwords.
    /// Note that these passwords must be obfuscated, like they
    /// would show up in an rclone config file.
    pub fn new(password1: &str, password2: &str) -> Result<CryptContext, CacheHandlerError> {
        let password =
            obscure::reveal(password1).map_err(|_| CacheHandlerError::CryptPassthrough)?;
        let salt = obscure::reveal(password2).map_err(|_| CacheHandlerError::CryptPassthrough)?;
        let cipher =
            Cipher::new(password, salt).map_err(|_| CacheHandlerError::CryptPassthrough)?;
        Ok(CryptContext { cipher })
    }

    // Get the decrypted file size for a file, given its encrypted file size.
    pub fn get_crypt_file_size(size: u64) -> u64 {
        let size_minus_header = size.saturating_sub(FILE_HEADER_SIZE as u64);
        let last_block_size = size_minus_header % BLOCK_SIZE as u64;
        let num_full_blocks =
            size_minus_header.saturating_sub(last_block_size) / BLOCK_SIZE as u64;
        let full_blocks_size = num_full_blocks * BLOCK_DATA_SIZE as u64;

        (full_blocks_size + last_block_size).saturating_sub(BLOCK_HEADER_SIZE as u64)
    }
}
