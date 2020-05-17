use anyhow::Result;
use rclone_crypt::{cipher::Cipher, obscure};
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
}
