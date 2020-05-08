use anyhow::Result;

pub struct Downloader {}

impl Downloader {
    pub fn new(
        client_id: String,
        client_secret: String,
        refresh_token: String,
    ) -> Result<Downloader> {
        Ok(Downloader {})
    }
}
