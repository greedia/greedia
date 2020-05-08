use crate::drive_cache::DriveCache;
use anyhow::Result;

/// Scan thread, with error handling
pub async fn scan_thread_eh(drive_cache: DriveCache) {
    scan_thread(drive_cache).await.unwrap()
}

/// Thread that handles scanning and updating of one defined drive
pub async fn scan_thread(drive_cache: DriveCache) -> Result<()> {
    Ok(())
}
