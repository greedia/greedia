use std::sync::Arc;

use crate::{cache_handlers::CacheDriveHandler, db::Db};

pub async fn scan_thread(name: String, db: Db, cache_handler: Arc<dyn CacheDriveHandler>) {
    let drive_id = cache_handler.get_drive_id();
    println!("Scanning {} ({})...", &name, drive_id);
}

struct ScanState {
    
}