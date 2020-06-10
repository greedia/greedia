use rustc_hash::FxHashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

struct OpenFileMapInner<T> {
    map: FxHashMap<u64, Arc<Mutex<T>>>,
    next_fh: u64,
}

pub struct OpenFileMap<T>(RwLock<OpenFileMapInner<T>>);

impl<T> OpenFileMap<T> {
    pub fn new() -> OpenFileMap<T> {
        OpenFileMap(RwLock::new(OpenFileMapInner {
            map: FxHashMap::default(),
            next_fh: 0,
        }))
    }

    pub async fn open(&self, file: T) -> u64 {
        let mut inner_w = self.0.write().await;

        let file = Arc::new(Mutex::new(file));

        let fh = inner_w.next_fh;
        inner_w.next_fh += 1;

        inner_w.map.insert(fh, file);

        fh
    }

    pub async fn get(&self, fh: u64) -> Option<Arc<Mutex<T>>> {
        let inner_r = self.0.read().await;
        inner_r.map.get(&fh).map(|file| file.clone())
    }

    pub async fn close(&self, fh: u64) {
        let mut inner_w = self.0.write().await;
        inner_w.map.remove(&fh);
    }
}
