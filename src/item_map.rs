use std::sync::Arc;

use rustc_hash::FxHashMap;
use tokio::sync::{Mutex, RwLock};

struct ItemMapInner<T> {
    map: FxHashMap<u64, Arc<Mutex<T>>>,
    next_fh: u64,
}

/// Map of items addressed by u64, mostly used for file handles.
pub struct ItemMap<T>(RwLock<ItemMapInner<T>>);

impl<T> ItemMap<T> {
    pub fn new() -> ItemMap<T> {
        Self::new_start_at(0)
    }

    pub fn new_start_at(start: u64) -> ItemMap<T> {
        ItemMap(RwLock::new(ItemMapInner {
            map: FxHashMap::default(),
            next_fh: start,
        }))
    }

    pub async fn create(&self, data: T) -> u64 {
        let mut inner_w = self.0.write().await;

        let file = Arc::new(Mutex::new(data));

        let fh = inner_w.next_fh;
        inner_w.next_fh += 1;

        inner_w.map.insert(fh, file);

        fh
    }

    pub async fn get(&self, fh: u64) -> Option<Arc<Mutex<T>>> {
        let inner_r = self.0.read().await;
        inner_r.map.get(&fh).cloned()
    }

    pub async fn remove(&self, fh: u64) {
        let mut inner_w = self.0.write().await;
        inner_w.map.remove(&fh);
    }

    pub async fn len(&self) -> usize {
        let inner_r = self.0.read().await;
        inner_r.map.len()
    }
}
