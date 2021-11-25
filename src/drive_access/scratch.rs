use std::{io, path::PathBuf};

use rustc_hash::FxHashMap;
use tokio::{fs::ReadDir, sync::Mutex};

use crate::item_map::ItemMap;

/// Mapping to a path on disk.
pub struct Inode {
    path: PathBuf,
}

pub struct ScratchDriveAccess {
    pub name: String,
    pub path: PathBuf,

    /// Mapping from scratch inodes to real filesystem files
    inodes: ItemMap<Inode>,
    /// Hold in-progress readdir calls
    readdir_calls: FxHashMap<u64, Mutex<ReadDirCall>>,
}

impl std::fmt::Debug for ScratchDriveAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScratchDriveAccess")
            .field("name", &self.name)
            .field("path", &self.path)
            .finish()
    }
}

impl ScratchDriveAccess {
    pub async fn new(name: String, path: PathBuf) -> Self {
        let inodes = ItemMap::new_start_at(1);
        // Add the root path
        inodes.create(Inode {
            path: PathBuf::from("/"),
        }).await;
        let readdir_calls = FxHashMap::default();
        Self {
            name,
            path,
            inodes,
            readdir_calls,
        }
    }

    pub async fn readdir(&self, inode: u64, offset: u64, for_each: impl FnMut(u64, ()) -> bool) -> io::Result<()> {
        let stream = if let Some(rdc) = self.readdir_calls.get(&inode) {
            let rdc = rdc.lock().await;
            if rdc.last_offset == offset {
                return self.handle_readdir(&rdc.stream, for_each).await;
            } else {
                self.new_readdir(inode, offset).await?
            }
        } else {
            self.new_readdir(inode, offset).await?
        };

        self.handle_readdir(&stream, for_each).await
    }

    pub async fn forget(&self, inode: u64) {
        todo!()
    }

    async fn new_readdir(&self, inode: u64, offset: u64) -> io::Result<ReadDir> {
        todo!()
    }

    async fn handle_readdir(&self, stream: &ReadDir, for_each: impl FnMut(u64, ()) -> bool) -> io::Result<()> {
        todo!()
    }
}

struct ReadDirCall {
    stream: ReadDir,
    last_offset: u64,
}
