use crate::drive_cache::DriveCache;
use anyhow::Result;
use polyfuse::io::{Reader, Writer};
use polyfuse::{Context, Filesystem, Operation};
use std::{ffi::OsStr, io, path::PathBuf};

struct GreediaFS {}

impl GreediaFS {
    pub fn new() -> GreediaFS {
        GreediaFS {}
    }
}

#[polyfuse::async_trait]
impl Filesystem for GreediaFS {
    async fn call<'a, 'cx, T: ?Sized>(
        &'a self,
        cx: &'a mut Context<'cx, T>,
        op: Operation<'cx>,
    ) -> io::Result<()>
    where
        T: Reader + Writer + Unpin + Send,
    {
        dbg!(&op);
        match op {
            _ => Ok(()),
        }
    }
}

/// Mount thread, with error handling
pub async fn mount_thread_eh(drive_caches: Vec<DriveCache>, mount_point: PathBuf) {
    mount_thread(drive_caches, mount_point).await.unwrap();
}

/// Thread that handles all FUSE requests
pub async fn mount_thread(drive_caches: Vec<DriveCache>, mount_point: PathBuf) -> Result<()> {
    let fuse_args: Vec<&OsStr> = vec![
        &OsStr::new("-o"),
        &OsStr::new("auto_unmount,ro,allow_other"),
    ];
    Ok(polyfuse_tokio::mount(GreediaFS::new(), mount_point, &fuse_args).await?)
}
