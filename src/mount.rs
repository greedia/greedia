use crate::{
    cache_handlers::CacheFileHandler,
    drive_access2::{DriveAccess, TypeResult},
    fh_map::FhMap,
    types::ArchivedDriveItemData,
};
use anyhow::Result;
use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut},
    KernelConfig, Operation, Request, Session,
};
use tokio::{
    io::{unix::AsyncFd, Interest},
    task::{self, JoinHandle},
};

use std::{
    ffi::OsStr,
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TTL_LONG: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ITEM_BITS: usize = 48;
const DRIVE_OFFSET: u64 = 1 << ITEM_BITS;
const ITEM_MASK: u64 = DRIVE_OFFSET - 1;

pub async fn mount_thread(drives: Vec<Arc<DriveAccess>>, mount_point: PathBuf) {
    let mut config = KernelConfig::default();
    config.mount_option("ro");
    config.mount_option("allow_other");

    let session = AsyncSession::mount(mount_point, config).await.unwrap();

    let fs = Arc::new(GreediaFS::new(drives));

    while let Some(req) = session.next_request().await.unwrap() {
        let fs = fs.clone();

        let _: JoinHandle<Result<()>> = tokio::spawn(async move {
            let operation = req.operation();
            match operation? {
                Operation::Lookup(op) => fs.do_lookup(&req, op).await?,
                Operation::Getattr(op) => fs.do_getattr(&req, op).await?,
                Operation::Readdir(op) => fs.do_readdir(&req, op).await?,
                Operation::Open(op) => fs.do_open(&req, op).await?,
                Operation::Opendir(op) => fs.do_opendir(&req, op).await?,
                Operation::Read(op) => fs.do_read(&req, op).await?,
                Operation::Release(op) => fs.do_release(op).await?,
                _ => req.reply_error(libc::ENOSYS)?,
            }

            Ok(())
        });

        task::yield_now().await;
    }
}

enum Inode {
    Root,
    Unknown,
    Drive(u16, u64),
}

struct FileHandle {
    offset: u64,
    reader: Box<dyn CacheFileHandler>,
    buffer: Vec<u8>,
}

struct GreediaFS {
    drives: Vec<Arc<DriveAccess>>,
    start_time: Duration,
    file_handles: FhMap<FileHandle>,
}

impl GreediaFS {
    pub fn new(drives: Vec<Arc<DriveAccess>>) -> GreediaFS {
        let start = SystemTime::now();
        let start_time = start.duration_since(UNIX_EPOCH).unwrap();
        let open_files = FhMap::new();
        GreediaFS {
            drives,
            start_time,
            file_handles: open_files,
        }
    }

    fn map_inode(&self, inode: u64) -> Inode {
        // The first 16 bytes in an inode are the drive_id, and the last 48 bytes are the item_id.
        // drive_id 0 is reserved, so drives start at drive_id 1.
        // Within drive_id 0, item_id 1 is the mount root, and item_id 2 is an "unknown" entry that always returns ENOENT.

        match inode {
            inode if inode == 1 => Inode::Root,
            inode if inode >= DRIVE_OFFSET => {
                Inode::Drive(((inode >> ITEM_BITS) - 1) as u16, inode & ITEM_MASK)
            }
            _ => Inode::Unknown,
        }
    }

    fn rev_inode(&self, inode: Inode) -> u64 {
        match inode {
            Inode::Root => 1,
            Inode::Unknown | Inode::Drive(std::u16::MAX, _) => 2, // Drive 65535 is invalid, due to the 1 offset.
            Inode::Drive(_, local_inode) if local_inode > ITEM_MASK => 2,
            Inode::Drive(drive, local_inode) => (drive as u64 + 1) << 48 | local_inode,
        }
    }

    fn getattr_root(&self, file_attr: &mut FileAttr) -> Duration {
        file_attr.mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.ino(1);
        file_attr.nlink(self.drives.len() as u32);
        file_attr.size(self.drives.len() as u64);
        file_attr.ctime(self.start_time);
        file_attr.mtime(self.start_time);
        TTL_LONG
    }

    fn getattr_drive(&self, drive: u16, file_attr: &mut FileAttr) -> Option<Duration> {
        if self.drives.len() <= drive as usize {
            None
        } else {
            file_attr.mode(libc::S_IFDIR as u32 | 0o555);
            file_attr.ino(self.rev_inode(Inode::Drive(drive, 0)));
            file_attr.ctime(self.start_time);
            file_attr.mtime(self.start_time);
            Some(TTL_LONG)
        }
    }

    fn getattr_item(&self, drive: u16, inode: u64, file_attr: &mut FileAttr) -> Option<Duration> {
        let drive_access = self.drives.get(drive as usize)?;
        drive_access.getattr_item(inode, |item| {
            let global_inode = self.rev_inode(Inode::Drive(drive, inode));
            file_attr.ino(global_inode);
            match &item.data {
                ArchivedDriveItemData::FileItem {
                    file_name: _,
                    data_id: _,
                    size,
                } => {
                    file_attr.mode(libc::S_IFREG as u32 | 0o444);
                    file_attr.size(*size);
                }
                ArchivedDriveItemData::Dir { items: _ } => {
                    file_attr.mode(libc::S_IFDIR as u32 | 0o555);
                }
            };
            todo!()
        })?;
        Some(TTL_LONG)
    }

    fn readdir_root(&self, offset: u64, readdir_out: &mut ReaddirOut) {
        for (num, drive) in self.drives.iter().skip(offset as usize).enumerate() {
            let name = OsStr::new(&drive.name);
            let ino = self.rev_inode(Inode::Drive(num as u16, 0));
            let typ = libc::DT_DIR as u32;
            let off = num as u64 + 1;
            if readdir_out.entry(name, ino, typ, off) {
                break;
            }
        }
    }

    async fn readdir_drive(
        &self,
        drive: u16,
        inode: u64,
        offset: u64,
        readdir_out: &mut ReaddirOut,
    ) -> Option<()> {
        let drive_access = self.drives.get(drive as usize)?;

        drive_access.readdir(inode, offset, |off, item| {
            let name = OsStr::new(item.name.as_str());
            let ino = self.rev_inode(Inode::Drive(drive, item.inode));
            let off = off + 1;
            let typ = if item.is_dir {
                libc::DT_DIR as u32
            } else {
                libc::DT_REG as u32
            };

            readdir_out.entry(name, ino, typ, off)
        })
    }

    fn lookup_root(&self, name: &OsStr) -> Option<EntryOut> {
        let name = name.to_str()?;
        let drive_num = self
            .drives
            .iter()
            .position(|drive_access| drive_access.name == name)?;

        let mut entry_out = EntryOut::default();
        entry_out.ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        entry_out.ttl_attr(TTL_LONG);
        entry_out.ttl_entry(TTL_LONG);

        let file_attr = entry_out.attr();
        file_attr.mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        file_attr.ctime(self.start_time);
        file_attr.mtime(self.start_time);

        Some(entry_out)
    }

    fn lookup_drive(&self, drive: u16, inode: u64, name: &OsStr) -> Option<EntryOut> {
        let drive_access = self.drives.get(drive as usize)?;
        let file_name = name.to_str()?;

        let reply_entry = drive_access.lookup_item(inode, file_name, |child_inode, drive_item| {
            let global_inode = self.rev_inode(Inode::Drive(drive, child_inode));
            let dur = Duration::from_secs(drive_item.modified_time as u64);

            let mut reply_entry = EntryOut::default();
            let file_attr = reply_entry.attr();
            if let ArchivedDriveItemData::FileItem { file_name: _, data_id: _, size } = drive_item.data {
                file_attr.size(size);
                file_attr.mode(libc::S_IFREG as u32 | 0o444);
            } else {
                file_attr.mode(libc::S_IFDIR as u32 | 0o555);
            }
            file_attr.ctime(dur);
            file_attr.mtime(dur);
            file_attr.ino(global_inode);

            reply_entry.ino(global_inode);
            reply_entry.ttl_attr(TTL_LONG);
            reply_entry.ttl_entry(TTL_LONG);
            reply_entry
        })?;

        Some(reply_entry)
    }

    async fn open_drive(&self, drive: u16, local_inode: u64) -> TypeResult<OpenOut> {
        if let Some(drive_access) = self.drives.get(drive as usize) {
            match drive_access.open_file(local_inode, 0).await {
                TypeResult::IsType(reader) => {
                    let fh = self
                        .file_handles
                        .open(FileHandle {
                            offset: 0,
                            reader,
                            buffer: vec![0u8; 65536],
                        })
                        .await;

                    let mut open_out = OpenOut::default();
                    open_out.fh(fh);

                    TypeResult::IsType(open_out)
                }
                TypeResult::IsNotType => TypeResult::IsNotType,
                TypeResult::DoesNotExist => TypeResult::DoesNotExist,
            }
        } else {
            TypeResult::DoesNotExist
        }
    }

    async fn opendir_root(&self) -> OpenOut {
        let mut open_out = OpenOut::default();
        open_out.fh(0);
        open_out.cache_dir(true);
        open_out
    }

    async fn opendir_drive(&self, drive: u16, local_inode: u64) -> TypeResult<OpenOut> {
        if let Some(drive_access) = self.drives.get(drive as usize) {
            match drive_access.check_dir(local_inode) {
                TypeResult::IsType(scanning) => {
                    let mut open_out = OpenOut::default();
                    open_out.fh(0);
                    open_out.keep_cache(!scanning);
                    open_out.cache_dir(!scanning);
                    TypeResult::IsType(open_out)
                }
                TypeResult::IsNotType => TypeResult::IsNotType,
                TypeResult::DoesNotExist => TypeResult::DoesNotExist,
            }
        } else {
            TypeResult::DoesNotExist
        }
    }

    async fn do_getattr(&self, req: &Request, op: op::Getattr<'_>) -> io::Result<()> {
        let mut attr_out = AttrOut::default();
        let file_attr = attr_out.attr();
        let ttl = match self.map_inode(op.ino()) {
            Inode::Root => Some(self.getattr_root(file_attr)),
            Inode::Unknown => None,
            Inode::Drive(drive, local_inode) if local_inode == 0 => {
                self.getattr_drive(drive, file_attr)
            }
            Inode::Drive(drive, local_inode) => self.getattr_item(drive, local_inode, file_attr),
        };

        match ttl {
            Some(ttl) => {
                attr_out.ttl(ttl);
                req.reply(attr_out)?
            }
            None => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    async fn do_readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        let offset = op.offset();
        let size = op.size();

        let mut readdir_out = ReaddirOut::new(size as usize);

        let dir = match self.map_inode(op.ino()) {
            Inode::Root => Some(self.readdir_root(offset, &mut readdir_out)),
            Inode::Unknown => None,
            Inode::Drive(drive, local_inode) => {
                self.readdir_drive(drive, local_inode, offset, &mut readdir_out)
                    .await
            }
        };

        match dir {
            Some(_) => req.reply(readdir_out)?,
            None => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    async fn do_lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {
        let inode = op.parent();
        let name = op.name();
        let lookup = match self.map_inode(inode) {
            Inode::Root => self.lookup_root(name),
            Inode::Drive(drive, local_inode) => self.lookup_drive(drive, local_inode, name),
            _ => None,
        };

        match lookup {
            Some(lookup) => req.reply(lookup)?,
            None => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    async fn do_open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        let inode = op.ino();
        let open = match self.map_inode(inode) {
            Inode::Drive(drive, local_inode) => self.open_drive(drive, local_inode).await,
            _ => TypeResult::DoesNotExist,
        };

        match open {
            TypeResult::IsType(open_out) => req.reply(open_out)?,
            TypeResult::IsNotType => req.reply_error(libc::EISDIR)?,
            TypeResult::DoesNotExist => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    async fn do_opendir(&self, req: &Request, op: op::Opendir<'_>) -> io::Result<()> {
        let inode = op.ino();
        let opendir = match self.map_inode(inode) {
            Inode::Root => TypeResult::IsType(self.opendir_root().await),
            Inode::Drive(drive, local_inode) => self.opendir_drive(drive, local_inode).await,
            _ => TypeResult::DoesNotExist,
        };

        match opendir {
            TypeResult::IsType(open_out) => req.reply(open_out)?,
            TypeResult::IsNotType => req.reply_error(libc::ENOTDIR)?,
            TypeResult::DoesNotExist => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    /// Perform a FUSE read() operation.
    async fn do_read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        let fh = op.fh();
        let offset = op.offset();
        let size = op.size() as usize;
        if let Some(file) = self.file_handles.get(fh).await {
            let mut f = file.lock().await;

            // Destructuring like this allows us to access multiple fields mutably.
            let FileHandle {
                offset: f_offset,
                reader,
                buffer,
            } = &mut *f;

            // FUSE passes us explicit offsets, so if our offset isn't where we expected
            // the reader probably made a seek.
            if *f_offset != offset {
                reader.seek_to(offset).await;
            }

            // FUSE requires us to give an exact size, so make sure the buffer is large
            // enough to accommodate the read. We only give a smaller size on EOF.
            if size > buffer.len() {
                buffer.resize_with(size, Default::default);
            }

            // Read, and push our expected offset forward.
            let read_len = reader.read_exact(&mut buffer[..size]).await;
            *f_offset = offset + read_len as u64;

            req.reply(&buffer[..read_len])?;
        } else {
            req.reply_error(libc::ENOENT)?
        }

        Ok(())
    }

    async fn do_release(&self, op: op::Release<'_>) -> io::Result<()> {
        let fh = op.fh();
        self.file_handles.close(fh).await;

        Ok(())
    }
}

struct AsyncSession {
    inner: AsyncFd<Session>,
}

impl AsyncSession {
    async fn mount(mountpoint: PathBuf, config: KernelConfig) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let session = Session::mount(mountpoint, config)?;
            Ok(Self {
                inner: AsyncFd::with_interest(session, Interest::READABLE)?,
            })
        })
        .await
        .expect("join error")
    }

    async fn next_request(&self) -> io::Result<Option<Request>> {
        use futures::{future::poll_fn, ready, task::Poll};

        poll_fn(|cx| {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;
            match self.inner.get_ref().next_request() {
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                    Poll::Pending
                }
                res => {
                    guard.retain_ready();
                    Poll::Ready(res)
                }
            }
        })
        .await
    }
}
