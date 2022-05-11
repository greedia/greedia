use std::{
    ffi::OsStr,
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use camino::Utf8PathBuf;
use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut},
    KernelConfig, Operation, Request, Session,
};
use tokio::{
    io::{unix::AsyncFd, Interest},
    task::{self, JoinHandle},
};
use tracing::{trace_span, trace};
use tracing_futures::Instrument;

use crate::{
    cache_handlers::{CacheFileHandler, CacheHandlerError},
    db::types::ArchivedDriveItemData,
    drive_access::{DriveAccess, TypeResult},
    fh_map::FhMap,
    tweaks,
};

const TTL_LONG: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ITEM_BITS: usize = 48;
const DRIVE_OFFSET: u64 = 1 << ITEM_BITS;
const ITEM_MASK: u64 = DRIVE_OFFSET - 1;

pub async fn mount_thread(drives: Vec<Arc<DriveAccess>>, mountpoint: Utf8PathBuf) {
    trace!("Starting mount thread");
    let mut config = KernelConfig::default();
    if tweaks().mount_read_write {
        println!("TWEAK: mounting read-write");
    } else {
        config.mount_option("ro");
    }
    config.mount_option("allow_other");

    let session = AsyncSession::mount(mountpoint, config)
        .await
        .expect("Could not mount FUSE filesystem");

    let fs = Arc::new(GreediaFS::new(drives));

    while let Some(req) = session
        .next_request()
        .await
        .expect("Could not process FUSE request")
    {
        let fs = fs.clone();

        let _: JoinHandle<Result<()>> = task::spawn(async move {
            let operation = req.operation()?;
            match operation {
                Operation::Lookup(op) => fs.do_lookup(&req, op).await?,
                Operation::Getattr(op) => fs.do_getattr(&req, op).await?,
                Operation::Opendir(op) => fs.do_opendir(&req, op).await?,
                Operation::Readdir(op) => fs.do_readdir(&req, op).await?,
                Operation::Open(op) => {
                    let span = trace_span!("fuse_open", ino=op.ino());
                    fs.do_open(&req, op).instrument(span).await?
                }
                Operation::Read(op) => {
                    let span = trace_span!("fuse_read", fh=op.fh(), off=op.offset(), sz=op.size());
                    fs.do_read(&req, op).instrument(span).await?
                }
                Operation::Release(op) => fs.do_release(&req, op).await?,
                Operation::Unlink(op) => fs.do_unlink(&req, op).await?,
                Operation::Rename(op) => fs.do_rename(&req, op).await?,
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
    kernel_dir_caching: bool,
}

impl GreediaFS {
    pub fn new(drives: Vec<Arc<DriveAccess>>) -> GreediaFS {
        let start = SystemTime::now();
        let start_time = start
            .duration_since(UNIX_EPOCH)
            .expect("Could not get duration since unix epoch");
        let file_handles = FhMap::new();
        GreediaFS {
            drives,
            start_time,
            file_handles,
            kernel_dir_caching: tweaks().enable_kernel_dir_caching,
        }
    }

    /// Maps a u64 incode to an internal inode representation.
    ///
    /// Each drive has its own "set" of inodes, and external FUSE inodes combine these,
    /// along with a drive number.
    ///
    /// The first 16 bits in an inode are the drive_id, and the last 48 bits are the item_id.
    /// drive_id 0 is reserved for the root, so drives start at drive_id 1.
    /// Within drive_id 0, item_id 1 is the mount root, and item_id 2 is an "unknown" entry that always returns ENOENT.
    fn map_inode(&self, inode: u64) -> Inode {
        match inode {
            inode if inode == 1 => Inode::Root,
            inode if inode >= DRIVE_OFFSET => {
                Inode::Drive(((inode >> ITEM_BITS) - 1) as u16, inode & ITEM_MASK)
            }
            _ => Inode::Unknown,
        }
    }

    /// Maps an internal Inode into a u64 FUSE inode.
    ///
    /// Performs the reverse of `map_inode`.
    fn rev_inode(&self, inode: Inode) -> u64 {
        match inode {
            Inode::Root => 1,
            Inode::Unknown | Inode::Drive(std::u16::MAX, _) => 2, // Drive 65535 is invalid, due to the 1 offset.
            Inode::Drive(_, local_inode) if local_inode > ITEM_MASK => 2,
            Inode::Drive(drive, local_inode) => (drive as u64 + 1) << 48 | local_inode,
        }
    }

    /// Fill in the file attribute for a `getattr()` call to the root mount.
    fn getattr_root(&self, file_attr: &mut FileAttr) -> Duration {
        file_attr.mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.ino(1);
        file_attr.nlink(self.drives.len() as u32);
        file_attr.size(self.drives.len() as u64);
        file_attr.ctime(self.start_time);
        file_attr.mtime(self.start_time);
        TTL_LONG
    }

    /// Fill in the file attribute for a `getattr()` call to a drive directory within the root mount.
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

    /// Fill in the file attribute for a `getattr()` call to any file or directory within a drive.
    fn getattr_item(&self, drive: u16, inode: u64, file_attr: &mut FileAttr) -> Option<Duration> {
        let drive_access = self.drives.get(drive as usize)?;
        drive_access.getattr_item(inode, |item, encrypted_size| {
            let global_inode = self.rev_inode(Inode::Drive(drive, inode));
            file_attr.ino(global_inode);
            match &item.data {
                ArchivedDriveItemData::FileItem {
                    file_name: _,
                    data_id: _,
                    size,
                } => {
                    file_attr.mode(libc::S_IFREG as u32 | 0o444);
                    file_attr.size(encrypted_size.unwrap_or(size.value()));
                }
                ArchivedDriveItemData::Dir { items: _ } => {
                    file_attr.mode(libc::S_IFDIR as u32 | 0o555);
                }
            };
        })?;
        Some(TTL_LONG)
    }

    /// Read the root directory for a `readdir()` call.
    ///
    /// Returns every drive as a directory in the root mount.
    ///
    /// As the root directory should never change during runtime,
    /// the kernel is instructed to always cache the root.
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

    /// Read a directory in a drive for a `readdir()` call.
    ///
    /// Takes an inode as a pointer to a directory to read.
    /// If the inode is 0, read the root directory of the drive.
    async fn readdir_drive(
        &self,
        drive: u16,
        inode: u64,
        offset: u64,
        readdir_out: &mut ReaddirOut,
    ) -> Option<()> {
        let drive_access = self.drives.get(drive as usize)?;

        drive_access.readdir(inode, offset, |off, item, file_name_override| {
            // File name overrides are typically used when a file is encrypted.
            // Items are stored in a read-only manner, so this allows us to use
            // the unencrypted file name, without having to clone and modify the item.
            let file_name = file_name_override.unwrap_or_else(|| item.name.as_str());
            let name = OsStr::new(file_name);
            let ino = self.rev_inode(Inode::Drive(drive, item.inode.value()));
            let off = off + 1;
            let typ = if item.is_dir {
                libc::DT_DIR as u32
            } else {
                libc::DT_REG as u32
            };

            readdir_out.entry(name, ino, typ, off)
        })
    }

    /// Looks up a filename within a directory for a `lookup()` call.
    ///
    /// Returns an entry if a drive name within the root mount exists.
    fn lookup_root(&self, name: &OsStr) -> Option<EntryOut> {
        let name = name.to_str()?;
        let drive_num = self
            .drives
            .iter()
            .position(|drive_access| drive_access.name == name)?;

        let mut entry_out = EntryOut::default();
        entry_out.ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        if self.kernel_dir_caching {
            entry_out.ttl_attr(TTL_LONG);
            entry_out.ttl_entry(TTL_LONG);
        }

        let file_attr = entry_out.attr();
        file_attr.mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        file_attr.ctime(self.start_time);
        file_attr.mtime(self.start_time);

        Some(entry_out)
    }

    /// Looks up a filename within a drive directory for a `lookup()` call.
    ///
    /// Takes an inode as a pointer to a directory to look up a file inside.
    /// If the inode is 0, look up in the root directory of the drive.
    fn lookup_drive(&self, drive: u16, inode: u64, name: &OsStr) -> Option<EntryOut> {
        let drive_access = self.drives.get(drive as usize)?;
        let file_name = name.to_str()?;

        let reply_entry =
            drive_access.lookup_item(inode, file_name, |child_inode, drive_item| {
                let global_inode = self.rev_inode(Inode::Drive(drive, child_inode));
                let dur = Duration::from_secs(drive_item.modified_time.value() as u64);

                let mut reply_entry = EntryOut::default();
                let file_attr = reply_entry.attr();
                if let ArchivedDriveItemData::FileItem {
                    file_name: _,
                    data_id: _,
                    size,
                } = drive_item.data
                {
                    file_attr.size(size.value());
                    file_attr.mode(libc::S_IFREG as u32 | 0o444);
                } else {
                    file_attr.mode(libc::S_IFDIR as u32 | 0o555);
                }
                file_attr.ctime(dur);
                file_attr.mtime(dur);
                file_attr.ino(global_inode);

                reply_entry.ino(global_inode);
                if self.kernel_dir_caching {
                    reply_entry.ttl_attr(TTL_LONG);
                    reply_entry.ttl_entry(TTL_LONG);
                }
                reply_entry
            })?;

        Some(reply_entry)
    }

    /// Unlinks a filename within a drive directory.
    ///
    /// Takes an inode as a pointer to a directory to look up a file inside.
    /// If the inode is 0, look up in the root directory of the drive.
    async fn unlink_drive(&self, drive: u16, inode: u64, name: &OsStr) -> Option<()> {
        let drive_access = self.drives.get(drive as usize)?;
        let file_name = name.to_str()?;

        drive_access.unlink_item(inode, file_name).await
    }

    /// Renames and/or moves a file within a drive directory.
    ///
    /// If new_parent differs from parent, move the file to that new parent.
    /// If new_name differs from name, rename the file.
    async fn rename_drive(
        &self,
        drive: u16,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Option<()> {
        let drive_access = self.drives.get(drive as usize)?;
        let file_name = name.to_str()?;
        let new_parent = if new_parent == parent {
            None
        } else {
            Some(new_parent)
        };
        let new_name = if new_name == name {
            None
        } else {
            Some(new_name.to_str()?)
        };

        drive_access
            .rename_item(parent, file_name, new_parent, new_name)
            .await
    }

    /// Open a file within a drive from an `open()` call.
    ///
    /// There is no root version of this, as the root only contains
    /// drives, which are all represented as directories.
    async fn open_drive(
        &self,
        drive: u16,
        local_inode: u64,
    ) -> Result<TypeResult<OpenOut>, CacheHandlerError> {
        Ok(
            if let Some(drive_access) = self.drives.get(drive as usize) {
                match drive_access.open_file(local_inode, 0).await? {
                    TypeResult::IsType(reader) => {
                        let fh = self
                            .file_handles
                            .open(FileHandle {
                                offset: 0,
                                reader,
                                buffer: vec![0u8; 65536],
                            })
                            .await;
                        println!("OPN {}, {}", fh, &self.file_handles.len().await);

                        let mut open_out = OpenOut::default();
                        open_out.fh(fh);

                        TypeResult::IsType(open_out)
                    }
                    TypeResult::IsNotType => TypeResult::IsNotType,
                    TypeResult::DoesNotExist => TypeResult::DoesNotExist,
                }
            } else {
                TypeResult::DoesNotExist
            },
        )
    }

    /// Open a directory within the root from an `opendir()` call.
    ///
    /// No parameters are taken, as there's only one root directory that can be opened.
    async fn opendir_root(&self) -> OpenOut {
        let mut open_out = OpenOut::default();
        open_out.fh(0);
        open_out.cache_dir(true);
        open_out
    }

    /// Open a directory within a drive from an `opendir()` call.
    ///
    /// This only returns an entry if an item exists at a local_inode,
    /// and that item is a directory.
    async fn opendir_drive(
        &self,
        drive: u16,
        local_inode: u64,
    ) -> Result<TypeResult<OpenOut>, CacheHandlerError> {
        Ok(
            if let Some(drive_access) = self.drives.get(drive as usize) {
                match drive_access.check_dir(local_inode)? {
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
            },
        )
    }

    /// Pass a FUSE `getattr()` call to the correct sub-method (root, drive, item).
    ///
    /// This function splits the FUSE inode apart using `map_inode` to figure out
    /// if we're requesting the attributes for the root mount, a drive directory,
    /// or an item within a drive directory.
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
                if self.kernel_dir_caching {
                    attr_out.ttl(ttl);
                }
                req.reply(attr_out)?
            }
            None => req.reply_error(libc::ENOENT)?,
        }

        Ok(())
    }

    /// Pass a FUSE `readdir()` call to the correct sub-method (root, drive).
    async fn do_readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        let offset = op.offset();
        let size = op.size();

        let mut readdir_out = ReaddirOut::new(size as usize);

        let dir = match self.map_inode(op.ino()) {
            Inode::Root => {
                self.readdir_root(offset, &mut readdir_out);
                Some(())
            }
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

    /// Pass a FUSE `lookup()` call to the correct sub-method (root, drive).
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

    /// Pass a FUSE `open()` call to the correct sub-method. In this case it must be an
    /// item within a drive, or it must not exist, as the root only consists of directories.
    async fn do_open(&self, req: &Request, op: op::Open<'_>) -> io::Result<()> {
        trace!("open");
        let inode = op.ino();
        let open = match self.map_inode(inode) {
            Inode::Drive(drive, local_inode) => self.open_drive(drive, local_inode).await,
            _ => Ok(TypeResult::DoesNotExist),
        };

        match open {
            Ok(TypeResult::IsType(open_out)) => req.reply(open_out)?,
            Ok(TypeResult::IsNotType) => req.reply_error(libc::EISDIR)?,
            Ok(TypeResult::DoesNotExist) => req.reply_error(libc::ENOENT)?,
            Err(_) => {
                // TODO: log error here
                req.reply_error(libc::EIO)?
            }
        }

        Ok(())
    }

    /// Pass a FUSE `opendir()` call to the correct sub-method (root, drive).
    async fn do_opendir(&self, req: &Request, op: op::Opendir<'_>) -> io::Result<()> {
        let inode = op.ino();
        let opendir = match self.map_inode(inode) {
            Inode::Root => Ok(TypeResult::IsType(self.opendir_root().await)),
            Inode::Drive(drive, local_inode) => self.opendir_drive(drive, local_inode).await,
            _ => Ok(TypeResult::DoesNotExist),
        };

        match opendir {
            Ok(TypeResult::IsType(open_out)) => req.reply(open_out)?,
            Ok(TypeResult::IsNotType) => req.reply_error(libc::ENOTDIR)?,
            Ok(TypeResult::DoesNotExist) => req.reply_error(libc::ENOENT)?,
            Err(_) => req.reply_error(libc::EIO)?,
        }

        Ok(())
    }

    /// Handle a FUSE `read()` call.
    ///
    /// This uses a file handle successfully retrieved from a previous `open()` call.
    /// The calls are passed through a `CacheFileHandler` reader, which does all the fun stuff.
    async fn do_read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        trace!("read begin");
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
            if *f_offset != offset && reader.seek_to(offset).await.is_err() {
                req.reply_error(libc::EIO)?;
                return Ok(());
            }

            if *f_offset != offset {
                trace!("SEEK {}, {} to {}", fh, f_offset, offset);
            }

            // println!("READ {}, {} {}, size: {}", fh, *f_offset, offset, size);

            // FUSE requires us to give an exact size, so make sure the buffer is large
            // enough to accommodate the read. We only give a smaller size on EOF.
            if size > buffer.len() {
                buffer.resize_with(size, Default::default);
            }

            // Read, and push our expected offset forward.
            trace!("begin actual read");
            match reader.read_exact(&mut buffer[..size]).await {
                Ok(read_len) => {
                    trace!("end actual read");
                    *f_offset = offset + read_len as u64;

                    req.reply(&buffer[..read_len])?
                }
                Err(e) => {
                            trace!("end actual read, but with error: {e}");
                            req.reply_error(libc::EIO)?
                        }
            }
        } else {
            req.reply_error(libc::ENOENT)?
        }

        trace!("read end");
        Ok(())
    }

    /// Handle a FUSE `release()` call.
    ///
    /// This is usually called when a file is closed.
    async fn do_release(&self, req: &Request, op: op::Release<'_>) -> io::Result<()> {
        let fh = op.fh();
        self.file_handles.close(fh).await;
        println!("REL {}, {}", fh, &self.file_handles.len().await);

        req.reply(())?;

        Ok(())
    }

    /// Handle a FUSE `unlink()` call.
    ///
    /// This allows deleting files on drives that support it.
    async fn do_unlink(&self, req: &Request, op: op::Unlink<'_>) -> io::Result<()> {
        let inode = op.parent();
        let name = op.name();
        let unlink = match self.map_inode(inode) {
            Inode::Drive(drive, local_inode) => {
                self.unlink_drive(drive, local_inode, name).await.is_some()
            }
            _ => false,
        };

        if unlink {
            req.reply(())?
        } else {
            req.reply_error(libc::ENOENT)?;
        }

        Ok(())
    }

    /// Handle a FUSE `rename()` call.
    ///
    /// This allows renaming or moving a file.
    async fn do_rename(&self, req: &Request, op: op::Rename<'_>) -> io::Result<()> {
        let parent = op.parent();
        let name = op.name();
        let new_parent = op.newparent();
        let new_name = op.newname();
        // TODO: For now, the flags are ignored. They should probably be handled at some point.

        let rename = match (self.map_inode(parent), self.map_inode(new_parent)) {
            (
                Inode::Drive(drive, local_parent_inode),
                Inode::Drive(new_drive, local_new_parent_inode),
            ) if drive == new_drive => self
                .rename_drive(
                    drive,
                    local_parent_inode,
                    name,
                    local_new_parent_inode,
                    new_name,
                )
                .await
                .is_some(),
            _ => false,
        };

        if rename {
            req.reply(())?
        } else {
            req.reply_error(libc::ENOENT)?;
        }

        Ok(())
    }
}

struct AsyncSession {
    inner: AsyncFd<Session>,
}

impl AsyncSession {
    async fn mount(mountpoint: Utf8PathBuf, config: KernelConfig) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let session = Session::mount(mountpoint.into_std_path_buf(), config)?;
            //session.notifier()
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
