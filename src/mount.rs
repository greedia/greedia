use crate::{
    drive_access::{DriveAccess, ReadItem},
    open_file_map::OpenFileMap, cache_reader::CacheRead,
};
use anyhow::Result;
use polyfuse::{
    io::{Reader, Writer},
    op,
    reply::{ReplyAttr, ReplyEntry, ReplyOpen},
    Context, DirEntry, FileAttr, Filesystem, Operation,
};
use std::{
    ffi::OsStr,
    io,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TTL_LONG: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ITEM_BITS: usize = 48;
const DRIVE_OFFSET: u64 = 1 << ITEM_BITS;
const ITEM_MASK: u64 = DRIVE_OFFSET - 1;

enum Inode {
    Root,
    Unknown,
    Drive(u16, u64),
}

struct GreediaFS {
    drives: Vec<DriveAccess>,
    start_time: SystemTime,
    open_files: OpenFileMap<Box<dyn CacheRead + 'static + Send>>,
}

impl GreediaFS {
    pub fn new(drives: Vec<DriveAccess>) -> GreediaFS {
        let start_time = SystemTime::now();
        let open_files = OpenFileMap::new();
        GreediaFS {
            drives,
            start_time,
            open_files,
        }
    }

    fn map_inode(&self, inode: u64) -> Inode {
        // The first 16 bytes if an inode are the drive_id, and the last 48 bytes are the item_id.
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

    fn getattr_root(&self) -> (FileAttr, Duration) {
        let mut file_attr = FileAttr::default();
        file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.set_ino(1);
        file_attr.set_nlink(self.drives.len() as u32);
        file_attr.set_size(self.drives.len() as u64);
        file_attr.set_ctime(self.start_time);
        file_attr.set_mtime(self.start_time);
        (file_attr, TTL_LONG)
    }

    fn getattr_drive(&self, drive: u16) -> Option<(FileAttr, Duration)> {
        if self.drives.len() <= drive as usize {
            None
        } else {
            let mut file_attr = FileAttr::default();
            file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
            file_attr.set_ino(self.rev_inode(Inode::Drive(drive, 0)));
            file_attr.set_ctime(self.start_time);
            file_attr.set_mtime(self.start_time);
            Some((file_attr, TTL_LONG))
        }
    }

    fn getattr_item(&self, drive: u16, inode: u64) -> Option<(FileAttr, Duration)> {
        let drive_access = self.drives.get(drive as usize)?;
        let global_inode = self.rev_inode(Inode::Drive(drive, inode));
        let mut file_attr = drive_access
            .read_item(inode, readitem_to_fileattr)
            .unwrap()?;
        file_attr.set_ino(global_inode);
        Some((file_attr, TTL_LONG))
    }

    fn readdir_root(&self, offset: u64) -> Vec<DirEntry> {
        self.drives
            .iter()
            .skip(offset as usize)
            .enumerate()
            .map(|(num, drive)| {
                DirEntry::dir(
                    &drive.name,
                    self.rev_inode(Inode::Drive(num as u16, 0)),
                    num as u64 + 1,
                )
            })
            .collect()
    }

    async fn readdir_drive(
        &self,
        drive: u16,
        inode: u64,
        offset: u64,
        size: u32,
    ) -> Option<Vec<DirEntry>> {
        let drive_access = self.drives.get(drive as usize)?;
        let mut cur_size = 0;
        let out = drive_access
            .read_dir(inode, offset, |item| {
                let ino = self.rev_inode(Inode::Drive(drive, item.inode));
                let off = item.off + 1;
                let dir_entry = if item.is_dir {
                    DirEntry::dir(item.name, ino, off)
                } else {
                    DirEntry::file(item.name, ino, off)
                };
                cur_size += dir_entry.as_ref().len();

                if cur_size < size as usize {
                    Some(dir_entry)
                } else {
                    None
                }
            })
            .unwrap();
        out
    }

    fn lookup_root(&self, name: &OsStr) -> Option<ReplyEntry> {
        let name = name.to_str()?;
        let drive_num = self
            .drives
            .iter()
            .position(|drive_cache| drive_cache.name == name)?;

        let mut file_attr = FileAttr::default();
        file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.set_ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        file_attr.set_ctime(self.start_time);
        file_attr.set_mtime(self.start_time);

        let mut reply_entry = ReplyEntry::default();
        reply_entry.ino(self.rev_inode(Inode::Drive(drive_num as u16, 0)));
        reply_entry.attr(file_attr);
        reply_entry.ttl_attr(TTL_LONG);
        reply_entry.ttl_entry(TTL_LONG);
        Some(reply_entry)
    }

    fn lookup_drive(&self, drive: u16, parent_inode: u64, name: &OsStr) -> Option<ReplyEntry> {
        let drive_access = self.drives.get(drive as usize)?;
        let name = name.to_str()?;

        if let Some((inode, mut file_attr)) = drive_access
            .lookup_item(parent_inode, name, readitem_to_fileattr)
            .unwrap()
        {
            let global_inode = self.rev_inode(Inode::Drive(drive, inode));
            file_attr.set_ino(global_inode);
            //println!("lookup_drive ({}) {}:{} -> {}", drive, parent_inode, name, inode);

            let mut reply_entry = ReplyEntry::default();
            reply_entry.ino(global_inode);
            reply_entry.attr(file_attr);
            reply_entry.ttl_attr(TTL_LONG);
            reply_entry.ttl_entry(TTL_LONG);
            Some(reply_entry)
        } else {
            None
        }
    }

    async fn open_drive(&self, drive: u16, local_inode: u64) -> Option<ReplyOpen> {
        let drive_access = self.drives.get(drive as usize)?;
        let file = drive_access.open_file(local_inode).await?;
        let fh = self.open_files.open(file).await;

        Some(ReplyOpen::new(fh))
    }

    async fn opendir_root(&self) -> ReplyOpen {
        let mut reply_open = ReplyOpen::new(0);
        reply_open.cache_dir(true);
        reply_open
    }

    async fn opendir_drive(&self, drive: u16, local_inode: u64) -> Option<ReplyOpen> {
        let drive_access = self.drives.get(drive as usize)?;
        let (is_dir, scanning) = drive_access.check_dir(local_inode);
        let is_dir = is_dir?;
        if !is_dir {
            // TODO: handle results in mount to be able to pass "not a directory"
        }
        let mut reply_open = ReplyOpen::new(0);
        reply_open.keep_cache(!scanning);
        reply_open.cache_dir(!scanning);
        Some(reply_open)
    }

    async fn do_getattr<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Getattr<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let attr = match self.map_inode(op.ino()) {
            Inode::Root => Some(self.getattr_root()),
            Inode::Unknown => None,
            Inode::Drive(drive, local_inode) if local_inode == 0 => self.getattr_drive(drive),
            Inode::Drive(drive, local_inode) => self.getattr_item(drive, local_inode),
        };

        match attr {
            Some((attr, ttl)) => cx.reply(ReplyAttr::new(attr).ttl_attr(ttl)).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

        Ok(())
    }

    async fn do_readdir<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let offset = op.offset();
        let size = op.size();

        let dir = match self.map_inode(op.ino()) {
            Inode::Root => Some(self.readdir_root(offset)),
            Inode::Unknown => None,
            Inode::Drive(drive, local_inode) => {
                self.readdir_drive(drive, local_inode, offset, size).await
            }
        };

        match dir {
            Some(dir) => cx.reply(dir).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

        Ok(())
    }

    async fn do_lookup<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Lookup<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let inode = op.parent();
        let name = op.name();
        let lookup = match self.map_inode(inode) {
            Inode::Root => self.lookup_root(name),
            Inode::Drive(drive, local_inode) => self.lookup_drive(drive, local_inode, name),
            _ => None,
        };

        match lookup {
            Some(lookup) => cx.reply(lookup).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

        Ok(())
    }

    async fn do_open<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Open<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let inode = op.ino();
        let open = match self.map_inode(inode) {
            Inode::Drive(drive, local_inode) => self.open_drive(drive, local_inode).await,
            _ => None,
        };

        match open {
            Some(open) => cx.reply(open).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

        Ok(())
    }

    async fn do_opendir<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Opendir<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let inode = op.ino();
        let opendir = match self.map_inode(inode) {
            Inode::Root => Some(self.opendir_root().await),
            Inode::Drive(drive, local_inode) => self.opendir_drive(drive, local_inode).await,
            _ => None,
        };


        match opendir {
            Some(opendir) => cx.reply(opendir).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

        Ok(())
    }

    async fn do_read<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Read<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let fh = op.fh();
        let offset = op.offset();
        let size = op.size();
        if let Some(file) = self.open_files.get(fh).await {
            let mut reader = file.lock().await;
            let read_data = reader.read(offset, size).await.unwrap();
            cx.reply(&read_data).await?;
        } else {
            cx.reply_err(libc::ENOENT).await?
        }

        Ok(())
    }

    async fn do_release<T: ?Sized>(
        &self,
        _cx: &mut Context<'_, T>,
        op: op::Release<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let fh = op.fh();
        self.open_files.close(fh).await;

        Ok(())
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

        match op { 
            Operation::Lookup(op) => self.do_lookup(cx, op).await,
            Operation::Getattr(op) => self.do_getattr(cx, op).await,
            Operation::Readdir(op) => self.do_readdir(cx, op).await,
            Operation::Open(op) => self.do_open(cx, op).await,
            Operation::Opendir(op) => self.do_opendir(cx, op).await,
            Operation::Read(op) => self.do_read(cx, op).await,
            Operation::Release(op) => self.do_release(cx, op).await,
            _ => Ok(()),
        }
    }
}

fn readitem_to_fileattr(read_item: ReadItem) -> FileAttr {
    let mut file_attr = FileAttr::default();
    match read_item {
        ReadItem::File {
            md5: _,
            size,
            modified_time,
        } => {
            file_attr.set_mode(libc::S_IFREG as u32 | 0o444);
            file_attr.set_size(size);
            file_attr.set_mtime(UNIX_EPOCH + Duration::from_secs(modified_time));
        }
        ReadItem::Dir { modified_time } => {
            file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
            file_attr.set_mtime(UNIX_EPOCH + Duration::from_secs(modified_time));
        }
    };

    file_attr
}

/// Mount thread, with error handling
pub async fn mount_thread_eh(drive_accessors: Vec<DriveAccess>, mount_point: PathBuf) {
    mount_thread(drive_accessors, mount_point).await.unwrap();
}

/// Thread that handles all FUSE requests
pub async fn mount_thread(drive_accessors: Vec<DriveAccess>, mount_point: PathBuf) -> Result<()> {
    let fuse_args: Vec<&OsStr> = vec![
        &OsStr::new("-o"),
        &OsStr::new("auto_unmount,ro,allow_other"),
    ];
    Ok(polyfuse_tokio::mount(GreediaFS::new(drive_accessors), mount_point, &fuse_args).await?)
}
