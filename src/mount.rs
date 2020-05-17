use crate::drive_cache::{ReadItem, DriveCache};
use anyhow::Result;
use polyfuse::{
    io::{Reader, Writer},
    op,
    reply::{ReplyAttr, ReplyEntry},
    Context, DirEntry, FileAttr, Filesystem, Operation,
};
use std::{
    ffi::OsStr,
    io,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ITEM_BITS: usize = 48;
const DRIVE_OFFSET: u64 = (1 << ITEM_BITS);
const ITEM_MASK: u64 = DRIVE_OFFSET - 1;

enum Inode {
    Root,
    Unknown,
    Drive(u16, u64),
}

struct GreediaFS {
    drives: Vec<DriveCache>,
    start_time: SystemTime,
}

impl GreediaFS {
    pub fn new(drives: Vec<DriveCache>) -> GreediaFS {
        let start_time = SystemTime::now();
        GreediaFS { drives, start_time }
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

    fn getattr_root(&self) -> FileAttr {
        let mut file_attr = FileAttr::default();
        file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.set_ino(1);
        file_attr.set_nlink(self.drives.len() as u32);
        file_attr.set_size(self.drives.len() as u64);
        file_attr.set_ctime(self.start_time);
        file_attr.set_mtime(self.start_time);
        file_attr
    }

    fn getattr_drive(&self, drive: u16) -> Option<FileAttr> {
        if self.drives.len() <= drive as usize {
            None
        } else {
            let mut file_attr = FileAttr::default();
            file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
            file_attr.set_ino(self.rev_inode(Inode::Drive(drive, 0)));
            file_attr.set_ctime(self.start_time);
            file_attr.set_mtime(self.start_time);
            Some(file_attr)
        }
    }

    fn getattr_item(&self, drive: u16, inode: u64) -> Option<FileAttr> {
        println!("getattr_item");
        let drive_cache = self.drives.get(drive as usize)?;
        
        drive_cache.read_item(inode, readitem_to_fileattr).unwrap()
    }

    fn readdir_root(&self, offset: u64) -> Vec<DirEntry> {
        println!("readdir root");
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
        let drive_cache = self.drives.get(drive as usize)?;
        let mut cur_size = 0;
        let out = drive_cache
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
        let drive = self.drives
            .iter()
            .position(|drive_cache| drive_cache.name == name)?;
        
        let mut file_attr = FileAttr::default();
        file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
        file_attr.set_ino(self.rev_inode(Inode::Drive(drive as u16, 0)));
        file_attr.set_ctime(self.start_time);
        file_attr.set_mtime(self.start_time);

        let mut reply_entry = ReplyEntry::default();
        reply_entry.ino(self.rev_inode(Inode::Drive(drive as u16, 0)));
        reply_entry.attr(file_attr);
        reply_entry.ttl_attr(TTL);
        reply_entry.ttl_entry(TTL);
        Some(reply_entry)
    }

    fn lookup_drive(&self, drive: u16, parent_inode: u64, name: &OsStr) -> Option<ReplyEntry> {
        let name = name.to_str()?;
        //println!("lookup_drive {}, {}, {}", drive, inode, name);
        let drive_cache = self.drives.get(drive as usize)?;

        if let Some((inode, file_attr)) = drive_cache.lookup_item(parent_inode, name, readitem_to_fileattr).unwrap() {
            let mut reply_entry = ReplyEntry::default();
            reply_entry.ino(self.rev_inode(Inode::Drive(drive, inode)));
            reply_entry.attr(file_attr);
            reply_entry.ttl_attr(TTL);
            reply_entry.ttl_entry(TTL);
            Some(reply_entry)
        } else {
            None
        }
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
            Some(attr) => cx.reply(ReplyAttr::new(attr).ttl_attr(TTL)).await?,
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

        //dbg!(&lookup);

        match lookup {
            Some(lookup) => cx.reply(lookup).await?,
            None => cx.reply_err(libc::ENOENT).await?,
        }

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
        //dbg!(&op);
        match op {
            Operation::Lookup(op) => self.do_lookup(cx, op).await,
            Operation::Getattr(op) => self.do_getattr(cx, op).await,
            Operation::Readdir(op) => self.do_readdir(cx, op).await,
            _ => Ok(()),
        }
    }
}

fn readitem_to_fileattr(read_item: ReadItem) -> FileAttr {
    let mut file_attr = FileAttr::default();
    match read_item {
        ReadItem::File { md5: _, size, modified_time} => {
            file_attr.set_mode(libc::S_IFREG as u32 | 0o444);
            file_attr.set_size(size);
            file_attr.set_mtime(UNIX_EPOCH + Duration::from_secs(modified_time));
        },
        ReadItem::Dir{ modified_time } => {
            file_attr.set_mode(libc::S_IFDIR as u32 | 0o555);
            file_attr.set_mtime(UNIX_EPOCH + Duration::from_secs(modified_time));
        }
    };

    file_attr
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
    Ok(polyfuse_tokio::mount(GreediaFS::new(drive_caches), mount_point, &fuse_args).await?)
}
