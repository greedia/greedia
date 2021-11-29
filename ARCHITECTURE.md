# Overview

```
            Greedia Archiecture General Overview
            ====================================

                ┌──────────┐
                │FUSE Mount│
                └────┬─────┘
                     │
                     │
                     │
                     │             ┌───────┐
                     │             │Scanner├────────────┐     (one per scannable drive_id)
                     │             └───┬───┘            ▼
                     │                 │              ┌──────────────┐
                     ├─────────────────┼──────────────┤  HardCacher  │
                     │                 │              │(SmartCachers)│
                     ▼                 ▼              └──────────────┘
               ┌───────────┐    ┌───────────┐
       ┌───────┤DriveAccess│    │DriveAccess├───────┐         (one per greedia drive)
       │       └─────┬───┬─┘    └─┬────┬────┘       │
       │             │   │        │    │            │
 ┌─────┴──────┐      │   │  ┌──┐  │    │      ┌─────┴──────┐
 │CryptContext│      │   └──┤DB├──┘    │      │CryptContext│
 └────────────┘      │      └─┬┘       │      └────────────┘
                     │        │        │
                     │        │        │
         ┌───────────┴─────┐  │ ┌──────┴──────────┐
         │CacheDriveHandler│  │ │CacheDriveHandler│           (one per drive_id)
         └─┬───────┬─────┬─┘  │ └─┬─────┬───────┬─┘
           │       │     │    │   │     │       │
           │       │     │    │   │     │       │
┌──────────┴─────┐ │  ┌──┴────┴───┴──┐  │ ┌─────┴──────────┐
│Mutex<OpenFiles>│ │  │Soft Cache LRU│  │ │Mutex<OpenFiles>│
└──────────┬─────┘ │  └──────────────┘  │ └─────┬──────────┘
           │       │                    │       │
           ▼       │                    │       ▼
      ┌────────────┴──┐              ┌──┴────────────┐
      │DownloaderDrive│              │DownloaderDrive│        (one per client_id)
      └───────────────┘              └───────────────┘
```

## Overview

In Greedia, the database consists of a key-value sled database, with values being rkyv-serialized types.

The database, LRU, and cache handlers are initialized in `main.rs`, with deduplication occuring along
DownloaderDrive and CacheDriveHandler lines. Then, the scanner tasks (one per CacheDriveHandler) are initiated
and a mount task (for running the FUSE mount) is started.

From this point, all actions are initiated from either the FUSE mount (someone accessing a file),
or from a scanner (a watcher finding a new file in the drive).

The following components are presented in alphabetical order.

## CacheDriveHandler

The `CacheDriveHandler` trait is contained in `src/cache_handlers/mod.rs`. It is the interface to the caches and
downloader for each drive. The most important method on the trait is `open_file()`, which returns a `CacheFileHandler`.
`CacheFileHandler`s are detailed further under the File Access Overview below.

Many of the methods here are passthroughs to methods on `DownloaderDrive`, to be used by scanners or the FUSE mount via
`DriveAccess` structs.

At the moment, there is only one `CacheDriveHandler` implementation, and that is the "filesystem" implementation, found
at `src/filesystem/mod.rs`. The filesystem implementation stores everything on disk in a "soft_cache" and "hard_cache"
directory, found at `$DB_PATH/soft_cache` and `$DB_PATH/hard_cache`, respectively (depending on what's set in the config
file's "caching" section). More information about what's contained in the caches can be found in File Access Overview.

## CryptContext

The `CryptContext` struct can be found in `src/cache_handlers/crypt_context.rs`.
It contains all details required to decrypt files, so long as a password is provided in the config file.
This struct can be found in `src/cache_handlers/crypt_context.rs`.

When a `DriveAccess` containing `CryptContext`s tries to open a file, each `CryptContext` will be iterated through until
one of them successfully decrypts the filename. If this happens, a `CryptPassthrough` `CacheFileHandler` is made, which
passes decrypted data through. On the other hand, if no decryption succeeds, it is opened as a regular file without
decryption.

## DB

The DB structures are contained in `src/db`. Depending on which type of access is required, there are a few interfaces
to the database:
- Regular access is done through the `DbAccess` struct, found at `src/db/access.rs`.
- LRU access is done through the `LruAccess` struct, found at `src/db/lru_access.rs`.

In regular access, a set of trees based on the drive ID are created. The trees can be found in `src/db/tree.rs`. They are used to store:
- Inode data - a mapping from a drive-specific inode to a `DriveItem`, and is typically used when opening files
from a FUSE mount.
- Access data - a mapping from a downloader-specific ID to an inode. This is mostly useful for the scanner to
access new files added from the watch thread or initial scan.
- Scan data - a few variables used to keep track of scanning progress.
- Lookup data - a mapping from a tuple of (drive-specific parent inode, filename) to an inode, which allows FUSE lookup
calls to be efficient.
- Reverse access data (raccess) - a mapping from an access ID to a `ReverseAccess` struct. This is used to delete
keys from other trees when a file is deleted on the underlying `DownloaderDrive`, i.e. removes an inode, a parent's 
lookup key, and removes the entry from a parent directory's inode.

In LRU access, there are also trees, but only two sets:
- A tree that holds general LRU data, such as current space usage.
- A timestamp tree that holds a mapping from timestamps to chunk files on disk. This is used to clear the least recent 
files from disk when the soft cache fills up.
- A data tree that holds a mapping from a chunk file to a timestamp key. This is used to "touch" files when accessed,
to signify the file was recently used.

All types used within the database are rkyv-serialized, and are found in `src/db/types.rs`.

## DownloaderDrive

The `DownloaderDrive` trait can be found in `src/downloaders/mod.rs`, and is used to expose a standard interface to a
cloud file storage service. It handles scanning a new drive for all files, watching for changes, and opening streams to
cloud files for downloading. It can even handle moving and deleting files, however that's optional.

The way `CacheFileHandler`s interact with `DownloaderDrive`s is intended to keep the `DownloaderDrive` side as simple
as possible. In the case of the gdrive downloader, open_file simply makes an HTTP request to the Drive API with a
Range header starting at a provided offset.

At the moment, there are only two `DownloaderDrive` implementations:
- gdrive, which can be found in `src/downloaders/gdrive.rs`
- timecode, which can be found in `src/downloaders/timecode.rs`. This one is mainly used for tests and debugging. For more
information on what timecode is and how it works, check out https://github.com/greedia/timecode_rs.

The gdrive API is implemented from scratch using reqwest. It uses a leaky_bucket-based rate limiter called `PrioLimit`,
which allows setting priorities on requests. Requests to refresh the access token is the highest priority,
then foreground requests, then background requests. For foreground and background requests, retry requests take one
priority higher than regular requests. PrioLimit can be found in `src/prio_limit.rs`.

The gdrive downloader has two ways to access a drive: client oauth2 credentials, and a service account. It's important
to have oauth2 credentials, as there are some actions service accounts cannot perform. On the other hand, more service
accounts mean many more requests per second. For most requests, the client/service accounts are round-robin'd.

## DriveAccess

The `DriveAccess` struct can be found in `src/drive_access.rs`, and is used to integrate the database with a
`CacheDriveHandler`, as well as bridge scanning status with the kernel caching of the FUSE mount. It also handles the
encryption aspect of file access, as `CacheDriveHandler` and `CacheFileHandler` are limited strictly to data as found
in a cloud drive.

## FUSE Mount

## HardCacher (Smart Cachers)

## Scanner

## Soft Cache LRU

# File Access

```
 Greedia Architecture File Access Overview
 =========================================

  ┌───────────┐             ┌───────────┐
  │FUSE Handle│             │FUSE Handle│
  └────┬──────┘             └────┬──────┘
       │                         │
       │                         │
       ▼                         ▼
┌────────────────┐        ┌────────────────┐
│CacheFileHandler│        │CacheFileHandler│
└────────┬─────┬─┘        └─┬─────┬────────┘
         │     │            │     │
         │     │            │     │
         │     ▼            ▼     │
         │   ┌───────────────┐    │
         │   │Mutex<OpenFile>│    │          (one per drive file)
         │   └───────┬───────┘    │
         │           │            │
         │           ▼            │
         │   ┌───────────────┐    │
         │   │DownloaderDrive│    │
         │   └───────────────┘    │
         │                        │
         │   ┌───────────────┐    │
         ├──►│Soft Cache LRU │◄───┤
         │   └───────────────┘    │
         ▼                        ▼
        ┌──────────────────────────┐
        │(Direct Filesystem Access)│
        └──────────────────────────┘
```

## Overview

The following components are presented in alphabetical order.

## CacheFileHandler

Both caches on disk share the same format, and are dependent on the `DataIdentifier` enum found in `src/db/types.rs`. At
the moment, the only type of data identifier is `GlobalMd5`, which stores files based on its md5sum. These files are stored in a directory hierarchy to limit too many directories in one directory. For example, if a file's md5sum were
equal to "2ab96390c7dbe3439de74d0c9b0b1767", you would find its hard cache chunks at
`$DB_PATH/hard_cache/global_md5/2a/b9/2ab96390c7dbe3439de74d0c9b0b1767/`. The soft cache chunks would be found at the 
same path, just replacing hard_cache with soft_cache.



## DownloaderDrive

## FUSE Handle

## Mutex<OpenFile>

## Soft Cache LRU