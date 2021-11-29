# Overview

```
           Greedia Architecture General Overview
           =====================================

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

The entirety of Greedia is built on top of tokio and async futures.

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

In regular access, a set of trees based on the drive ID are created. The trees can be found in `src/db/tree.rs`.
They are used to store:
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

Many of the methods in a `DriveAccess` access the database to find the access_id and data_id of a file given an inode
from the FUSE mount, and pass it through a `CacheFileHandler`, optionall with a `CryptPassthrough`. It also handles the
database side of moves, renames, and deletes.

## FUSE Mount

The FUSE mount is a thread function that can be found in `src/mount.rs`. It uses the polyfuse crate under the hood to
provide an asynchronous interface between FUSE and the rest of Greedia, and does so with a much more recent FUSE protocol
than many other FUSE crates.

Due to the alpha nature of Greedia, many of the FUSE mount options are hard-coded. This should probably be changed in the
future.

The root of the FUSE mount contains one directory for each drive specified in the config file (i.e. one directory per
`DriveAccess`). FUSE handles files via inodes, where each inode represents either a single file or directory.

Within the database, inodes are stored on a per-drive basis, which allows drives to be put in the config file in any order
without causing corruption. However, FUSE expects inodes to not collide across the whole mount, i.e. *all* specified
drives. To accommodate this, all FUSE inodes (64 bits) are split into a 16-bit drive ID and a 48-bit local inode. There
are `map_inode` and `rev_inode` methods provided to translate between the two types of inodes.

Beyond that, most of the methods are implementations for FUSE calls that are either handled by the mount thread (if in
the root of the mount), or by a specific `DriveAccess` (if inside one of the drive directories).

## HardCacher (Smart Cachers)

The `HardCacher` struct can be found in `src/hard_cache/mod.rs`, and is used to handle adding data to the hard cache via
smart cachers. It's called by the scanner thread, and also handles picking which smart cacher to use for a given file.

Given a file name, the first thing the `HardCacher` tries to do is pick a smart cacher based on file extension. Each
smart cacher specifies which file extensions it should work on in its `SmartCacherSpec`. It will try to cache using
every smart cacher that matches a specific extension until one of them succeeds (by calling finalize). If all extension
smart cachers fail, it will attempt to use all other smart cachers. If every smart cacher fails, use the generic cacher.
If any file is below the min_size as specified in the config (defaults to 100KB), no smart cachers are executed, and the
entire file is cached.

Each smart cacher implements the `SmartCacher` trait, which just has a `cache()` method that's passed file info and a
`HardCacheDownloader`. The `HardCacheDownloader` provides actions that a smart cacher can execute, such as reading data
at a specified offset, telling the downloader to cache a range of data, and to finalize or cancel a cache operation.

When a smart cacher succeeds, `metadata.json` is added to the hard_cache directory for said file. This json contains
the name of the cacher that succeeded on this file, as well as a global "version" number. This number is specified in
`src/hard_cache/smart_cacher.rs` as the `SMART_CACHER_VERSION` constant. When changes to the smart cacher implementations
are added, this constant should be incremented, which will cause `HardCacher` to re-run the smart cachers on that file.

Note that every smart cacher should try to "fail fast", that is, as soon as it detects that it won't successfully cache
the beginning of a file, it should call cancel. This is because every smart cacher is called until one succeeds. Also
note that if a smart cacher fails, the data it has read will still be used in the next smart cacher, and all download
threads remain open, to minimize the impact of a smart cacher failing.

There are currently two working smart cachers:
- sc_mp3 - used to cache MP3 files, found at `src/hard_cache/sc_mp3.rs`. This one is fairly simple, and simply tries to
skip the ID3v2 tags to find the MP3 header. From there, it gets the MP3 bitrate (assuming 320kbps if VBR), and multiplies
the bitrate with the number of seconds to find the number of bytes to download. It also grabs the last 128 bytes because
it may contain ID3v1 tags.
- sc_mkv - used to cache MKV-contained media, found at `src/hard_cache/sc_mkv/mod.rs`. This one's quite a bit more
complicated, as MKV files are encoded in a format called EBML. This one pulls in an external crate called bitstream-io,
forked by Greedia to support async operations and fit in with tokio, in order to read EBML. From there, the MKV smart
cacher reads the root EBML element and attempts to grab common sections of an MKV file, such as chapters, attachments,
and tags. Finally, it will look for MKV "clusters", which each contain a timestamp, and grab them sequentially until
it finds a cluster whose timestamp is past the provided number of seconds.

For developing and testing new smart cachers, there's a Greedia feature called `sctest` which, when built with the
feature activated, adds an sctest command which takes an input file, and outputs a file with only the data that
would have been cached by a smart cacher. That output file can then be put into a media player, and if 10 seconds
are successfully played back, the smart cacher has been implemented correctly.

## Mutex\<OpenFiles\>

The map containing all open files can be found in `src/filesystem/mod.rs` in the `FilesystemCacheHandler` struct. 
They are stored as weak references in order to make sure all downloaders for a file are dropped once the handle
to a file itself is dropped. The `OpenFile` struct is detailed further in the File Access Overview below.

## Scanner

The scanner thread can be found in `src/scanner.rs`, and is responsible for initially scanning for all files in
newly-added drives. Pages of files are given in arbitrary order from the `DownloaderDrive`'s `scan_pages()` method
(passed through DriveAccess), and these pages are organized into directories based on the parents of each given
item in these pages.

Once a scan is complete, the scanner thread begins caching all files. It does so by calling into the `HardCacher`
struct, which will then call out to the proper smart cachers to grab the beginning seconds of the media file.

On top of the scanning and caching, a watch thread is created that listens to the `DownloaderDrive`'s `watch_changes()`
method for newly-added files or deleted files, and updates the database and caches accordingly.

## Soft Cache LRU

The soft cache LRU can be found in `src/cache_handlers/filesystem/lru.rs`, as it is specific to the filesystem cache
handler. It is used to keep the soft_cache directory approximately below the limit specified in the config. It works
asynchronously as a background thread, and messages are sent over a channel to said thread when a new chunk file is added,
deleted, accessed, or appended to. This means the size limit isn't explicitly guaranteed, but it should stay pretty close
to the size limit, within about 100MB.

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

The mechanisms for accessing data within a file are fairly complicated, and so there's a separate section outlining
what each file-specific component does.

The following components are presented in alphabetical order.

## CacheFileHandler

The `CacheFileHandler` struct can be found in `src/cache_handlers/mod.rs`, and provides methods to read and cache
data, as well as seek through, an open file.

Like with `CacheDriveHandler`, there is only one implementation of `CacheFileHandler`, and that is
`FilesystemCacheFileHandler` found in `src/cache_handlers/filesystem/mod.rs`.

When a file is first opened, a unique `OpenFile` is created in the `FilesystemCacheDriveHandler`. Whenever more processes
open this file, the same `OpenFile` instance is used, as Greedia must synchronize accesses between these handles.

The `FilesystemCacheFileHandler` is responsible for reading from both hard and soft caches, writing to the soft cache,
and reading from a downloader when data does not exist in either cache.

Both caches on disk share the same format, and are dependent on the `DataIdentifier` enum found in `src/db/types.rs`. At
the moment, the only type of data identifier is `GlobalMd5`, which stores files based on its md5sum. These files are
stored in a directory hierarchy to limit too many directories in one directory. For example, if a file's md5sum were
equal to "2ab96390c7dbe3439de74d0c9b0b1767", you would find its hard cache chunks at
`$DB_PATH/hard_cache/global_md5/2a/b9/2ab96390c7dbe3439de74d0c9b0b1767/`. The soft cache chunks would be found at the 
same path, just replacing hard_cache with soft_cache. Within this directory, each cache chunk exists as a file called
`chunk_<offset>` where offset is the offset within the original cloud file. For example, a file named `chunk_0` would
contain the data right at the beginning of the file.

The first step taken when a file is opened is to use `ByteRanger` to load the chunk offsets and lengths into a format
that's efficient to query, create one for each of the hard and soft caches, and store that data in the synchronized
`OpenFile`. (For more information on `ByteRanger`, visit https://github.com/greedia/byte-ranger)

Next, once a handle tries to read from a file, a chunk needs to be found. This means locking the `OpenFile`, querying
both the hard_cache and soft_cache `ByteRanger`s, and getting the `ChunkData` associated with the chunk.

If a chunk exists in the `ByteRanger`, that `ChunkData` struct will contain a value called `end_offset`. It states how 
much data in the chunk has already been downloaded to the cache. If we don't need to go any further than that offset, we 
can read data directly from disk without needing to re-lock `OpenFile`. This pretty heavily relies on Linux kernel
filesystem caching specifics, to be able to read from a chunk file while another handle may be appending to it at the
same time.

If a chunk cannot be found at an offset, a new chunk needs to be created, which involves creating the `chunk_<offset>`
chunk file on disk, adding a new range to the soft_cache `ByteRanger`, and creating a `DownloadStatus` struct within that
range's `ChunkData`. The `DownloadStatus` struct contains the actual download stream used to write to the new chunk,
as well as some internal buffering details.

When a chunk is being downloaded and written to, the `ChunkData` struct occasionally has its `end_offset` value updated,
which allows other handles to read from that same file up to the `end_offset` safely, without having to re-lock the
`OpenFile` on every read operation.

There's a lot more to `CacheFileHandler` than is described here, and is definitely the most complicated aspect of Greedia,
so it is recommended you read through `src/cache_handlers/filesystem/mod.rs` and
`src/cache_handlers/filesystem/open_file.rs` for a better understanding of how file reads are performed.

## DownloaderDrive

The file-specific details of `DownloaderDrive` are fairly simple, as it's just returning an async stream of Bytes (from
the bytes crate). Once the `CacheFileHandler` closes the file, reaches the start of another cache chunk, or reaches the 
end of the file, the stream is simply closed and the HTTP (or other) connection to the cloud drive is closed.

## FUSE Handle

When a file is opened from the FUSE mount, a new `FileHandle` is created and stored in an `FhMap`. The `FhMap` is
basically just a hash map with a very efficient hash, as well as a way to ensure file handle IDs aren't repeated.
A `FileHandle` simply includes a `CacheFileHandler` and an offset, and FUSE read/seek operations work on these values.

## OpenFile

The `OpenFile` struct can be found in `src/cache_handlers/filesystem/open_file.rs`, and is used to represent a single
open file within a cache. It is used for synchronizing between multiple handles accessing the same file, of which any
of them may be downloading, reading from hard or soft caches, or writing to the soft cache. `OpenFile` is protected by
a mutex, and must be locked on most operations.

In some situations, a `CacheFileHandler` may read data from a cache without locking the `OpenFile`. This is further
explained in the `CacheFileHandler` section of this document.

## Soft Cache LRU

The LRU works on a drive-wide basis, and operates over the entire `soft_cache` directory for a whole Greedia instance,
not on a per-file basis. With that said though, `OpenFile` structs do contain a reference to the LRU, and will update
the LRU whenever a file is opened or more data within a file is downloaded.

# Other

## Testing

`CacheFileHandler` uses proptest for testing, and the testing framework can be found at the end of
`src/cache_handlers/filesystem/mod.rs`, with methods called `tester()` and `tester_crypt()`.

The general strategy is to open one file with three handles, and have each handle perform random seeks, reads, and caches.
Occasionally, they may also reopen the file.

When a file is opened, it may be opened to write to hard_cache, or it may be opened "normally" (like if a FUSE mount 
opened it).

Additionally, when a read occurs, timecode is used to ensure the data was read at the proper offset.

By default, the timecode drive is used for testing and is done locally, however this does not support `CryptPassthrough`.

In order to test crypt, a timecode.bin file needs to be uploaded to a google drive via an rclone drive+crypt drive, and
environment variables at the start of `tester_crypt()` must be set to point to that file.

Some of the test methods have a `keep_path` variable that's set to false by default. If set to true, the cache
directories will be kept which can allow for further debugging.