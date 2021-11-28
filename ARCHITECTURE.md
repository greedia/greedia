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

### CacheDriveHandler

### CryptContext

### DB

### DownloaderDrive

### DriveAccess

### FUSE Mount

### HardCacher (Smart Cachers)

### Scanner

### Soft Cache LRU

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

### CacheFileHandler

### DownloaderDrive

### FUSE Handle

### Mutex<OpenFile>

### Soft Cache LRU