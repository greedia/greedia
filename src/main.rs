use std::{collections::HashMap, path::PathBuf, sync::Arc};
use std::{fs, path::Path};

use anyhow::{bail, Result};
use cache_handlers::{
    filesystem::{lru::Lru, FilesystemCacheHandler},
    CacheDriveHandler,
};
use downloaders::{gdrive::GDriveClient, timecode::TimecodeDrive, DownloaderClient};
//use futures::future::join_all;
use structopt::StructOpt;

// mod cache;
// mod cache_reader;
// mod crypt_context;
// mod downloader;
// mod downloader_inner;
// mod drive_access;
// mod drive_cache;
// mod encrypted_cache_reader;
// mod hard_cache;
// mod mount;
// mod open_file_map;
// mod scanner;
// mod soft_cache_lru;

// New stuff
mod cache_handlers;
mod config;
//mod db;
mod downloaders;
mod prio_limit;
mod types;

//#[allow(dead_code, unused_imports)]
//mod db_generated;

//use cache::Cache;
use config::{validate_config, Config, ConfigGoogleDrive, ConfigTimecodeDrive};
//use hard_cache::HardCacher;

#[derive(Debug, StructOpt)]
#[structopt(name = "greedia", about = "Greedily cache media and serve it up fast.")]
enum Greedia {
    /// Run greedia with a given config file.
    Run {
        #[structopt(short, long)]
        config_path: PathBuf,
    },

    #[cfg(feature = "sctest")]
    /// Test a smart cacher by copying the cached portions of a file.
    Sctest {
        /// Full file for this smart cacher to cache/copy.
        #[structopt(short, long)]
        input: PathBuf,

        /// Output file that only contains the cached portions.
        #[structopt(short, long)]
        output: PathBuf,

        /// Number of seconds to cache (default 10).
        #[structopt(short, long, default_value = "10")]
        seconds: u64,

        /// Fill uncached bytes with a different byte instead (hex-encoded).
        #[structopt(short, long)]
        fill_byte: Option<String>,

        /// Fill uncached bytes with random bytes instead. Overrides fill_byte.
        #[structopt(short = "r", long)]
        fill_random: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Greedia::from_args();

    match opt {
        Greedia::Run { config_path } => run(&config_path).await?,
        #[cfg(feature = "sctest")]
        Greedia::Sctest {
            input,
            output,
            seconds,
            fill_byte,
            fill_random,
        } => sctest(input, output, seconds, fill_byte, fill_random).await?,
    }

    Ok(())
}

async fn run(config_path: &Path) -> Result<()> {
    let config_data = fs::read(config_path)?;
    let cfg: Config = toml::from_slice(&config_data)?;

    if !validate_config(&cfg) {
        bail!("Config didn't validate.");
    }

    dbg!(&cfg);

    let db_path = &cfg.caching.db_path;

    // Initialize DB
    let db = sled::open(&db_path.join("db_v1"))?;

    // Initialize LRU
    let lru = Lru::new(&db, &db_path, cfg.caching.soft_cache_limit).await;

    // Initialize cache handlers
    let mut cache_handlers = HashMap::new();
    if let Some(gdrives) = cfg.gdrive {
        cache_handlers.extend(get_gdrive_drives(&cfg.caching.db_path, gdrives).await?);
    }
    if let Some(timecode_drives) = cfg.timecode {
        cache_handlers.extend(get_timecode_drives(&cfg.caching.db_path, timecode_drives).await?);
    }

    dbg!(&cache_handlers.keys());

    println!("WOOOO");

    Ok(())
}

async fn get_gdrive_drives(
    cache_path: &Path,
    gdrives: HashMap<String, ConfigGoogleDrive>,
) -> Result<HashMap<String, Box<dyn CacheDriveHandler>>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    let mut clients: HashMap<String, Arc<GDriveClient>> = HashMap::new();
    let mut fs_out = HashMap::new();

    for (name, cfg_drive) in gdrives {
        let client = if let Some(client) = clients.get(&cfg_drive.client_id) {
            client.clone()
        } else {
            let new_client = Arc::new(
                GDriveClient::new(
                    &cfg_drive.client_id,
                    &cfg_drive.client_secret,
                    &cfg_drive.refresh_token,
                )
                .await?,
            );
            clients.insert(cfg_drive.client_id.clone(), new_client.clone());
            new_client
        };

        let drive = client.open_drive(&cfg_drive.drive_id);

        let fs = FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            &hard_cache_root,
            &soft_cache_root,
            drive.into(),
        );

        fs_out.insert(name.clone(), fs);
    }

    Ok(fs_out)
}

async fn get_timecode_drives(
    cache_path: &Path,
    timecode_drives: HashMap<String, ConfigTimecodeDrive>,
) -> Result<HashMap<String, Box<dyn CacheDriveHandler>>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    let mut fs_out = HashMap::new();
    for (name, cfg_drive) in timecode_drives {
        let drive = Arc::new(TimecodeDrive {});
        let fs = FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            &hard_cache_root,
            &soft_cache_root,
            drive,
        );

        fs_out.insert(name.clone(), fs);
    }

    Ok(fs_out)
}

// async fn run(config_path: PathBuf, mount_point: PathBuf) -> Result<()> {
//     let config_data = fs::read(config_path)?;
//     let cfg: Config = toml::from_slice(&config_data)?;

//     let mount_point = mount_point;

//     // Validate password and password2 for each drive
//     if let Some(gdrive) = &cfg.gdrive {
//         for (name, drive) in gdrive {
//             if (drive.password.is_some() && drive.password2.is_none())
//                 || (drive.password.is_none() && drive.password2.is_some())
//             {
//                 bail!(
//                     "Drive '{}' error: both passwords must be set to use encryption.",
//                     name
//                 );
//             }
//         }
//     };

//     let (drive_caches, drive_accessors) = Cache::new(&cfg).await?;
//     let mut join_handles = Vec::with_capacity(1 + drive_caches.len());

//     for drive in drive_caches {
//         join_handles.push(tokio::spawn(scanner::scan_thread_eh(drive)));
//     }

//     join_handles.push(tokio::spawn(mount::mount_thread_eh(
//         drive_accessors,
//         mount_point,
//     )));

//     join_all(join_handles).await;

//     Ok(())
// }

#[cfg(feature = "sctest")]
async fn sctest(
    input: PathBuf,
    output: PathBuf,
    seconds: u64,
    fill_byte: Option<String>,
    fill_random: bool,
) -> Result<()> {
    dbg!(&input, &output, &seconds, &fill_byte, &fill_random);

    let meta = input.metadata().unwrap();
    let file_name = input.file_name().unwrap().to_str().unwrap().to_string();
    let size = meta.len();

    let hard_cacher = HardCacher::new_sctest(input, output, seconds, fill_byte, fill_random);
    hard_cacher.process_sctest(file_name, size).await;

    Ok(())
}
