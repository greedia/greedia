use std::{collections::HashMap, path::PathBuf, sync::Arc};
use std::{fs, path::Path};

use anyhow::{bail, Result};
use cache_handlers::filesystem::{lru::Lru, FilesystemCacheHandler};
use crypt_context::CryptContext;
use db::Db;
use downloaders::{gdrive::GDriveClient, timecode::TimecodeDrive, DownloaderClient};
use drive_access2::DriveAccess;
use futures::future::join_all;
use mount::mount_thread;
use scanner2::scan_thread;
use structopt::StructOpt;

#[cfg(feature = "sctest")]
use hard_cache::HardCacher;

// mod cache;
// mod cache_reader;
// mod downloader;
// mod downloader_inner;
// mod drive_access;
// mod drive_cache;
// mod encrypted_cache_reader;
// mod scanner;
// mod soft_cache_lru;

// New stuff
mod cache_handlers;
mod config;
mod crypt_context;
mod db;
mod downloaders;
mod drive_access2;
mod fh_map;
mod hard_cache;
mod mount;
mod prio_limit;
mod scanner2;
mod types;

use config::{validate_config, Config, ConfigGoogleDrive, ConfigTimecodeDrive};

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
        #[structopt()]
        input: PathBuf,

        /// Output file that only contains the cached portions.
        #[structopt()]
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

    let db_path = &cfg.caching.db_path;

    // Initialize DB
    let db = Db::open(&db_path.join("db_v1"))?;

    // Initialize LRU
    let lru = Lru::new(&db, &db_path, cfg.caching.soft_cache_limit).await;

    // Initialize cache handlers
    let mut drives = Vec::new();
    if let Some(gdrives) = cfg.gdrive {
        drives.extend(get_gdrive_drives(&cfg.caching.db_path, db.clone(), gdrives).await?);
    }
    if let Some(timecode_drives) = cfg.timecode {
        drives
            .extend(get_timecode_drives(&cfg.caching.db_path, db.clone(), timecode_drives).await?);
    }

    // Start a scanner for each cache handler
    let mut join_handles = vec![];
    for drive_access in &drives {
        join_handles.push(tokio::spawn(scan_thread(drive_access.clone())));
    }

    // Start a mount thread
    join_handles.push(tokio::spawn(mount_thread(drives, cfg.caching.mount_point)));

    join_all(join_handles).await;

    Ok(())
}

async fn get_gdrive_drives(
    cache_path: &Path,
    db: Db,
    gdrives: HashMap<String, ConfigGoogleDrive>,
) -> Result<Vec<Arc<DriveAccess>>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    // DriveAccesses share clients, which is necessary for things like rate limiting.
    let mut clients: HashMap<String, Arc<GDriveClient>> = HashMap::new();
    // Since scans are done by client rather than by DriveAccess, we need to know all passwords
    // given to the drive configs, so that encrypted files are scanned properly.
    let mut client_crypts: HashMap<String, Vec<Arc<CryptContext>>> = HashMap::new();
    let mut da_out = Vec::new();

    for (_, cfg_drive) in &gdrives {
        if let (Some(password), Some(password2)) = (&cfg_drive.password, &cfg_drive.password2) {
            let cc = CryptContext::new(password, password2)?;
            if let Some(v) = client_crypts.get_mut(&cfg_drive.drive_id) {
                v.push(Arc::new(cc));
            } else {
                client_crypts.insert(cfg_drive.drive_id.clone(), vec![Arc::new(cc)]);
            }
        }
    }

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

        let cache_handler = FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            &hard_cache_root,
            &soft_cache_root,
            drive.into(),
        );

        let root_path = cfg_drive.root_path.map(|x| PathBuf::from(x));

        let da = DriveAccess::new(
            name.clone(),
            cache_handler,
            db.clone(),
            root_path,
            client_crypts
                .get(&cfg_drive.drive_id)
                .cloned()
                .unwrap_or(vec![]),
        );

        da_out.push(Arc::new(da));
    }

    Ok(da_out)
}

async fn get_timecode_drives(
    cache_path: &Path,
    db: Db,
    timecode_drives: HashMap<String, ConfigTimecodeDrive>,
) -> Result<Vec<Arc<DriveAccess>>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    let mut da_out = Vec::new();
    for (name, cfg_drive) in timecode_drives {
        let drive = Arc::new(TimecodeDrive {});
        let cache_handler = FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            &hard_cache_root,
            &soft_cache_root,
            drive,
        );

        let da = DriveAccess::new(name.clone(), cache_handler, db.clone(), None, vec![]);

        da_out.push(Arc::new(da));
    }

    Ok(da_out)
}

#[cfg(feature = "sctest")]
async fn sctest(
    input: PathBuf,
    output: PathBuf,
    seconds: u64,
    fill_byte: Option<String>,
    fill_random: bool,
) -> Result<()> {
    let meta = input.metadata().unwrap();
    let file_name = input.file_name().unwrap().to_str().unwrap().to_string();
    let size = meta.len();

    let hard_cacher = HardCacher::new_sctest(input, output, seconds, fill_byte, fill_random);
    hard_cacher.process_sctest(file_name, size).await;

    Ok(())
}
