use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Result};
use cache_handlers::{
    crypt_context::CryptContext,
    filesystem::{lru::Lru, FilesystemCacheHandler},
};
use db::Db;
use downloaders::{gdrive::GDriveClient, timecode::TimecodeDrive, DownloaderClient};
use drive_access::DriveAccess;
use futures::future::join_all;
#[cfg(feature = "sctest")]
use hard_cache::HardCacher;
use mount::mount_thread;
use once_cell::sync::OnceCell;
use scanner::scan_thread;
use structopt::StructOpt;

mod cache_handlers;
mod config;
mod db;
mod downloaders;
mod drive_access;
mod fh_map;
mod hard_cache;
mod mount;
mod prio_limit;
mod scanner;
mod types;

use config::{validate_config, Config, ConfigGoogleDrive, ConfigTimecodeDrive, Tweaks};

pub static TWEAKS: OnceCell<Tweaks> = OnceCell::new();

pub fn tweaks() -> &'static Tweaks {
    TWEAKS.get().expect("TWEAKS not initialized")
}

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
        /// Full file for the smart cachers to cache/copy.
        #[structopt()]
        input: PathBuf,

        /// Output file that only contains the cached portions.
        #[structopt()]
        output: PathBuf,

        /// Number of seconds to cache (default 10).
        #[structopt(short, long, default_value = "10")]
        seconds: u64,

        /// Fill uncached bytes with a different byte instead of null (hex-encoded).
        #[structopt(short, long)]
        fill_byte: Option<String>,

        /// Fill uncached bytes with random bytes instead. Overrides fill_byte.
        #[structopt(short = "r", long)]
        fill_random: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
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

    let tweaks = cfg.tweaks.as_ref().cloned().unwrap_or_default();
    TWEAKS
        .set(tweaks)
        .expect("TWEAKS should not be set at this point");

    if !validate_config(&cfg) {
        bail!("Config didn't validate.");
    }

    let db_path = &cfg.caching.db_path;

    // Initialize DB
    let db = Db::open(&db_path.join("db_v1"))?;

    // Initialize LRU
    let lru = Lru::new(&db, db_path, cfg.caching.soft_cache_limit).await;

    // Initialize cache handlers
    let mut drives = Vec::new();
    if let Some(gdrives) = cfg.gdrive {
        drives.extend(
            get_gdrive_drives(&cfg.caching.db_path, lru.clone(), db.clone(), gdrives).await?,
        );
    }
    if let Some(timecode_drives) = cfg.timecode {
        drives.extend(
            get_timecode_drives(
                &cfg.caching.db_path,
                lru.clone(),
                db.clone(),
                timecode_drives,
            )
            .await?,
        );
    }

    // Start a scanner for each cache handler
    let mut join_handles = vec![];
    for drive_access in drives
        .iter()
        .filter(|(_, do_scan)| *do_scan)
        .map(|(da, _)| da)
    {
        join_handles.push(tokio::spawn(scan_thread(drive_access.clone())));
    }

    // Start a mount thread
    join_handles.push(tokio::spawn(mount_thread(
        drives.into_iter().map(|(da, _)| da).collect(),
        cfg.caching.mount_point,
    )));

    join_all(join_handles).await;

    Ok(())
}

async fn get_gdrive_drives(
    cache_path: &Path,
    lru: Lru,
    db: Db,
    gdrives: HashMap<String, ConfigGoogleDrive>,
) -> Result<Vec<(Arc<DriveAccess>, bool)>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    // DriveAccesses share clients, which is necessary for things like rate limiting.
    let mut clients: HashMap<String, Arc<GDriveClient>> = HashMap::new();
    // Since scans are done by client rather than by DriveAccess, we need to know all passwords
    // given to the drive configs, so that encrypted files are scanned properly.
    let mut client_crypts: HashMap<String, Vec<Arc<CryptContext>>> = HashMap::new();
    // We only want one scanner per drive ID, so check if a scanner already exists for a drive.
    let mut drive_set: HashSet<String> = HashSet::new();
    let mut da_out = Vec::new();

    for cfg_drive in gdrives.values() {
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
            let service_accounts = cfg_drive.service_accounts.unwrap_or_else(Vec::new);
            let sa_refs: Vec<_> = service_accounts.iter().map(|sa| sa.as_path()).collect();

            let new_client = Arc::new(
                GDriveClient::new(
                    &cfg_drive.client_id,
                    &cfg_drive.client_secret,
                    &cfg_drive.refresh_token,
                    &sa_refs,
                )
                .await?,
            );
            clients.insert(cfg_drive.client_id.clone(), new_client.clone());
            new_client
        };

        let do_scan = drive_set.insert(cfg_drive.drive_id.clone());

        let drive = client.open_drive(&cfg_drive.drive_id);

        let cache_handler = Box::new(FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            Some(lru.clone()),
            &hard_cache_root,
            &soft_cache_root,
            drive.into(),
        ));

        let root_path = cfg_drive.root_path.map(PathBuf::from);

        let da = DriveAccess::new(
            name.clone(),
            cache_handler,
            db.clone(),
            root_path,
            cfg_drive.password.is_some() && cfg_drive.password2.is_some(),
            client_crypts
                .get(&cfg_drive.drive_id)
                .cloned()
                .unwrap_or_else(Vec::new),
        );

        da_out.push((Arc::new(da), do_scan));
    }

    Ok(da_out)
}

async fn get_timecode_drives(
    cache_path: &Path,
    lru: Lru,
    db: Db,
    timecode_drives: HashMap<String, ConfigTimecodeDrive>,
) -> Result<Vec<(Arc<DriveAccess>, bool)>> {
    let hard_cache_root = cache_path.join("hard_cache");
    let soft_cache_root = cache_path.join("soft_cache");

    let mut da_out = Vec::new();
    for (name, cfg_drive) in timecode_drives {
        let drive = Arc::new(TimecodeDrive {
            root_name: cfg_drive.drive_id.clone(),
        });
        let cache_handler = Box::new(FilesystemCacheHandler::new(
            &cfg_drive.drive_id,
            Some(lru.clone()),
            &hard_cache_root,
            &soft_cache_root,
            drive,
        ));

        let da = DriveAccess::new(name.clone(), cache_handler, db.clone(), None, false, vec![]);

        da_out.push((Arc::new(da), true));
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
    TWEAKS.set(Tweaks::default()).expect("TWEAKS should not be set at this point");
    let meta = input
        .metadata()
        .expect("sctest could not get metadata for file");
    let file_name = input
        .file_name()
        .expect("sctest could not get input file name")
        .to_str()
        .expect("sctest could not convert file name to string")
        .to_string();
    let size = meta.len();

    let hard_cacher = HardCacher::new_sctest(input, output, seconds, fill_byte, fill_random);
    hard_cacher.process_sctest(file_name, size).await;

    Ok(())
}
