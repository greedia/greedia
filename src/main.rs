use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Result};
use futures::future::join_all;
use structopt::StructOpt;

mod cache;
mod cache_reader;
mod config;
mod crypt_context;
mod downloader;
mod downloader_inner;
mod drive_access;
mod drive_cache;
mod encrypted_cache_reader;
mod hard_cache;
mod mount;
mod open_file_map;
mod scanner;
mod soft_cache_lru;
mod types;

#[allow(dead_code, unused_imports)]
mod db_generated;

use cache::Cache;
use config::Config;
use hard_cache::HardCacher;

#[derive(Debug, StructOpt)]
#[structopt(name = "greedia", about = "Greedily cache media and serve it up fast.")]
enum Greedia {
    /// Run a greedia config at a specified mount point.
    Run {
        #[structopt(short, long)]
        config_path: PathBuf,

        #[structopt(short, long)]
        mount_point: PathBuf,
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
        Greedia::Run {
            config_path,
            mount_point,
        } => run(config_path, mount_point).await?,
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

async fn run(config_path: PathBuf, mount_point: PathBuf) -> Result<()> {
    let config_data = fs::read(config_path)?;
    let cfg: Config = toml::from_slice(&config_data)?;

    let mount_point = mount_point;

    // Validate password and password2 for each drive
    for (name, drive) in &cfg.gdrive {
        if (drive.password.is_some() && drive.password2.is_none())
            || (drive.password.is_none() && drive.password2.is_some())
        {
            bail!(
                "Drive '{}' error: both passwords must be set to use encryption.",
                name
            );
        }
    }

    let mut join_handles = Vec::with_capacity(1 + cfg.gdrive.len());
    let (drive_caches, drive_accessors) = Cache::new(&cfg).await?;

    for drive in drive_caches {
        join_handles.push(tokio::spawn(scanner::scan_thread_eh(drive)));
    }

    join_handles.push(tokio::spawn(mount::mount_thread_eh(
        drive_accessors,
        mount_point,
    )));

    join_all(join_handles).await;

    Ok(())
}

#[cfg(feature = "sctest")]
async fn sctest(
    input: PathBuf,
    output: PathBuf,
    seconds: u64,
    fill_byte: Option<String>,
    fill_random: bool,
) -> Result<()> {
    dbg!(&input, &output, &seconds, &fill_byte, &fill_random);

    let hard_cacher = HardCacher::new_sctest(input, output, seconds, fill_byte, fill_random);
    hard_cacher.process_sctest().await;

    Ok(())
}
