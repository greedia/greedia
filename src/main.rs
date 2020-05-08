use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use futures::future::join_all;
use structopt::StructOpt;

mod cache;
mod cache_reader;
mod config;
mod crypt_context;
#[allow(warnings)]
mod db_generated;
mod downloader;
mod drive_cache;
mod mount;
mod scanner;
mod soft_cache_lru;
mod types;

use cache::Cache;
use config::Config;

#[derive(Debug, StructOpt)]
#[structopt(name = "greedia", about = "Greedily cache media and serve it up fast.")]
struct Opt {
    #[structopt(short, long)]
    config_path: PathBuf,

    #[structopt(short, long)]
    mount_point: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let config_data = fs::read(opt.config_path)?;
    let cfg: Config = toml::from_slice(&config_data)?;

    let head_dl = cfg.dl["head"].clone();
    let tail_dl = cfg.dl["tail"].clone();
    let mount_point = opt.mount_point;

    let mut join_handles = Vec::with_capacity(1 + cfg.drive.len());
    let drive_caches = Cache::new(
        &cfg.cache_path,
        &cfg.soft_cache,
        head_dl,
        tail_dl,
        cfg.drive,
    )?;

    join_handles.push(tokio::spawn(mount::mount_thread_eh(
        drive_caches.clone(),
        mount_point,
    )));

    for drive in drive_caches {
        join_handles.push(tokio::spawn(scanner::scan_thread_eh(drive)));
    }

    join_all(join_handles).await;

    Ok(())
}
