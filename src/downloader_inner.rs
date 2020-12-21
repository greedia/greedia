use crate::{
    downloader::{CacheFileResult, ChanMessage, ReturnWhen, ToDownload},
    types::Page,
};
use chrono::{DateTime, Utc};
use futures::select_biased;
use futures::FutureExt;
use futures::StreamExt;
use leaky_bucket::LeakyBucket;
use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use oauth2::AsyncRefreshTokenRequest;
use oauth2::{AccessToken, AuthUrl, ClientId, ClientSecret, RefreshToken, TokenResponse, TokenUrl};
use std::{cmp::max, path::Path, time::Duration};
use tokio::fs::create_dir_all;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

async fn get_new_token(
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Option<AccessToken> {
    let auth_url = AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string())
        .expect("Invalid authorization endpoint URL");
    let token_url = TokenUrl::new("https://www.googleapis.com/oauth2/v3/token".to_string())
        .expect("Invalid token endpoint URL");

    // Set up the config for the Google OAuth2 process.
    let client = BasicClient::new(
        ClientId::new(client_id.to_string()),
        Some(ClientSecret::new(client_secret.to_string())),
        auth_url,
        Some(token_url),
    );

    let refresh_token = RefreshToken::new(refresh_token.to_string());

    let rtr = client.exchange_refresh_token(&refresh_token);
    let access_key = rtr.request_async(async_http_client).await.ok()?;

    Some(access_key.access_token().clone())
}

async fn handle_scan_page(
    http_client: reqwest::Client,
    access_token: &AccessToken,
    drive_id: &str,
    last_page_token: &Option<String>,
    last_modified_date: &Option<DateTime<Utc>>,
) -> Page {
    let mut query = "trashed = false".to_string();

    if last_page_token.is_none() {
        if let Some(last_modified_date) = last_modified_date {
            query.push_str(&format!(
                " and modifiedTime > '{}'",
                last_modified_date.to_rfc3339()
            ));
        }
    }

    let mut query = vec![
        ("alt", "json".to_string()),
        ("includeItemsFromAllDrives", "true".to_string()),
        ("prettyPrint", "false".to_string()),
        ("supportsAllDrives", "true".to_string()),
        ("pageSize", "1000".to_string()),
        ("driveId", drive_id.to_string()),
        ("corpora", "drive".to_string()),
        ("q", query.to_string()),
        (
            "fields",
            "files(id,name,parents,modifiedTime,md5Checksum,size,mimeType),nextPageToken"
                .to_string(),
        ),
    ];

    if let Some(last_page_token) = last_page_token {
        query.push(("pageToken", last_page_token.to_string()));
    }

    let res = http_client
        .get("https://www.googleapis.com/drive/v3/files")
        .bearer_auth(access_token.secret())
        .query(&query)
        .send()
        .await
        .unwrap()
        .json::<Page>()
        .await
        .unwrap();

    res
}

async fn handle_cache_file(
    http_client: reqwest::Client,
    access_token: &AccessToken,
    file_id: &str,
    path: &Path,
    range: &ToDownload,
    return_when: &ReturnWhen,
    mut result: mpsc::Sender<()>,
) -> CacheFileResult {
    let path_exists = path.exists();

    if return_when == &ReturnWhen::Immediately {
        let _ = result.send(()).await;
    }

    let requested_size = match range {
        ToDownload::Start(x) => *x,
        ToDownload::Range(x, y) => *y - *x,
        ToDownload::End(x) => *x,
    };

    if path_exists {
        let path_size = path.metadata().unwrap().len();
        if path_size >= requested_size {
            if return_when != &ReturnWhen::Immediately {
                result.send(()).await.unwrap();
            }
            return CacheFileResult::AlreadyCached;
        }
    } else {
        if requested_size == 0 {
            if return_when != &ReturnWhen::Immediately {
                result.send(()).await.unwrap();
            }
            create_dir_all(path.parent().unwrap()).await.unwrap();
            File::create(path).await.unwrap();
            return CacheFileResult::AlreadyCached;
        }
    }

    let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);

    let range_string = match range {
        ToDownload::Start(x) => format!("bytes=0-{}", x - 1),
        ToDownload::Range(x, y) => format!("bytes={}-{}", x, y - 1),
        ToDownload::End(x) => format!("bytes=-{}", x),
    };

    let res = http_client
        .get(&url)
        .bearer_auth(access_token.secret())
        .header("Range", &range_string)
        .query(&[("alt", "media")])
        .send()
        .await;

    match res {
        Ok(mut res) => {
            match res.status().as_u16() {
                401 => {
                    //println!("Invalid access token");
                    return CacheFileResult::InvalidAccessToken;
                }
                403 | 500 => {
                    //println!("Rate limit");
                    return CacheFileResult::RateLimit;
                }
                404 => {
                    println!("Not found");
                    return CacheFileResult::NotFound;
                }
                200 | 206 => {
                    //println!("Success!");
                }
                416 => {
                    println!("Range not satisfiable {}", res.status());
                    dbg!(&range_string);
                    dbg!(res);
                    return CacheFileResult::RangeNotSatisfiable;
                }
                _ => {
                    println!("Unknown status {}", res.status());
                    dbg!(&range_string);
                    dbg!(res);
                    return CacheFileResult::RateLimit;
                }
            }

            create_dir_all(path.parent().unwrap()).await.unwrap();
            let f_out = File::create(path).await.unwrap();
            let mut f_out = tokio::io::BufWriter::new(f_out);

            if return_when == &ReturnWhen::Started {
                result.send(()).await.unwrap();
            }

            while let Some(chunk) = res.chunk().await.unwrap() {
                f_out.write(&chunk).await.unwrap();
            }

            f_out.flush().await.unwrap();

            if return_when == &ReturnWhen::Finished {
                result.send(()).await.unwrap();
            }

            CacheFileResult::Ok
        }
        Err(err) => {
            println!("Download error! {:?}", err.status());
            println!("{}", err);
            println!("{:?}", err);
            CacheFileResult::DownloadError
        }
    }
}

/// Download from one point in a file, continuously.
/// Returns a handle that can directly read or background-cache data.
/// Note: pos is assumed to be the beginning of the file at `path`.
async fn handle_continuable_cache_file(
    http_client: reqwest::Client,
    access_token: &AccessToken,
    file_id: &str,
    path: &Path,
    pos: u64,
    send_chan: &mut mpsc::Sender<ChanMessage>,
    result: &mut mpsc::Sender<CacheHandle>,
) -> CacheFileResult {
    // If cache file exists, open and read to end. Ignore caching up to this point.
    // Once end is reached (or file does not exist), open downloader and perform reads/caches from it.

    let (reader, writer) = if path.exists() {
        let file = File::open(path).await.unwrap();
        let data_left = path.metadata().unwrap().len();
        let reader = CacheHandleReader::File { file, data_left };
        let writer = OpenOptions::new().append(true).open(path).await.unwrap();

        (reader, writer)
    } else {
        let downloader = match open_downloader(http_client, access_token, file_id, pos).await {
            Ok(downloader) => downloader,
            Err(cache_file_result) => return cache_file_result,
        };

        let reader = CacheHandleReader::Downloader(downloader);
        let writer = File::create(path).await.unwrap();
        (reader, writer)
    };

    let cache_handle = CacheHandle {
        pos,
        file_id: file_id.to_string(),
        send_chan: send_chan.clone(),
        reader,
        writer,
    };

    result.send(cache_handle).await.unwrap();

    CacheFileResult::Ok
}

async fn handle_continuable_cache_file_downloader(
    http_client: reqwest::Client,
    access_token: &AccessToken,
    file_id: &str,
    start_offset: u64,
    result: &mut mpsc::Sender<reqwest::Response>,
) -> CacheFileResult {
    let downloader = match open_downloader(http_client, access_token, file_id, start_offset).await {
        Ok(downloader) => downloader,
        Err(cache_file_result) => return cache_file_result,
    };

    result.send(downloader).await.unwrap();

    CacheFileResult::Ok
}

async fn open_downloader(
    http_client: reqwest::Client,
    access_token: &AccessToken,
    file_id: &str,
    start_offset: u64,
) -> Result<reqwest::Response, CacheFileResult> {
    let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
    let range_string = format!("bytes={}-", start_offset);

    let res = http_client
        .get(&url)
        .bearer_auth(access_token.secret())
        .header("Range", &range_string)
        .query(&[("alt", "media")])
        .send()
        .await;

    match res {
        Ok(res) => {
            match res.status().as_u16() {
                401 => {
                    //println!("Invalid access token");
                    return Err(CacheFileResult::InvalidAccessToken);
                }
                403 | 500 => {
                    //println!("Rate limit");
                    return Err(CacheFileResult::RateLimit);
                }
                404 => {
                    println!("Not found");
                    return Err(CacheFileResult::NotFound);
                }
                200 | 206 => {
                    //println!("Success!");
                }
                416 => {
                    println!("Range not satisfiable {}", res.status());
                    dbg!(&range_string);
                    dbg!(res);
                    return Err(CacheFileResult::RangeNotSatisfiable);
                }
                _ => {
                    println!("Unknown status {}", res.status());
                    dbg!(&range_string);
                    dbg!(res);
                    return Err(CacheFileResult::RateLimit);
                }
            }
            Ok(res)
        }
        Err(err) => {
            println!("Download error! {:?}", err.status());
            println!("{}", err);
            println!("{:?}", err);
            Err(CacheFileResult::DownloadError)
        }
    }
}

#[derive(Debug)]
enum CacheHandleReader {
    File { file: File, data_left: u64 },
    Downloader(reqwest::Response),
}

#[derive(Debug)]
pub struct CacheHandle {
    pos: u64,
    file_id: String,
    send_chan: mpsc::Sender<ChanMessage>,
    reader: CacheHandleReader,
    writer: File,
}

impl CacheHandle {
    // Read exact amount of data, unless EOF is reached.
    pub async fn read_data(&mut self, size: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(size as usize);
        out.resize(size as usize, 0);

        let mut out_data_offset = 0usize;
        let mut out_data_left = size as usize;
        loop {
            if out_data_left == 0 {
                // TODO: also cache/save data
                // TODO: update pos
                return out;
            }
            let new_dl = match &mut self.reader {
                CacheHandleReader::File { file, data_left } => {
                    let amount_read = file
                        .read(
                            &mut out[out_data_offset
                                ..max(out_data_left, out_data_offset + out_data_left)],
                        )
                        .await
                        .unwrap();
                    *data_left -= amount_read as u64;
                    self.pos += amount_read as u64;
                    out_data_left -= amount_read;
                    out_data_offset += amount_read;
                    if *data_left == 0 && out_data_left != 0 {
                        // Get a new downloader
                        let (result, mut res_receiver) = mpsc::channel(1);
                        let msg = ChanMessage::ContinuableCacheFileDownloader {
                            file_id: self.file_id.to_string(),
                            start_offset: self.pos,
                            result,
                        };

                        self.send_chan.clone().send(msg).await.unwrap();
                        Some(res_receiver.next().await.unwrap())
                    } else {
                        None
                    }
                }
                CacheHandleReader::Downloader(dl) => todo!(),
            };
            if let Some(new_dl) = new_dl {
                self.reader = CacheHandleReader::Downloader(new_dl);
            }
        }
    }

    // Read exact amount of data and save it, but don't return it.
    pub async fn cache_data(&mut self, size: u64) {
        todo!()
    }
}

async fn handle_message(
    http_client: reqwest::Client,
    access_token: AccessToken,
    access_token_sender: mpsc::UnboundedSender<ChanMessage>,
    retry_sender: mpsc::UnboundedSender<ChanMessage>,
    mut message: ChanMessage,
) {
    let mut retry = false;
    match &mut message {
        ChanMessage::ScanPage {
            drive_id,
            last_page_token,
            last_modified_date,
            result,
        } => {
            result
                .send(
                    handle_scan_page(
                        http_client,
                        &access_token,
                        drive_id,
                        last_page_token,
                        last_modified_date,
                    )
                    .await,
                )
                .await
                .unwrap();
        }
        ChanMessage::CacheFile {
            file_id,
            path,
            range,
            return_when,
            result,
        } => {
            let result = handle_cache_file(
                http_client,
                &access_token,
                file_id,
                path,
                range,
                return_when,
                result.clone(),
            )
            .await;
            match result {
                CacheFileResult::InvalidAccessToken => {
                    access_token_sender
                        .send(ChanMessage::InvalidAccessToken(access_token.clone()))
                        .unwrap();
                    retry = true;
                }
                CacheFileResult::RateLimit | CacheFileResult::DownloadError => {
                    retry = true;
                }
                CacheFileResult::NotFound => {
                    println!("Notfound, TODO delete?");
                }
                _ => (),
            }
        }
        ChanMessage::ContinuableCacheFile {
            file_id,
            path,
            start_offset,
            send_chan,
            result,
        } => {
            let result = handle_continuable_cache_file(
                http_client,
                &access_token,
                file_id,
                path,
                *start_offset,
                send_chan,
                result,
            )
            .await;
            match result {
                CacheFileResult::InvalidAccessToken => {
                    access_token_sender
                        .send(ChanMessage::InvalidAccessToken(access_token.clone()))
                        .unwrap();
                    retry = true;
                }
                CacheFileResult::RateLimit | CacheFileResult::DownloadError => {
                    retry = true;
                }
                CacheFileResult::NotFound => {
                    println!("Notfound, TODO delete?");
                }
                _ => (),
            }
        }
        ChanMessage::ContinuableCacheFileDownloader {
            file_id,
            start_offset,
            result,
        } => {
            let result = handle_continuable_cache_file_downloader(
                http_client,
                &access_token,
                file_id,
                *start_offset,
                result,
            )
            .await;

            match result {
                CacheFileResult::InvalidAccessToken => {
                    access_token_sender
                        .send(ChanMessage::InvalidAccessToken(access_token.clone()))
                        .unwrap();
                    retry = true;
                }
                CacheFileResult::RateLimit | CacheFileResult::DownloadError => {
                    retry = true;
                }
                CacheFileResult::NotFound => {
                    println!("Notfound, TODO delete?");
                }
                _ => (),
            }
        }
        ChanMessage::InvalidAccessToken(_) => unreachable!(),
    }
    if retry {
        //println!("Retrying");
        retry_sender.send(message).unwrap();
    }
}

pub async fn downloader_thread(
    client_id: String,
    client_secret: String,
    refresh_token: String,
    mut req_chan: mpsc::Receiver<ChanMessage>,
    mut hp_req_chan: mpsc::Receiver<ChanMessage>,
) {
    let http_client = reqwest::Client::builder().referer(false).build().unwrap();

    let (access_token_sender, mut access_token_chan) = mpsc::unbounded_channel::<ChanMessage>();
    let (retry_sender, mut retry_chan) = mpsc::unbounded_channel::<ChanMessage>();
    let (hp_retry_sender, mut hp_retry_chan) = mpsc::unbounded_channel::<ChanMessage>();

    let leaky_bucket = LeakyBucket::builder()
        .max(10)
        .tokens(1)
        .refill_amount(1)
        .refill_interval(Duration::from_millis(120))
        .build()
        .unwrap();
    let access_token = get_new_token(&client_id, &client_secret, &refresh_token)
        .await
        .clone();

    if access_token.is_none() {
        println!("Bad refresh token for {}", &client_id);
    }

    let mut access_token = if let Some(access_token) = access_token {
        access_token
    } else {
        panic!("Bad refresh token for {}", &client_id);
    };

    loop {
        let _ = leaky_bucket
            .acquire_one()
            .await
            .expect("Could not acquire rate-limiting token");

        // If a high-priority message comes in, always pick it first
        let (is_hp, message) = select_biased! {
            x = access_token_chan.next().fuse() => (true, x),
            x = hp_retry_chan.next().fuse() => (true, x),
            x = hp_req_chan.next().fuse() => (true, x),
            x = retry_chan.next().fuse() => (false, x),
            x = req_chan.next().fuse() => (false, x),
        };

        if let Some(ChanMessage::InvalidAccessToken(invalid_access_token)) = message {
            if access_token.secret() == invalid_access_token.secret() {
                let new_access_token =
                    get_new_token(&client_id, &client_secret, &refresh_token).await;
                if let Some(nat) = new_access_token {
                    access_token = nat;
                }
            }
        } else if let Some(message) = message {
            let http_client = http_client.clone();
            let access_token = access_token.clone();
            let access_token_sender = access_token_sender.clone();
            let retry_sender = if is_hp {
                hp_retry_sender.clone()
            } else {
                retry_sender.clone()
            };

            tokio::task::spawn(async {
                handle_message(
                    http_client,
                    access_token,
                    access_token_sender,
                    retry_sender,
                    message,
                )
                .await
            });
        } else {
            println!("Closing downloader thread for {}", client_id);
            break;
        }
    }
}
