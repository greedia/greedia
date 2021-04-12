use super::{DownloaderClient, DownloaderDrive, DownloaderError, FileInfo, Page, PageItem};
use crate::{prio_limit::PrioLimit, types::DataIdentifier};
use anyhow::{format_err, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt};
use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AccessToken, AuthUrl, ClientId, ClientSecret,
    RefreshToken, TokenResponse, TokenUrl,
};
use reqwest::{self, Client};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex,
};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone, Debug)]
struct ConnInfo {
    client_id: ClientId,
    client_secret: ClientSecret,
    refresh_token: RefreshToken,
}

#[derive(Debug)]
pub struct GDriveClient {
    http_client: reqwest::Client,
    rate_limiter: Arc<PrioLimit>,
    access_token: Arc<Mutex<AccessToken>>,
    conn_info: ConnInfo,
}

impl GDriveClient {
    pub async fn new(
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
    ) -> Result<GDriveClient> {
        let http_client = reqwest::Client::builder().referer(false).build().unwrap();
        let rate_limiter = Arc::new(PrioLimit::new(1, 10, Duration::from_millis(120)));

        let client_id = ClientId::new(client_id.to_owned());
        let client_secret = ClientSecret::new(client_secret.to_owned());
        let refresh_token = RefreshToken::new(refresh_token.to_owned());

        let conn_info = ConnInfo {
            client_id,
            client_secret,
            refresh_token,
        };

        let initial_access_token =
            get_new_token(&conn_info, &rate_limiter)
                .await
                .ok_or_else(|| {
                    format_err!(
                        "Could not initiate access token for gdrive client {}",
                        conn_info.client_id.as_str(),
                    )
                })?;
        let access_token = Arc::new(Mutex::new(initial_access_token));
        Ok(GDriveClient {
            http_client,
            rate_limiter,
            access_token,
            conn_info,
        })
    }
}

impl DownloaderClient for GDriveClient {
    fn open_drive(&self, drive_id: &str) -> Box<dyn DownloaderDrive> {
        Box::new(GDriveDrive {
            http_client: self.http_client.clone(),
            rate_limiter: self.rate_limiter.clone(),
            access_token: self.access_token.clone(),
            conn_info: self.conn_info.clone(),
            drive_id: drive_id.to_owned(),
        })
    }
}

pub struct GDriveDrive {
    http_client: reqwest::Client,
    rate_limiter: Arc<PrioLimit>,
    access_token: Arc<Mutex<AccessToken>>,
    conn_info: ConnInfo,
    drive_id: String,
}

pub struct GDriveStreamState {
    access_token_mutex: Arc<Mutex<AccessToken>>,
    root_query: Vec<(&'static str, String)>,
    rate_limiter: Arc<PrioLimit>,
    http_client: Client,
    conn_info: ConnInfo,
    access_token: Option<AccessToken>,
    last_page_token: Option<String>,
}

/// Background task for sending new pages to a scan_pages Stream.
/// This is necessary because hyper Response objects do not implement Sync.
/// Therefore we need to use a channel, rather than async_stream or Stream::unfold.
async fn scanner_bg_thread(
    mut state: GDriveStreamState,
    sender: Sender<Result<Page, DownloaderError>>,
) {
    if state.access_token.is_none() {
        state.access_token = Some(state.access_token_mutex.lock().await.clone());
    }

    let mut access_token = state.access_token.unwrap();

    state.rate_limiter.bg_wait().await;

    loop {

        let mut query = state.root_query.clone();
        if let Some(page_token) = &state.last_page_token {
            query.push(("pageToken", page_token.to_string()));
        }

        let res = state
            .http_client
            .get("https://www.googleapis.com/drive/v3/files")
            .bearer_auth(access_token.secret())
            .query(&query)
            .send()
            .await;

        match res {
            Ok(r) => {
                match r.status().as_u16() {
                    // Everything's good
                    200 => (),
                    // Bad access token, refresh it and retry request
                    401 => {
                        access_token = refresh_access_token(
                            &state.access_token_mutex,
                            &state.rate_limiter,
                            &state.conn_info,
                            access_token,
                        )
                        .await;
                        continue;
                    }
                    // Rate limit or server error, retry request
                    _ => continue,
                }

                let page = r.json::<GPage>().await.unwrap();
                state.last_page_token = page.next_page_token;
                if let Some(files) = page.files {
                    let items = files
                        .into_iter()
                        .filter(|x| accepted_document_type(x))
                        .map(|x| to_page_item(x))
                        .collect();

                    let _ = sender
                        .send(Ok(Page {
                            items,
                            next_page_token: state.last_page_token.clone(),
                        }))
                        .await;
                }

                if state.last_page_token.is_none() {
                    // This is the final page.
                    break;
                }
            }
            Err(_) => {
                // Something bad happened, but continue anyways.
                continue;
            }
        }
    }
}

#[async_trait]
impl DownloaderDrive for GDriveDrive {
    fn scan_pages(
        &self,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Box<dyn Stream<Item = Result<Page, DownloaderError>> + Send + Sync + Unpin> {
        let drive_id = self.drive_id.to_string();
        let access_token_mutex = self.access_token.clone();
        let rate_limiter = self.rate_limiter.clone();
        let http_client = self.http_client.clone();
        let conn_info = self.conn_info.clone();

        let mut query = "trashed = false".to_string();

        if last_page_token.is_none() {
            if let Some(last_modified_date) = last_modified_date {
                query.push_str(&format!(
                    " and modifiedTime > '{}'",
                    last_modified_date.to_rfc3339()
                ));
            }
        }

        let root_query = vec![
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

        let state = GDriveStreamState {
            access_token_mutex,
            root_query,
            rate_limiter,
            http_client,
            conn_info,
            access_token: None,
            last_page_token: last_page_token.clone(),
        };

        let (sender, recv) = mpsc::channel(1);
        tokio::spawn(scanner_bg_thread(state, sender));
        Box::new(ReceiverStream::new(recv))
    }

    async fn open_file(
        &self,
        file_id: String,
        offset: u64,
        bg_request: bool,
    ) -> Result<
        Box<dyn Stream<Item = Result<Bytes, DownloaderError>> + Unpin + Send + Sync>,
        DownloaderError,
    > {
        let res = open_request(
            file_id,
            offset,
            bg_request,
            &self.http_client,
            &self.access_token,
            &self.rate_limiter,
            &self.conn_info,
        )
        .await?;

        let stream = res.bytes_stream().map_err(|e| e.into());
        Ok(Box::new(stream))
    }

    fn get_drive_type(&self) -> &'static str {
        "gdrive"
    }
}

async fn open_request(
    file_id: String,
    offset: u64,
    bg_request: bool,
    http_client: &reqwest::Client,
    access_token_mutex: &Arc<Mutex<AccessToken>>,
    rate_limiter: &Arc<PrioLimit>,
    conn_info: &ConnInfo,
) -> Result<reqwest::Response, DownloaderError> {
    let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
    let range_string = format!("bytes={}-", offset);

    let mut access_token = access_token_mutex.lock().await.clone();
    let mut retry = false;

    for _ in 0..5 {
        if bg_request {
            if retry {
                rate_limiter.bg_retry_wait().await;
            } else {
                rate_limiter.bg_wait().await;
            }
        } else {
            if retry {
                rate_limiter.retry_wait().await;
            } else {
                rate_limiter.wait().await;
            }
        }

        let res = http_client
            .get(&url)
            .bearer_auth(access_token.secret())
            .header("Range", &range_string)
            .query(&[("alt", "media")])
            .send()
            .await;

        match res {
            Ok(r) => {
                match r.status().as_u16() {
                    // Everything's good
                    200 | 206 => {
                        return Ok(r);
                    }
                    // Bad access token, refresh it and retry request
                    401 => {
                        access_token = refresh_access_token(
                            access_token_mutex,
                            rate_limiter,
                            &conn_info,
                            access_token,
                        )
                        .await;
                    }
                    // Rate limit or server error, retry request
                    _ => (),
                }
            }
            Err(_) => {}
        }

        retry = true
    }

    // KTODO: 5 tries failed, return error
    todo!()
}

async fn refresh_access_token(
    access_token_mutex: &Arc<Mutex<AccessToken>>,
    rate_limiter: &Arc<PrioLimit>,
    conn_info: &ConnInfo,
    old_access_token: AccessToken,
) -> AccessToken {
    let mut current_access_token_lock = access_token_mutex.lock().await;
    let current_access_token = current_access_token_lock.clone();
    if old_access_token.secret() != current_access_token.secret() {
        current_access_token
    } else {
        let new_access_token = get_new_token(conn_info, rate_limiter).await.unwrap();
        *current_access_token_lock = new_access_token.clone();
        new_access_token
    }
}

async fn get_new_token(conn_info: &ConnInfo, rate_limiter: &PrioLimit) -> Option<AccessToken> {
    let auth_url = AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string())
        .expect("Invalid authorization endpoint URL");
    let token_url = TokenUrl::new("https://www.googleapis.com/oauth2/v3/token".to_string())
        .expect("Invalid token endpoint URL");

    // Set up the config for the Google OAuth2 process.
    let client = BasicClient::new(
        conn_info.client_id.clone(),
        Some(conn_info.client_secret.clone()),
        auth_url,
        Some(token_url),
    );

    rate_limiter.access_token_wait().await;

    let rtr = client.exchange_refresh_token(&conn_info.refresh_token);
    let access_key = rtr.request_async(async_http_client).await.ok()?;

    Some(access_key.access_token().clone())
}

fn accepted_document_type(page_item: &GPageItem) -> bool {
    // Allow any mime type that doesn't start with vnd.google-apps, unless it's a folder
    match page_item.mime_type {
        Some(ref m) if m == "application/vnd.google-apps.folder" => true,
        Some(ref m) if m.starts_with("application/vnd.google-apps.") => false,
        Some(_) => true,
        None => false,
    }
}

fn to_page_item(file: GPageItem) -> PageItem {
    PageItem {
        id: file.id,
        name: file.name,
        parent: file.parents[0].clone(),
        modified_time: file.modified_time.parse().unwrap(),
        file_info: if file.mime_type.as_deref() == Some("application/vnd.google-apps.folder") {
            None
        } else {
            let md5 = hex::decode(file.md5_checksum.unwrap()).unwrap();
            Some(FileInfo {
                data_id: DataIdentifier::GlobalMd5(md5),
                size: file.size.unwrap().parse().unwrap(),
            })
        },
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GPage {
    pub files: Option<Vec<GPageItem>>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GPageItem {
    pub id: String,
    pub name: String,
    pub parents: Vec<String>,
    pub modified_time: String,
    pub md5_checksum: Option<String>,
    pub size: Option<String>,
    pub mime_type: Option<String>,
}

#[cfg(test)]
mod test {
    use super::{DownloaderClient, GDriveClient};

    use futures::StreamExt;
    #[tokio::test]
    async fn do_stuff() {
        let client_id = "***REMOVED***";
        let client_secret = "***REMOVED***";
        let refresh_token = "***REMOVED***";
        let drive_id = "***REMOVED***";

        let c = GDriveClient::new(client_id, client_secret, refresh_token)
            .await
            .unwrap();

        let d = c.open_drive(drive_id);

        let mut pages = d.scan_pages(None, None);

        let mut items = vec![];
        while let Some(Ok(mut page)) = pages.next().await {
            dbg!(&page);
            items.append(&mut page.items);
        }

        dbg!(&items[1]);

        let mut f = d
            .open_file(items[1].id.to_string(), 0, false)
            .await
            .unwrap();

        if let Some(Ok(a)) = f.next().await {
            dbg!(&a.len());
        };

        if let Some(Ok(a)) = f.next().await {
            dbg!(&a.len());
        };

        if let Some(Ok(a)) = f.next().await {
            dbg!(&a.len());
        };

        /*let buf_out = f.read_bytes(4).await;
        dbg!(&buf_out);

        f.cache(4, &mut cursor).await;

        dbg!(&cursor);*/
    }
}
