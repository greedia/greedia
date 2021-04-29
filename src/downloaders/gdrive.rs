use super::{Change, DownloaderClient, DownloaderDrive, DownloaderError, FileInfo, Page, PageItem};
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
use std::{borrow::Cow, sync::Arc, time::Duration};
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
        let rate_limiter = Arc::new(PrioLimit::new(1, 5, Duration::from_millis(100)));

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
    root_query: Vec<(&'static str, Cow<'static, str>)>,
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
        if let Some(page_token) = state.last_page_token.as_deref() {
            query.push(("pageToken", Cow::Borrowed(page_token)));
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
                        .map(to_page_item)
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

pub struct GDriveWatchState {
    access_token_mutex: Arc<Mutex<AccessToken>>,
    rate_limiter: Arc<PrioLimit>,
    http_client: Client,
    conn_info: ConnInfo,
    drive_id: String,
    access_token: Option<AccessToken>,
    last_page_token: Option<String>,
}

async fn watcher_bg_thread(
    mut state: GDriveWatchState,
    sender: Sender<Result<Vec<Change>, DownloaderError>>,
) {
    if state.access_token.is_none() {
        state.access_token = Some(state.access_token_mutex.lock().await.clone());
    }

    let mut access_token = state.access_token.unwrap();

    state.rate_limiter.bg_wait().await;

    let mut start_page_token: Option<String> = None;

    let root_query = vec![
        ("alt", Cow::Borrowed("json")),
        ("includeItemsFromAllDrives", Cow::Borrowed("true")),
        ("prettyPrint", Cow::Borrowed("false")),
        ("supportsAllDrives", Cow::Borrowed("true")),
        ("pageSize", Cow::Borrowed("1000")),
        ("driveId", Cow::Borrowed(state.drive_id.as_str())),
        ("restrictToMyDrive", Cow::Borrowed("true")),
        ("includeRemoved", Cow::Borrowed("true")),
        ("corpora", Cow::Borrowed("drive")),
        (
            "fields",
            Cow::Borrowed("changes(changeType,removed,file(id,name,parents,modifiedTime,md5Checksum,size,mimeType, trashed)),nextPageToken,newStartPageToken"),
        ),
    ];

    loop {
        let res = if let Some(start_page_token) = &start_page_token {
            // If we wanted to get extra fancy, we could get a 10-second deadline, bg_wait,
            // then sleep for whatever time's left. This is probably good enough for now, though.
            state.rate_limiter.bg_wait().await;
            tokio::time::sleep(Duration::from_secs(10)).await;

            // If we have a last_page_token, use that
            // Otherwise use the start_page_token
            let page_token = if let Some(last_page_token) = state.last_page_token.as_deref() {
                last_page_token
            } else {
                start_page_token.as_str()
            };

            let mut query = root_query.clone();
            query.push(("pageToken", Cow::Borrowed(page_token)));

            state
                .http_client
                .get("https://www.googleapis.com/drive/v3/changes")
                .bearer_auth(access_token.secret())
                .query(&query)
                .send()
                .await
        } else {
            // getStartPageToken
            let init_query = vec![
                ("alt", Cow::Borrowed("json")),
                ("prettyPrint", Cow::Borrowed("true")),
                ("driveId", Cow::Borrowed(state.drive_id.as_str())),
                ("supportsAllDrives", Cow::Borrowed("true")),
            ];

            state
                .http_client
                .get("https://www.googleapis.com/drive/v3/changes/startPageToken")
                .bearer_auth(access_token.secret())
                .query(&init_query)
                .send()
                .await
        };

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

                if let Some(start_page_token) = &mut start_page_token {
                    let changes = r.json::<GChange>().await.unwrap();

                    if let Some(next_page_token) = changes.next_page_token {
                        state.last_page_token = Some(next_page_token);
                    }

                    if let Some(new_start_page_token) = changes.new_start_page_token {
                        *start_page_token = new_start_page_token;
                    }

                    let items: Vec<_> = changes
                        .changes
                        .into_iter()
                        .filter_map(to_change_item)
                        .collect();

                    if !items.is_empty() {
                        if sender.send(Ok(items)).await.is_err() {
                            break;
                        }
                    }
                } else {
                    let change_start = r.json::<GChangeStart>().await.unwrap();
                    start_page_token = Some(change_start.start_page_token.clone());
                };
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
            ("alt", Cow::Borrowed("json")),
            ("includeItemsFromAllDrives", Cow::Borrowed("true")),
            ("prettyPrint", Cow::Borrowed("false")),
            ("supportsAllDrives", Cow::Borrowed("true")),
            ("pageSize", Cow::Borrowed("1000")),
            ("driveId", Cow::Owned(drive_id)),
            ("corpora", Cow::Borrowed("drive")),
            ("q", Cow::Owned(query)),
            (
                "fields",
                Cow::Borrowed(
                    "files(id,name,parents,modifiedTime,md5Checksum,size,mimeType),nextPageToken",
                ),
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

    fn watch_changes(
        &self,
    ) -> Box<dyn Stream<Item = Result<Vec<Change>, DownloaderError>> + Send + Sync + Unpin> {
        let drive_id = self.drive_id.to_string();
        let access_token_mutex = self.access_token.clone();
        let rate_limiter = self.rate_limiter.clone();
        let http_client = self.http_client.clone();
        let conn_info = self.conn_info.clone();

        let state = GDriveWatchState {
            access_token_mutex,
            rate_limiter,
            http_client,
            conn_info,
            drive_id,
            access_token: None,
            last_page_token: None,
        };

        let (sender, recv) = mpsc::channel(1);
        tokio::spawn(watcher_bg_thread(state, sender));
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

    fn get_downloader_type(&self) -> &'static str {
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

    for _ in 0..10 {
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
                        println!("access token needs refresh");
                        access_token = refresh_access_token(
                            access_token_mutex,
                            rate_limiter,
                            &conn_info,
                            access_token,
                        )
                        .await;
                    }
                    416 => {
                        return Err(DownloaderError::RangeNotSatisfiable(range_string));
                    }
                    403 => {
                        if let Ok(error_json) = r.json::<GErrorTop>().await {
                            let errors = error_json.error.errors;
                            for error in errors.iter() {
                                if error.reason == "downloadQuotaExceeded" {
                                    return Err(DownloaderError::QuotaExceeded)
                                }
                            }
                        }
                    }
                    // Rate limit or server error, retry request
                    _ => {
                        println!("Other: {}", r.status());
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
                    }
                }
            }
            Err(e) => {
                println!("Error: {}", e)
            }
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

fn to_change_item(change: GChangeItem) -> Option<Change> {
    // dbg!(&change);

    if let Some(file) = change.file {
        if file.trashed || change.removed {
            Some(Change::Removed(file.id))
        } else {
            Some(Change::Added(PageItem {
                id: file.id,
                name: file.name,
                parent: file.parents[0].clone(),
                modified_time: file.modified_time.parse().ok()?,
                file_info: if file.mime_type.as_deref()
                    == Some("application/vnd.google-apps.folder")
                {
                    None
                } else {
                    let md5 = hex::decode(file.md5_checksum?).ok()?;
                    Some(FileInfo {
                        data_id: DataIdentifier::GlobalMd5(md5),
                        size: file.size?.parse().ok()?,
                    })
                },
            }))
        }
    } else {
        None
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GChangeStart {
    pub start_page_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GChange {
    pub changes: Vec<GChangeItem>,
    pub next_page_token: Option<String>,
    pub new_start_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GChangeItem {
    change_type: String,
    removed: bool,
    file: Option<GChangeFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GChangeFile {
    pub id: String,
    pub name: String,
    pub parents: Vec<String>,
    pub modified_time: String,
    pub md5_checksum: Option<String>,
    pub size: Option<String>,
    pub mime_type: Option<String>,
    pub trashed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GErrorTop {
    error: GErrorInner
}

#[derive(Debug, Deserialize)]
pub struct GErrorInner {
    errors: Vec<GError>,
    code: u64,
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct GError {
    domain: String,
    reason: String,
    message: String,
}

#[cfg(test)]
mod test {
    use std::env;

    use super::{DownloaderClient, GDriveClient};

    use futures::StreamExt;
    #[tokio::test]
    async fn do_gdrive_stuff() {
        let client_id = env::var("TEST_CLIENT_ID").unwrap();
        let client_secret = env::var("TEST_CLIENT_SECRET").unwrap();
        let refresh_token = env::var("TEST_REFRESH_TOKEN").unwrap();
        let drive_id = env::var("TEST_DRIVE_ID").unwrap();

        let c = GDriveClient::new(&client_id, &client_secret, &refresh_token)
            .await
            .unwrap();

        let d = c.open_drive(&drive_id);

        let mut changes = d.watch_changes();

        while let Some(Ok(change_list)) = changes.next().await {
            dbg!(&change_list);
        }
    }
}
