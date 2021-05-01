use super::{Change, DownloaderClient, DownloaderDrive, DownloaderError, FileInfo, Page, PageItem};
use crate::{prio_limit::PrioLimit, types::DataIdentifier};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt};
use jwt_simple::prelude::{Claims, Duration as JwtDuration, RS256KeyPair, RSAKeyPairLike};
use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AccessToken, AuthUrl, ClientId, ClientSecret,
    RefreshToken, TokenResponse, TokenUrl,
};
use reqwest::{self, Client};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, path::Path, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration};
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
    access_instances: Arc<AccessInstanceHandler>,
}

#[derive(Debug)]
pub struct AccessInstanceHandler {
    index: AtomicU64,
    access_instances: Vec<AccessInstance>,
}

impl AccessInstanceHandler {
    pub fn new(access_instances: Vec<AccessInstance>) -> AccessInstanceHandler {
        let index = AtomicU64::new(0);
        AccessInstanceHandler {
            index,
            access_instances,
        }
    }

    pub fn next<'a>(&'a self) -> &'a AccessInstance {
        let index = self.index.fetch_add(1, Ordering::AcqRel) % self.access_instances.len() as u64;
        
        let access_instance = self.access_instances.get(index as usize).unwrap();
        println!("access_instance_handler next {}/{}", index, self.access_instances.len());
        access_instance
    }
}

#[derive(Debug)]
pub enum AccessInstance {
    Client(ClientInstance),
    ServiceAccount(ServiceAccountInstance),
}

impl AccessInstance {
    pub async fn access_token(&self) -> AccessToken {
        match self {
            AccessInstance::Client(c) => {
                c.access_token.lock().await.clone()
            }
            AccessInstance::ServiceAccount(sa) => {
                sa.access_token.lock().await.clone()
            }
        }
    }

    pub async fn refresh_access_token(&self, http_client: &Client) -> Result<(), DownloaderError> {
        match self {
            AccessInstance::Client(c) => {
                let access_token = get_new_token(&c.conn_info, &c.rate_limiter).await.ok_or_else(|| DownloaderError::AccessTokenError)?;
                *c.access_token.lock().await = access_token;
            }
            AccessInstance::ServiceAccount(sa) => {
                let access_token = ServiceAccountInstance::get_new_token(
                    http_client,
                    &sa.service_account,
                ).await?;
                *sa.access_token.lock().await = access_token;
            }
        }

        Ok(())
    }

    pub async fn rate_limit(&self, bg_request: bool, retry: bool) {
        let rate_limiter = match self {
            AccessInstance::Client(c) => {
                &c.rate_limiter
            }
            AccessInstance::ServiceAccount(sa) => {
                &sa.rate_limiter
            }
        };

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

#[derive(Debug)]
pub struct ClientInstance {
    rate_limiter: PrioLimit,
    access_token: Mutex<AccessToken>,
    conn_info: ConnInfo,
}

impl ClientInstance {
    pub async fn new(client_id: &str, client_secret: &str, refresh_token: &str) -> Result<ClientInstance, DownloaderError> {
        let rate_limiter = PrioLimit::new(1, 5, Duration::from_millis(100));

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
                    DownloaderError::AccessTokenError
                })?;
        let access_token = Mutex::new(initial_access_token);

        let client_instance = ClientInstance{
            rate_limiter,
            access_token,
            conn_info,
        };

        Ok(client_instance)
    }
}

#[derive(Debug)]
pub struct ServiceAccountInstance {
    rate_limiter: PrioLimit,
    access_token: Mutex<AccessToken>,
    service_account: ServiceAccount,
}

impl ServiceAccountInstance {
    pub async fn new(http_client: &Client, path: &Path) -> Result<ServiceAccountInstance, DownloaderError> {
        let rate_limiter = PrioLimit::new(1, 5, Duration::from_millis(100));

        let sa_bytes = tokio::fs::read(path).await.unwrap();
        let service_account: ServiceAccount = serde_json::from_slice(&sa_bytes).unwrap();

        let initial_access_token = Self::get_new_token(http_client, &service_account).await?;
        let access_token = Mutex::new(initial_access_token);

        Ok(ServiceAccountInstance {
            rate_limiter,
            access_token,
            service_account,
        })
    }



    async fn get_new_token(http_client: &Client, service_account: &ServiceAccount) -> Result<AccessToken, DownloaderError> {
        let key_pair = RS256KeyPair::from_pem(&service_account.private_key).unwrap();
        let scope = "https://www.googleapis.com/auth/drive.readonly";

        let claim_data = SaJwtClaims {
            iss: service_account.client_email.clone(),
            scope: scope.to_string(),
            aud: service_account.token_uri.clone(),
        };
        let claims = Claims::with_custom_claims(claim_data, JwtDuration::from_hours(1));
        let token = key_pair.sign(claims).map_err(|_| DownloaderError::AccessTokenError)?;

        let res = http_client.post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &token),
            ])
            .send()
            .await?;

        let res_data = res.json::<SaJwtResponse>().await?;
        Ok(AccessToken::new(res_data.access_token))
    }
}

impl GDriveClient {
    pub async fn new(
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
        service_account_files: &[&Path],
    ) -> Result<GDriveClient, DownloaderError> {
        let http_client = reqwest::Client::builder().referer(false).build().unwrap();

        // Create the client instance
        let client_instance = ClientInstance::new(client_id, client_secret, refresh_token).await?;

        // Create all service account instances
        let mut access_instances = vec![AccessInstance::Client(client_instance)];
        for sa in service_account_files {
            let service_account_instance = AccessInstance::ServiceAccount(ServiceAccountInstance::new(&http_client, *sa).await?);
            access_instances.push(service_account_instance);
        }

        let access_instances = Arc::new(AccessInstanceHandler::new(access_instances));

        Ok(GDriveClient {
            http_client,
            access_instances,
        })
    }
}

impl DownloaderClient for GDriveClient {
    fn open_drive(&self, drive_id: &str) -> Box<dyn DownloaderDrive> {
        Box::new(GDriveDrive {
            http_client: self.http_client.clone(),
            access_instances: self.access_instances.clone(),
            drive_id: drive_id.to_owned(),
        })
    }
}

pub struct GDriveDrive {
    http_client: reqwest::Client,
    drive_id: String,
    access_instances: Arc<AccessInstanceHandler>,
}

pub struct GDriveStreamState {
    root_query: Vec<(&'static str, Cow<'static, str>)>,
    http_client: Client,
    last_page_token: Option<String>,
    access_instances: Arc<AccessInstanceHandler>,
}

/// Background task for sending new pages to a scan_pages Stream.
/// This is necessary because hyper Response objects do not implement Sync.
/// Therefore we need to use a channel, rather than async_stream or Stream::unfold.
async fn scanner_bg_thread(
    mut state: GDriveStreamState,
    sender: Sender<Result<Page, DownloaderError>>,
) {

    loop {
        let mut query = state.root_query.clone();
        if let Some(page_token) = state.last_page_token.as_deref() {
            query.push(("pageToken", Cow::Borrowed(page_token)));
        }

        let access_instance = state.access_instances.next();
        access_instance.rate_limit(true, false).await;

        let access_token = access_instance.access_token().await;

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
                        access_instance.refresh_access_token(&state.http_client).await.unwrap();
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
    http_client: Client,
    drive_id: String,
    last_page_token: Option<String>,
    access_instances: Arc<AccessInstanceHandler>,
}

async fn watcher_bg_thread(
    mut state: GDriveWatchState,
    sender: Sender<Result<Vec<Change>, DownloaderError>>,
) {
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
        let access_instance = state.access_instances.next();
        access_instance.rate_limit(true, false).await;

        let access_token = access_instance.access_token().await;
        
        let res = if let Some(start_page_token) = &start_page_token {
            // If we wanted to get extra fancy, we could get a 10-second deadline, bg_wait,
            // then sleep for whatever time's left. This is probably good enough for now, though.

            // state.rate_limiter.bg_wait().await;
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
                        access_instance.refresh_access_token(&state.http_client).await.unwrap();
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
            root_query,
            http_client: self.http_client.clone(),
            access_instances: self.access_instances.clone(),
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
        let http_client = self.http_client.clone();

        let state = GDriveWatchState {
            http_client,
            drive_id,
            last_page_token: None,
            access_instances: self.access_instances.clone(),
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
        let access_instance = self.access_instances.next();
        let res = open_request(
            file_id,
            offset,
            bg_request,
            &self.http_client,
            access_instance,
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
    access_instance: &AccessInstance,
) -> Result<reqwest::Response, DownloaderError> {
    let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
    let range_string = format!("bytes={}-", offset);

    let mut retry = false;

    for _ in 0..10 {
        access_instance.rate_limit(bg_request, retry).await;
        let access_token = access_instance.access_token().await;

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
                        access_instance.refresh_access_token(&http_client).await?;
                    }
                    416 => {
                        return Err(DownloaderError::RangeNotSatisfiable(range_string));
                    }
                    403 => {
                        if let Ok(error_json) = r.json::<GErrorTop>().await {
                            let errors = &error_json.error.errors;
                            for error in errors.iter() {
                                if error.reason == "downloadQuotaExceeded" {
                                    println!("{:?}", &error_json);
                                    return Err(DownloaderError::QuotaExceeded)
                                }
                            }
                        }
                    }
                    // Rate limit or server error, retry request
                    _ => {
                        println!("Other: {}", r.status());
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

#[derive(Debug, Deserialize)]
pub struct ServiceAccount {
    project_id: String,
    private_key_id: String,
    private_key: String,
    client_email: String,
    client_id: String,
    auth_uri: String,
    token_uri: String,
    auth_provider_x509_cert_url: String,
    client_x509_cert_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaJwtClaims {
    iss: String,
    scope: String,
    aud: String,
}

#[derive(Debug, Deserialize)]
pub struct SaJwtResponse {
    access_token: String,
    expires_in: u64,
    token_type: String,
}

#[cfg(test)]
mod test {
    use std::env;

    use crate::downloaders::gdrive::{SaJwtClaims, SaJwtResponse, ServiceAccount};

    use super::{DownloaderClient, GDriveClient};

    use futures::StreamExt;
    use jwt_simple::prelude::{Claims, Duration, RS256KeyPair, RSAKeyPairLike};
    #[tokio::test]
    async fn do_gdrive_stuff() {
        let client_id = env::var("TEST_CLIENT_ID").unwrap();
        let client_secret = env::var("TEST_CLIENT_SECRET").unwrap();
        let refresh_token = env::var("TEST_REFRESH_TOKEN").unwrap();
        let drive_id = env::var("TEST_DRIVE_ID").unwrap();

        let c = GDriveClient::new(&client_id, &client_secret, &refresh_token, &[])
            .await
            .unwrap();

        let d = c.open_drive(&drive_id);

        let mut changes = d.watch_changes();

        while let Some(Ok(change_list)) = changes.next().await {
            dbg!(&change_list);
        }
    }

    #[tokio::test]
    async fn test_service_accounts() {
        let sa_path = env::var("TEST_SA_PATH").unwrap();
        let file_id = env::var("TEST_FILE_ID").unwrap();
        let sa_bytes = tokio::fs::read(sa_path).await.unwrap();
        let sa: ServiceAccount = serde_json::from_slice(&sa_bytes).unwrap();
        dbg!(&sa);

        let key_pair = RS256KeyPair::from_pem(&sa.private_key).unwrap();
        let scope = "https://www.googleapis.com/auth/drive.readonly";

        let claim_data = SaJwtClaims {
            iss: sa.client_email,
            scope: scope.to_string(),
            aud: sa.token_uri.clone(),
        };
        let claims = Claims::with_custom_claims(claim_data, Duration::from_hours(1));
        dbg!(&claims);
        let token = key_pair.sign(claims).unwrap();
        println!("token: {}", token);

        let rclient = reqwest::Client::new();
        let res = rclient.post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &token),
            ])
            .send()
            .await
            .unwrap();
        dbg!(&res.status());
        let res_data = res.json::<SaJwtResponse>().await.unwrap();
        dbg!(&res_data);
        
        let url = format!("https://www.googleapis.com/drive/v3/files/{}", file_id);
        let res = rclient
            .get(&url)
            .bearer_auth(res_data.access_token)
            //.header("Range", &range_string)
            .query(&[("alt", "media")])
            .send()
            .await
            .unwrap();

        dbg!(&res.status());
    }
}
