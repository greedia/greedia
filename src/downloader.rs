use crate::{downloader_inner::downloader_thread, types::Page};
use anyhow::{format_err, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use oauth2::AccessToken;
use std::path::PathBuf;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum ToDownload {
    Start(u64),
    Range(u64, u64),
    End(u64),
}

#[derive(Debug, PartialEq)]
pub enum ReturnWhen {
    Immediately,
    Started,
    Finished,
}

#[derive(Debug)]
pub enum ChanMessage {
    InvalidAccessToken(AccessToken),
    ScanPage {
        drive_id: String,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
        result: mpsc::Sender<Page>,
    },
    CacheFile {
        file_id: String,
        path: PathBuf,
        range: ToDownload,
        return_when: ReturnWhen,
        result: mpsc::Sender<()>,
    },
    // TODO: changes getstartpagetoken
    // returns start token
    // This should be called before scanning begins
    StartChanges {
        result: mpsc::Sender<String>,
    },
    // TODO: changes list
    // TODO: handle multiple pages
    // This should be called after scanning is done, and again every 5 seconds
    ContinueChanges {
        page_token: String,
        result: mpsc::Sender<()>,
    },
}

pub enum ScanPageResult {
    Ok(Page),
    InvalidAccessToken,
    RateLimit,
}

// TODO: handle key refreshing
#[derive(PartialEq)]
pub enum CacheFileResult {
    Ok,
    AlreadyCached,
    InvalidAccessToken,
    NotFound,
    RateLimit,
    RangeNotSatisfiable,
    DownloadError,
}

#[derive(Clone, Debug)]
pub struct Downloader {
    hp_send_chan: mpsc::Sender<ChanMessage>,
    send_chan: mpsc::Sender<ChanMessage>,
}

impl Downloader {
    pub async fn new(
        client_id: String,
        client_secret: String,
        refresh_token: String,
    ) -> Result<Downloader> {
        let (hp_send_chan, hp_req_chan) = mpsc::channel(1);
        let (send_chan, req_chan) = mpsc::channel(1);

        tokio::spawn(downloader_thread(
            client_id,
            client_secret,
            refresh_token,
            req_chan,
            hp_req_chan,
        ));

        Ok(Downloader {
            hp_send_chan,
            send_chan,
        })
    }

    pub async fn scan_one_page(
        &self,
        drive_id: String,
        last_page_token: Option<String>,
        last_modified_date: Option<DateTime<Utc>>,
    ) -> Result<Page> {
        let (result, mut res_receiver) = mpsc::channel(1);

        let msg = ChanMessage::ScanPage {
            drive_id,
            last_page_token,
            last_modified_date,
            result,
        };

        self.send_chan.clone().send(msg).await?;

        Ok(res_receiver
            .next()
            .await
            .ok_or_else(|| format_err!("scan_one_page recv channel closed without data"))?)
    }

    pub async fn cache_data(
        &self,
        file_id: String,
        path: PathBuf,
        return_when: ReturnWhen,
        range: ToDownload,
    ) -> Result<()> {
        let (result, mut res_receiver) = mpsc::channel(1);

        let msg = ChanMessage::CacheFile {
            file_id,
            path,
            range,
            return_when,
            result,
        };

        self.send_chan.clone().send(msg).await?;

        Ok(res_receiver
            .next()
            .await
            .ok_or_else(|| format_err!("cache_data recv channel closed without data"))?)
    }
}
