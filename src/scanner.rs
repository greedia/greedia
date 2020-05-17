use crate::{types::{PageItem, Page, ScannedItem, FileInfo}, drive_cache::DriveCache};
use anyhow::Result;
use chrono::{DateTime, Utc};
use itertools::Itertools;


/// Scan thread, with error handling
pub async fn scan_thread_eh(drive_cache: DriveCache) {
    scan_thread(drive_cache).await.unwrap()
}

/// Thread that handles scanning and updating of one defined drive
pub async fn scan_thread(drive_cache: DriveCache) -> Result<()> {
    println!("Scan thread for {}", drive_cache.name);
    let (mut last_page_token, mut last_modified_date) = drive_cache.get_scan_state()?;

    dbg!(&last_page_token, &last_modified_date);

    perform_scan(&drive_cache, &mut last_page_token, &mut last_modified_date).await.unwrap();

    Ok(())
}

pub async fn perform_scan(
    drive_cache: &DriveCache,
    last_page_token: &mut Option<String>,
    last_modified_date: &mut Option<DateTime<Utc>>,
) -> Result<()> {
    drive_cache.start_scan()?;
    println!(
        "Scanning drive {} ({})...",
        &drive_cache.name, &drive_cache.id
    );
    loop {
        let page =
            drive_cache.scan_one_page(last_page_token, last_modified_date).await?;

        handle_one_page(&drive_cache, &page).await;

        if let Some(ref next_page_token) = page.next_page_token {
            drive_cache.set_last_page_token(&next_page_token)?;

            *last_page_token = Some(next_page_token.clone());
        } else {
            break;
        }
    }

    drive_cache.finish_scan()?;
    println!("Finished scanning drive {} ({})", &drive_cache.name, &drive_cache.id);

    let (count, size) = drive_cache.count_access_keys().await?;
    println!("Caching {} items of size {} for drive {} ({})...", count, size, &drive_cache.name, &drive_cache.id);

    Ok(())
}

fn accepted_document_type(page_item: &PageItem) -> bool {
    // Allow any mime type that doesn't start with vnd.google-apps, unless it's a folder
    match page_item.mime_type {
        Some(ref m) if m == "application/vnd.google-apps.folder" => true,
        Some(ref m) if m.starts_with("application/vnd.google-apps.") => false,
        Some(_) => true,
        None => false,
    }
}

fn to_scanned_item(file: &PageItem) -> ScannedItem {
    ScannedItem {
        id: file.id.clone(),
        name: file.name.clone(),
        parent: file.parents[0].clone(),
        modified_time: file.modified_time.parse().unwrap(),
        file_info: if file.mime_type.as_deref() == Some("application/vnd.google-apps.folder") {
            None
        } else {
            Some(FileInfo {
                md5: file.md5_checksum.clone().unwrap(),
                size: file.size.clone().unwrap().parse().unwrap(),
            })
        },
    }
}

fn page_to_parents(page: &Page) -> Vec<ScannedItem> {
    if let Some(ref files) = page.files {
        files
            .into_iter()
            .filter(|x| accepted_document_type(x))
            .map(to_scanned_item)
            .filter(|x| x.file_info.is_none())
            .collect()
    } else {
        vec![]
    }
}

fn page_to_files_and_parents(
    page: &Page,
) -> Vec<(String, Vec<ScannedItem>)> {
    if let Some(ref files) = page.files {
        files
            .into_iter()
            .filter(|x| accepted_document_type(x))
            .map(to_scanned_item)
            .group_by(|f| f.parent.clone())
            .into_iter()
            .map(|(s, d)| (s.to_string(), d.collect()))
            .collect()
    } else {
        vec![]
    }
}

async fn handle_one_page(drive_cache: &DriveCache, page: &Page) {
    let files = page_to_files_and_parents(page);
    let parents = page_to_parents(page);

    for (p, i) in files {
        if let Err(e) = drive_cache.add_items(&p, i).await {
            dbg!(&e);
        }
    }

    for p in parents {
        if let Err(e) = drive_cache.update_parent(&p).await {
            dbg!(&e);
        }
    }
}