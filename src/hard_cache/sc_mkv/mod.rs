// Matroska files use an encoding format called EBML to encode all the data in the headers.
//
// Since it is meant to be a very flexible format that takes pretty much any type of video or audio streams,
// MKV files don't appear to have bitrates in their metadata. Instead, they have "clusters" which contain a
// starting timestamp in each. We can use this to figure out how far to download. We can also use the
// seek table to find other components within the mkv file, such as tags and attachments.
//
// I have (somewhat shamefully) stolen and modified ebml.rs from the rust `matroska` library
// (https://github.com/tuffy/matroska/blob/master/src/ebml.rs).

use core::time::Duration;
use std::collections::BTreeMap;

use crate::config::SmartCacherConfig;

use self::{ebml::MResult, mkv::Seek};

use super::{
    smart_cacher::{FileSpec, ScErr::*, ScOk::*, ScResult, SmartCacher, SmartCacherSpec},
    HardCacheDownloader,
};
use async_trait::async_trait;
use ebml::{read_uint, Element, ElementType};
use ids::EBML_DOC_TYPE;
use mkv::Info;
use tokio::io::AsyncRead;

mod ebml;
mod ids;
mod mkv;

static SPEC: SmartCacherSpec = SmartCacherSpec {
    name: "mkv_testing",
    exts: &["mkv", "webm"],
};

pub struct ScMkv;

#[async_trait]
impl SmartCacher for ScMkv {
    fn spec(&self) -> &'static SmartCacherSpec {
        &SPEC
    }

    async fn cache(
        &self,
        config: &SmartCacherConfig,
        _file_specs: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult {
        // Attempt to read header
        let mut r = action.reader(0);

        let (id_0, size_0, _) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
        if id_0 != ids::EBML_HEADER {
            return Err(Cancel);
        }

        // Verify that the doctype is matroska or webm
        let m = Element::parse_master(&mut r, size_0).await.unwrap();
        for e in m {
            if e.id == EBML_DOC_TYPE {
                if let ElementType::String(val) = e.val {
                    if val != "matroska" && val != "webm" {
                        return Err(Cancel);
                    }
                }
            }
        }

        // Find other elements
        let (id_0, _, _) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;

        if id_0 != ids::SEGMENT {
            // Root element should be a SEGMENT
            return Err(Cancel);
        }

        let segment_start = r.tell();

        let mut seek_table = None;
        let big_element_offset;
        let mut info: Option<Info> = None;

        let mut got_info = false;
        let mut got_attachments = false;
        let mut got_chapters = false;
        let mut got_tracks = false;
        let mut got_cues = false;
        let mut got_tags = false;

        // Loop to get the sequential headers
        loop {
            let start_offset = r.tell();
            let (id_1, size_1, _) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
            let after_eid_offset = r.tell();

            match id_1 {
                ids::SEEKHEAD => {
                    let st = OrderedSeektable::parse(&mut r, size_1)
                        .await
                        .or(Err(Cancel))?;
                    seek_table = Some(st);
                }
                ids::INFO => {
                    got_info = true;
                    info = Some(Info::parse(&mut r, size_1).await.or(Err(Cancel))?);
                }
                ids::ATTACHMENTS => {
                    got_attachments = true;
                    r.cache_bytes(size_1).await?;
                }
                ids::CHAPTERS => {
                    got_chapters = true;
                    r.cache_bytes(size_1).await?;
                }
                ids::TRACKS => {
                    got_tracks = true;
                    r.cache_bytes(size_1).await?;
                }
                ids::CUES => {
                    got_cues = true;
                    r.cache_bytes(size_1).await?;
                }
                ids::TAGS => {
                    got_tags = true;
                    r.cache_bytes(size_1).await?;
                }
                ids::EBML_VOID => {
                    r.cache_bytes(size_1).await?;
                }
                ids::CLUSTER => {
                    let timecode_scale =
                        info.as_ref().ok_or(Cancel)?.timecode_scale.ok_or(Cancel)?;

                    // Get the starting timecode of this cluster
                    let (_, size_2, _) =
                        ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                    let timecode = read_uint(&mut r, size_2).await.or(Err(Cancel))?;

                    let cluster_start = Duration::from_nanos(timecode_scale * timecode).as_secs();
                    if cluster_start > config.seconds {
                        big_element_offset = Some(after_eid_offset + size_1);
                        break;
                    } else {
                        r.cache_bytes_to(after_eid_offset + size_1).await?;
                    }
                }
                _ => {
                    // Unknown ID
                    if size_1 < 100_000 {
                        // It's a small enough element, so just cache it.
                        r.cache_bytes(size_1).await?;
                    } else {
                        big_element_offset = Some(start_offset);
                        break;
                    }
                }
            }
        }

        // If any sections were missed in the headers and seek_table entries exist for them, grab them now.
        if let Some(seek_table) = seek_table {
            let seek_items = if let Some(big_element_offset) = big_element_offset {
                seek_table.seek.range(big_element_offset..)
            } else {
                seek_table.seek.range(..)
            };

            // ffmpeg grabs a bunch of bytes before the tail for some reason, so lets get 10kb just in case.
            let pre_tail_cache = 10_000;
            let mut pre_tail_cached = false;

            for (item_off, item_id) in seek_items {
                if !pre_tail_cached {
                    r.seek(segment_start + item_off - pre_tail_cache);
                    r.cache_bytes(pre_tail_cache).await?;
                    pre_tail_cached = true;
                }

                match *item_id {
                    ids::INFO => {
                        if got_info {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::INFO {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    ids::TRACKS => {
                        if got_tracks {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::TRACKS {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    ids::CHAPTERS => {
                        if got_chapters {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::CHAPTERS {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    ids::CUES => {
                        if got_cues {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::CUES {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    ids::ATTACHMENTS => {
                        if got_attachments {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::ATTACHMENTS {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    ids::TAGS => {
                        if got_tags {
                            continue;
                        }
                        r.seek(segment_start + item_off);

                        let (id_1, size_1, _) =
                            ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
                        if id_1 == ids::TAGS {
                            r.cache_bytes(size_1).await?;
                        }
                    }
                    _item_id => {}
                }
            }
        }

        Ok(Finalize)
    }
}

/// Seek table that keys the offsets rather than the IDs.
#[derive(Debug)]
struct OrderedSeektable {
    pub seek: BTreeMap<u64, u32>,
}

impl OrderedSeektable {
    fn new() -> OrderedSeektable {
        OrderedSeektable {
            seek: BTreeMap::new(),
        }
    }

    async fn parse<R: AsyncRead + Send + Sync + Unpin>(
        r: &mut R,
        size: u64,
    ) -> MResult<OrderedSeektable> {
        let mut seektable = OrderedSeektable::new();
        for e in Element::parse_master(r, size).await? {
            if let Element {
                id: ids::SEEK,
                val: ElementType::Master(sub_elements),
                ..
            } = e
            {
                let seek = Seek::build(sub_elements);
                seektable.seek.insert(seek.position, seek.id());
            }
        }
        Ok(seektable)
    }
}
