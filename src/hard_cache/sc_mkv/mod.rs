// Matroska files use an encoding format called EBML to encode all the data in the headers.
//
// Since it is meant to be a very flexible format that takes pretty much any type of video or audio streams,
// MKV files don't appear to have bitrates in their metadata. Instead, they just have seek tables, which should
// be enough to figure out approximately how far to download.
//
// I have (somewhat shamefully) stolen and modified ebml.rs from the rust `matroska` library
// (https://github.com/tuffy/matroska/blob/master/src/ebml.rs).

use crate::config::SmartCacherConfig;

use super::{HardCacheDownloader, smart_cacher::{FileSpec, ScOk::*, ScErr::*, ScResult, SmartCacher, SmartCacherSpec}};
use async_trait::async_trait;
use ebml::{Element, ElementType};
use ids::EBML_DOC_TYPE;

mod mkv;
mod ebml;
mod ids;

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
        _config: &SmartCacherConfig,
        _file_specs: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult {
        // Attempt to read header
        let mut r = action.reader(0);
        let (id_0, size_0, _) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
        if id_0 != ids::EBML_HEADER {
            return Err(Cancel);
        }

        // Verify that the doctype is matroska
        let m = Element::parse_master(&mut r, size_0).await.unwrap();
        for e in m {
            if e.id == EBML_DOC_TYPE {
                if let ElementType::String(val) = e.val {
                    if val != "matroska" && val != "webm" {
                        return Err(Cancel)
                    }
                    println!("We have file type {}", val);
                }
            }
        }

        // Find other elements
        let (mut id_0, mut size_0, _) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
        println!("id: 0x{:X} size: {}", id_0, size_0);

        if id_0 != ids::SEGMENT {
            return Err(Cancel); // TODO: loop through to find SEGMENT
        }

        let segment_start = r.offset;
        let (id_1, size_1, len) = ebml::read_element_id_size(&mut r).await.or(Err(Cancel))?;
        println!("id: 0x{:X} size: {} len: {}", id_1, size_1, len);

        
        

        Ok(Finalize)
    }
}