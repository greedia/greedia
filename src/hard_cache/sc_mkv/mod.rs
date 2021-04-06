// Matroska files use an encoding format called EBML to encode all the data in the headers.
//
// Since it is meant to be a very flexible format that takes pretty much any type of video or audio streams,
// MKV files don't appear to have bitrates in their metadata. Instead, they just have seek tables, which should
// be enough to figure out approximately how far to download.
//
// I have (somewhat shamefully) stolen and modified ebml.rs from the rust `matroska` library
// (https://github.com/tuffy/matroska/blob/master/src/ebml.rs).

use crate::config::SmartCacherConfig;

use super::{HardCacheDownloader, smart_cacher::{FileSpec, ScErr::*, ScResult, SmartCacher, SmartCacherSpec}};
use async_trait::async_trait;

mod ebml;
mod ids;

static SPEC: SmartCacherSpec = SmartCacherSpec {
    name: "mkv_testing",
    exts: &["mkv"],
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
        file_specs: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult {
        let (mut id_0, mut size_0, _) = ebml::read_element_id_size(&mut action.reader(0)).await.or(Err(Cancel))?;
        dbg!(&id_0, &size_0, ids::HEADER);
        Err(Cancel)
    }
}