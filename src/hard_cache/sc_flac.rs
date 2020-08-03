// Related resources:
// - https://xiph.org/flac/format.html

use super::{HardCacheDownloader, smart_cacher::{
    FileSpec, ScErr::*, ScResult, SmartCacher,
    SmartCacherSpec,
}};
use async_trait::async_trait;
use crate::config::SmartCacherConfig;

static SPEC: SmartCacherSpec = SmartCacherSpec {
    name: "flac_alpha",
    version: 0,
    exts: &["flac"],
    header_bytes: 65536,
};

pub struct ScFlac;

#[async_trait]
impl SmartCacher for ScFlac {
    fn spec(&self) -> &'static SmartCacherSpec {
        &SPEC
    }

    async fn cache(
        &self,
        header_data: &[u8],
        config: &SmartCacherConfig,
        file_spec: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult {
        // FLAC files start with "fLaC"
        // The interesting data will be:
        // Header length in STREAM
        // METADATA_BLOCK_SEEKTABLE
        // Offset in SEEKPOINT
        // TODO
        Err(Cancel)
    }
}