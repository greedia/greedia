// Related resources:
// - https://xiph.org/flac/format.html

use super::{
    smart_cacher::{FileSpec, ScErr::*, ScResult, SmartCacher, SmartCacherSpec},
    HardCacheDownloader,
};
use crate::config::SmartCacherConfig;
use async_trait::async_trait;

static SPEC: SmartCacherSpec = SmartCacherSpec {
    name: "flac_testing",
    exts: &["flac"],
};

pub struct ScFlac;

#[async_trait]
impl SmartCacher for ScFlac {
    fn spec(&self) -> &'static SmartCacherSpec {
        &SPEC
    }

    async fn cache(
        &self,
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
