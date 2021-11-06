// Related resources:
// - https://xiph.org/flac/format.html

use async_trait::async_trait;

use super::{
    smart_cacher::{FileSpec, ScErr::*, ScResult, SmartCacher, SmartCacherSpec},
    HardCacheDownloader,
};
use crate::config::SmartCacherConfig;

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
        _config: &SmartCacherConfig,
        _file_spec: &FileSpec,
        _action: &mut HardCacheDownloader,
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
