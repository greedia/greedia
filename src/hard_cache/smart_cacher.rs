use super::HardCacheDownloader;
use crate::{cache_handlers::CacheHandlerError, config::SmartCacherConfig};
use async_trait::async_trait;

/// Global smart cacher version. Increment this any time a release is made with
/// a new or updated smart cacher.
pub static SMART_CACHER_VERSION: u64 = 0;

/// General information about the file that could assist in scanning.
pub struct FileSpec {
    /// File name.
    pub name: String,
    /// Total size of file.
    pub size: u64,
}

/// Specification for a SmartCacher.
pub struct SmartCacherSpec {
    /// Name of this SmartCacher.
    pub name: &'static str,
    /// File extensions that are generally supported by this SmartCacher.
    pub exts: &'static [&'static str],
}

#[async_trait]
pub trait SmartCacher: Sync + Send {
    /// Function that returns specifications about this cacher.
    fn spec(&self) -> &'static SmartCacherSpec;
    /// Called to validate that the SmartCacher can process this file.
    /// If successful, return new instance of this SmartCacher.
    async fn cache(
        &self,
        config: &SmartCacherConfig,
        file_specs: &FileSpec,
        action: &mut HardCacheDownloader,
    ) -> ScResult;
}

pub type ScResult = Result<ScOk, ScErr>;

pub enum ScOk {
    /// Consider file download to be successful after all data reads are finished.
    Finalize,
}

/// Finish caching the file, once all read and cache calls have been completed.
pub enum ScErr {
    /// Consider file download to be unsuccessful, but keep downloaded data.
    /// The cacher will then attempt to download using a different SmartCacher.
    Cancel,
    CacheHandlerError(CacheHandlerError)
}

impl From<CacheHandlerError> for ScErr {
    fn from(e: CacheHandlerError) -> Self {
        ScErr::CacheHandlerError(e)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_cacher() {
        // This isn't necessarily meant to be a unit test, but
        // rather a testbed to experiment with a SmartCacher.
    }
}
