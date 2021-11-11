use std::path::PathBuf;

#[derive(Debug)]
pub struct ScratchDriveAccess {
    pub name: String,
    pub path: PathBuf,
}

impl ScratchDriveAccess {
    pub fn new(name: String, path: PathBuf) -> Self {
        Self { name, path }
    }
}
