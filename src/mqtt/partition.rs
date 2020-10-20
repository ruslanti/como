use std::path::Path;

use anyhow::{anyhow, Result};
use tokio::fs;
use tokio::stream::StreamExt;

pub(crate) struct Partition {}

impl Partition {
    pub(crate) async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let mut entries = fs::read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            println!("{:?}", entry.path());
            /*            match entry.path().extension() {
                Some("index".as_ref()) => {},
                None => {}
            }*/
        }
        Ok(Partition {})
    }
}
