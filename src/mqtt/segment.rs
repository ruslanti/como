use std::path::Path;

use anyhow::{anyhow, Error, Result};
use futures::TryStreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{BufReader, BufWriter};
use tokio::stream::StreamExt;

pub(crate) struct Segment {
    segment: BufWriter<File>,
}

impl Segment {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut entries = tokio::fs::read_dir(path).await?;

        /*        while let Some(entry) = entries.try_filter(|d| true).next_entry().await? {
            println!("{:?}", entry.path().ends_with());
        }*/
        Ok(Segment {
            segment: BufWriter::new(OpenOptions::new().append(true).open(".").await?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            Segment::open("data/topic/").await.unwrap();
        })
    }
}
