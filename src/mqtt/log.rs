use std::path::Path;
use std::path::PathBuf;

use anyhow::{Error, Result};
use futures::TryFutureExt;
use tokio::fs;
use tokio::stream::StreamExt;

use crate::mqtt::index::Index;
use crate::mqtt::segment::Segment;

pub struct Record {}

struct SegmentEntry {
    base: u64,
    index: Index,
    segment: Segment,
}

struct Log {
    path: PathBuf,
    segment_size: u32,
    segments: Vec<SegmentEntry>,
}

impl Log {
    async fn new<P: AsRef<Path>>(path: P, name: &str, segment_size: u32) -> Result<Self> {
        let mut segments = vec![];
        let path = path.as_ref().join(name);
        if let Ok(files) = fs::read_dir(path.as_path()).await {
            let mut base_offsets: Vec<u64> = files
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    entry.path().file_name().and_then(|file| {
                        file.to_str().and_then(|name| {
                            name.strip_suffix(".log")
                                .and_then(|s| s.parse::<u64>().ok())
                        })
                    })
                })
                .collect()
                .await;
            base_offsets.sort();
            for base in base_offsets {
                let index = Index::new(path.as_path(), base).await?;
                let segment = Segment::new(path.as_path(), base);
                segments.push(SegmentEntry {
                    base,
                    index,
                    segment,
                })
            }
        } else {
            fs::create_dir(path.as_path()).map_err(Error::msg).await?;
        }
        Ok(Log {
            path,
            segment_size,
            segments,
        })
    }

    async fn append(&mut self, payload: &[u8]) -> Result<u32> {
        let entry = self.segments.last_mut().expect("missing data segment");
        let index = entry.segment.append(payload).await?;
        let offset = index.offset;
        entry.index.append(index).await;
        Ok(offset)
    }

    async fn read(&self, pos: u32) -> Result<()> {
        //ensure!(self.start < pos, "invalid segment position");
        /*        let index: usize = pos - self.start;
        let segment = self
            .segments
            .get(index / self.segment_size)
            .context("unknown segment")?;*/
        Ok(())
        // segment.read().await.read(index % self.segment_size).await
    }
}
