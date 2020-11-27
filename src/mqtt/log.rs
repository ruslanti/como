use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Error, Result};
use bytes::Bytes;
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
                let index = Index::new(path.as_path(), base);
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
        if segments.is_empty() {
            let base = 0;
            let index = Index::new(path.as_path(), base);
            let segment = Segment::new(path.as_path(), base);
            segments.push(SegmentEntry {
                base,
                index,
                segment,
            })
        }
        Ok(Log {
            path,
            segment_size,
            segments,
        })
    }

    async fn append(&mut self, payload: &[u8]) -> Result<usize> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let entry = self.segments.last_mut().expect("missing data segment");
        let offset = entry.segment.append(timestamp, payload).await?;
        Ok(entry.index.append(timestamp, offset).await?)
    }

    async fn read(&mut self, offset: u32) -> Result<(u32, Bytes)> {
        let entry = self.segments.last_mut().expect("missing data segment");
        let index = entry.index.read(offset).await?;
        entry.segment.read(index.offset).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log() {
        tracing_subscriber::fmt::init();
        let mut log = Log::new("/tmp", "test_log", 10).await.unwrap();
        let r = log.append(b"TTTTTTTTTTTTTTTTTTTTTT").await.unwrap();
    }
}
