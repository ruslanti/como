use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use std::borrow::Borrow;
use std::cmp::Ordering::Greater;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio_stream::StreamExt;
use tracing::{debug, trace};

use crate::mqtt::index::Index;
use crate::mqtt::segment::Segment;

pub struct Record {}

struct SegmentEntry {
    base: usize,
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
            let mut base_offsets: Vec<usize> = files
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    trace!("file: {:?}", entry);
                    entry.path().file_name().and_then(|file| {
                        file.to_str().and_then(|name| {
                            name.strip_suffix(".log")
                                .and_then(|s| s.parse::<usize>().ok())
                        })
                    })
                })
                .collect()
                .await;
            base_offsets.sort();
            for base in base_offsets {
                trace!("offset: {}", base);

                let index = Index::new(path.as_path(), base);
                let segment = Segment::new(path.as_path(), base);
                segments.push(SegmentEntry {
                    base,
                    index,
                    segment,
                })
            }
        } else {
            trace!("create dir: {:?}", path.as_path());
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

    fn get_segment_index(&self, offset: usize) -> usize {
        let index = self
            .segments
            .binary_search_by(|segment| segment.base.cmp(offset.borrow()).then(Greater));
        match index {
            Err(0) => 0,
            Err(i) => i - 1,
            Ok(i) => i,
        }
    }

    fn get_last_offset(&mut self) -> usize {
        let entry = self.segments.last_mut().expect("missing data segment");
        entry.base + entry.index.size()
    }

    async fn append(&mut self, payload: &[u8]) -> Result<usize> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let entry = self.segments.last_mut().expect("missing data segment");
        let offset = entry.segment.append(timestamp, payload).await?;
        let index = entry.index.append(timestamp, offset).await?;
        if entry.segment.size() >= self.segment_size {
            self.flush().await;
            let base = index + 1;
            let index = Index::new(self.path.as_path(), base);
            let segment = Segment::new(self.path.as_path(), base);
            self.segments.push(SegmentEntry {
                base,
                index,
                segment,
            })
        }
        Ok(index)
    }

    async fn read(&mut self, offset: usize) -> Result<(u32, Bytes)> {
        let index = self.get_segment_index(offset);
        debug!("index {}", index);
        debug!("segments {}", self.segments.len());
        if let Some(entry) = self.segments.get_mut(index) {
            debug!("offset {} - {}", offset, entry.base);
            let offset = (offset - entry.base) as u32;
            debug!("offset {}", offset);
            let index = entry.index.read(offset).await?;
            entry.segment.read(index.offset).await
        } else {
            Err(anyhow!("log segment not found for: {}", offset))
        }
    }

    async fn flush(&mut self) {
        if let Some(entry) = self.segments.last_mut() {
            entry.segment.flush().await;
            entry.index.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    #[tokio::test]
    async fn test_log() {
        tracing_subscriber::fmt::init();
        let mut payload = BytesMut::with_capacity(4);
        payload.put_u32(0xDEADBEAF);
        let mut log = Log::new("/tmp", "test_log", 100 * 1024 * 1024)
            .await
            .unwrap();
        for i in 0..100000 {
            let payload = format!("TEST_{}", i);
            let r = log.append(payload.as_bytes()).await.unwrap();
            debug!("append: {}", r);
        }
        log.flush().await;
        let last = log.get_last_offset() - 1;
        debug!("last offset: {}", last);
        let record = log.read(last).await.unwrap();
        debug!("{:?}", record);

        for mut seg in log.segments {
            let until = seg.index.last().await.unwrap();
            debug!(
                "segment {} - {} - {}",
                seg.base, until.timestamp, until.offset
            );
        }
    }
}
