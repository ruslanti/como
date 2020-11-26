use std::path::Path;

use anyhow::Result;
use memmap::Mmap;
use tokio::fs::{File, OpenOptions};
use tokio::io::BufWriter;

#[derive(Debug)]
pub(crate) struct IndexEntry {
    pub offset: u32,
    pub position: u32,
    pub timestamp: u32,
}

pub(crate) struct Index {
    offset: u32,
    position: u32,
    buf: Mmap,
}

impl Index {
    pub async fn new<P: AsRef<Path>>(path: P, segment: u64) -> Result<Self> {
        let name = path.as_ref().join(format!("{:020}.idx", segment));

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(name)
            .await?;
        let metadata = file.metadata().await?;

        let buf = unsafe { Mmap::map(&file.into_std().await)? };

        Ok(Index {
            offset: 0,
            position: metadata.len() as u32,
            buf,
        })
    }

    pub fn last_offset(&self) -> u32 {
        0
    }

    pub async fn append(&mut self, entry: IndexEntry) {
        let g = self.buf[0];
    }
}
