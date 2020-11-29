use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tracing::{error, trace};

const ENTRY_SIZE: usize = 8;

#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub timestamp: u32,
    pub offset: u32,
}

pub(crate) struct Index {
    base: usize,
    path: PathBuf,
    inner: Option<Inner>,
}

#[derive(Debug)]
struct Inner {
    writer: BufWriter<File>,
    indexes: Vec<IndexEntry>,
}
impl Index {
    pub fn new(path: impl AsRef<Path>, base: usize) -> Self {
        let path = path.as_ref().join(format!("{:020}.idx", base));
        Index {
            base,
            path,
            inner: None,
        }
    }

    async fn init(&mut self) -> Result<()> {
        if let None = self.inner {
            self.inner = Some(Inner::open(self.path.as_path()).await?)
        }
        Ok(())
    }

    pub async fn append(&mut self, timestamp: u32, offset: u32) -> Result<usize> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            Ok(self.base + inner.append(IndexEntry { timestamp, offset }).await?)
        } else {
            Err(anyhow!("not initialized segment index"))
        }
    }

    pub async fn read(&mut self, index: u32) -> Result<&IndexEntry> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.read(index).await
        } else {
            Err(anyhow!("not initialized segment index"))
        }
    }

    pub fn size(&mut self) -> usize {
        if let Some(inner) = self.inner.as_mut() {
            inner.size()
        } else {
            0
        }
    }

    pub async fn flush(&mut self) {
        if let Some(inner) = self.inner.as_mut() {
            inner.flush().await
        }
    }

    fn close(&mut self) {
        self.inner = None;
    }

    pub async fn last(&mut self) -> Option<IndexEntry> {
        if let Some(inner) = self.inner.as_mut() {
            inner.indexes.last().copied()
        } else {
            Inner::read_last(self.path.as_path()).await.ok()
        }
    }
}

impl Inner {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await?;
        let metadata = file.metadata().await?;
        trace!("segment index open: {:?}", file);

        let writer = BufWriter::new(file);

        let indexes = if metadata.len() > 0 {
            Inner::load(path).await?
        } else {
            vec![]
        };
        Ok(Inner { writer, indexes })
    }

    async fn read_last(path: impl AsRef<Path>) -> Result<IndexEntry> {
        let mut file = OpenOptions::new().read(true).open(path.as_ref()).await?;
        file.seek(SeekFrom::End(-1 * (ENTRY_SIZE as i64))).await?;
        let mut buf = BytesMut::with_capacity(ENTRY_SIZE);
        buf.resize(ENTRY_SIZE, 0);
        file.read_exact(buf.as_mut()).await?;
        Ok(IndexEntry {
            timestamp: buf.get_u32(),
            offset: buf.get_u32(),
        })
    }

    async fn load(path: impl AsRef<Path>) -> Result<Vec<IndexEntry>> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let size = file.metadata().await?.len() as usize;
        let mut buf = BytesMut::with_capacity(size);
        buf.resize(size, 0);
        file.read_exact(buf.as_mut()).await?;

        let indexes = buf
            .chunks(ENTRY_SIZE)
            .into_iter()
            .map(|mut entry| {
                let timestamp = entry.get_u32();
                let offset = entry.get_u32();
                //trace!("load {}:{}", offset, timestamp);
                IndexEntry { timestamp, offset }
            })
            .collect();

        Ok(indexes)
    }

    fn size(&self) -> usize {
        self.indexes.len()
    }

    async fn append(&mut self, entry: IndexEntry) -> Result<usize> {
        trace!("append: {:?}", entry);
        let mut buf = BytesMut::with_capacity(ENTRY_SIZE);
        buf.put_u32(entry.timestamp);
        buf.put_u32(entry.offset);
        //trace!("append:{} -  {:?}", buf.len(), buf);
        self.writer.write_all(buf.borrow()).await?;
        self.indexes.push(entry);
        Ok(self.indexes.len() - 1)
    }

    async fn read(&mut self, offset: u32) -> Result<&IndexEntry> {
        trace!("read: {}", offset);
        self.indexes
            .get(offset as usize)
            .ok_or(anyhow!("missing index offset: {}", offset))
    }

    async fn flush(&mut self) {
        if let Err(err) = self.writer.flush().await {
            error!(cause = ?err, "flush error");
        }
    }
}
