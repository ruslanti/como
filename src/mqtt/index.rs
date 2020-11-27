use std::borrow::Borrow;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Error, Result};
use serde::de::Visitor;
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::trace;

#[derive(Debug)]
pub(crate) struct IndexEntry {
    pub timestamp: u32,
    pub offset: u32,
}

pub(crate) struct Index {
    path: PathBuf,
    inner: Option<Inner>,
}

#[derive(Debug)]
struct Inner {
    writer: BufWriter<File>,
    indexes: Vec<IndexEntry>,
}

struct IndexEntryVisitor;

impl<'de> Visitor<'de> for IndexEntryVisitor {
    type Value = IndexEntry;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between 0 and 2")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(IndexEntry {
            timestamp: (value >> 32) as u32,
            offset: (value & 0x00000000FFFFFFFF) as u32,
        })
    }
}

impl<'de> Deserialize<'de> for IndexEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(IndexEntryVisitor)
    }
}

impl Serialize for IndexEntry {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut index = serializer.serialize_struct("Index", 2)?;
        index.serialize_field("timestamp", &self.timestamp)?;
        index.serialize_field("offset", &self.offset)?;
        index.end()
    }
}

impl Index {
    pub fn new(path: impl AsRef<Path>, segment: u64) -> Self {
        let path = path.as_ref().join(format!("{:020}.idx", segment));
        Index { path, inner: None }
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
            inner.append(IndexEntry { timestamp, offset }).await
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

    fn size(&mut self) -> usize {
        if let Some(inner) = self.inner.as_mut() {
            inner.size()
        } else {
            0
        }
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(inner) = self.inner.as_mut() {
            inner.flush().await
        } else {
            Ok(())
        }
    }

    fn close(&mut self) {
        self.inner = None;
    }
}

impl Inner {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await?;
        trace!("segment index open: {:?}", file);

        let writer = BufWriter::new(file);

        let indexes = Inner::load(path).await?;
        Ok(Inner { writer, indexes })
    }

    async fn load(
        path: impl AsRef<Path>,
    ) -> ::std::result::Result<Vec<IndexEntry>, Box<bincode::ErrorKind>> {
        let file = OpenOptions::new().read(true).open(path).await?;
        bincode::deserialize_from(file.into_std().await)
    }

    fn size(&self) -> usize {
        self.indexes.len()
    }

    async fn append(&mut self, entry: IndexEntry) -> Result<usize> {
        let buf = bincode::serialize(entry.borrow())?;
        self.writer.write_all(buf.as_ref()).await?;
        self.indexes.push(entry);
        Ok(self.indexes.len() - 1)
    }

    async fn read(&mut self, offset: u32) -> Result<&IndexEntry> {
        trace!("read: {}", offset);
        self.indexes
            .get(offset as usize)
            .ok_or(anyhow!("missing index offset: {}", offset))
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await.map_err(Error::msg)
    }
}
