use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Error, Result};
use bytes::{Bytes, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tracing::{info, trace};

use crate::mqtt::index::IndexEntry;

pub(crate) struct Segment {
    path: PathBuf,
    inner: Option<Inner>,
}

struct Inner {
    offset: u32,
    position: u32,
    buf: BufWriter<File>,
}

impl Segment {
    pub fn new(path: impl AsRef<Path>, segment: u64) -> Self {
        let path = path.as_ref().join(format!("{:020}.log", segment));
        Segment { path, inner: None }
    }

    pub async fn append(&mut self, payload: &[u8]) -> Result<IndexEntry> {
        let inner = self
            .inner
            .get_or_insert(Inner::open(self.path.as_path()).await?);
        inner.append(payload).await
    }

    async fn read(&mut self, pos: u32) -> Result<(u32, Bytes)> {
        let inner = self
            .inner
            .get_or_insert(Inner::open(self.path.as_path()).await?);
        inner.read(pos).await
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(inner) = self.inner.as_mut() {
            inner.flush().await
        } else {
            Ok(())
        }
    }
}

impl Inner {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?;
        let metadata = file.metadata().await?;

        let buf = BufWriter::new(file);

        Ok(Inner {
            offset: 0,
            position: metadata.len() as u32,
            buf,
        })
    }

    fn set_offset(&mut self, offset: u32) {
        self.offset = offset;
    }

    fn size(&self) -> u32 {
        self.position
    }

    async fn append(&mut self, payload: &[u8]) -> Result<IndexEntry> {
        self.buf
            .get_mut()
            .seek(SeekFrom::Start(self.position as u64))
            .await?;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;
        let ret = IndexEntry {
            offset: self.offset,
            position: self.position,
            timestamp,
        };
        self.buf.write_u32(self.offset).await?;
        self.buf.write_u32(self.position).await?;
        self.buf.write_u32(timestamp).await?;
        self.buf.write_u32(payload.len() as u32).await?;
        self.buf.write_all(payload).await?;

        self.offset += 1;
        self.position += 16 + payload.len() as u32;
        trace!("append: {:?}", ret);
        Ok(ret)
    }

    async fn read(&mut self, pos: u32) -> Result<(u32, Bytes)> {
        self.buf.get_mut().seek(SeekFrom::Start(pos as u64)).await?;
        let _ = self.buf.read_u64().await?;
        let timestamp = self.buf.read_u32().await?;
        let size = self.buf.read_u32().await?;
        let mut payload = BytesMut::with_capacity(size as usize);
        self.buf.read_exact(payload.as_mut()).await?;
        Ok((timestamp, Bytes::from(payload)))
    }

    async fn flush(&mut self) -> Result<()> {
        self.buf.flush().await.map_err(Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    use super::*;

    #[tokio::test]
    async fn test_segment() {
        let (non_blocking, _) = tracing_appender::non_blocking(std::io::stdout());
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            //.with_ansi(false)
            .with_writer(non_blocking)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no global subscriber has been set");
        info!("This will _not_ be logged to stdout");

        let mut segment = Segment::new("/tmp/", 1234);
        for i in 0..100 {
            let ret = segment.append(b"0123456789").await.unwrap();
            assert_eq!(i, ret.offset);
            assert_eq!((26 * i), ret.position);
            assert_ne!(0, ret.timestamp);
        }

        let ret = segment.append(b"0123456789").await.unwrap();
        assert_eq!(1, ret.offset);
        assert_eq!(26, ret.position);
        assert_ne!(0, ret.timestamp);

        segment.flush().await;
    }
}
