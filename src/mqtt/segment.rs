use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tracing::trace;

use crate::mqtt::index::IndexEntry;

const HEADER_SIZE: u32 = 16;

pub(crate) struct Segment {
    path: PathBuf,
    inner: Option<Inner>,
}

#[derive(Debug)]
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

    async fn init(&mut self) -> Result<()> {
        if let None = self.inner {
            self.inner = Some(Inner::open(self.path.as_path()).await?)
        }
        Ok(())
    }

    pub async fn append(&mut self, payload: &[u8]) -> Result<IndexEntry> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.append(payload).await
        } else {
            Err(anyhow!("not initialized segment"))
        }
    }

    async fn read(&mut self, position: u32, size: u32) -> Result<(u32, Bytes)> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.read(position, size).await
        } else {
            Err(anyhow!("not initialized segment"))
        }
    }

    fn size(&mut self) -> u32 {
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
}

impl Inner {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .await?;
        let metadata = file.metadata().await?;
        trace!("open: {:?}", file);

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
        trace!("append: {:?}", ret);
        let mut buf = BytesMut::with_capacity(HEADER_SIZE as usize + payload.len());
        buf.put_u32(self.offset);
        buf.put_u32(self.position);
        buf.put_u32(timestamp);
        buf.put_u32(payload.len() as u32);
        buf.put(payload);
        self.buf.write_all(buf.as_ref()).await?;

        self.offset += 1;
        self.position += 16 + payload.len() as u32;
        Ok(ret)
    }

    async fn read(&mut self, pos: u32, size: u32) -> Result<(u32, Bytes)> {
        trace!("read: {}", pos);
        self.buf.get_mut().seek(SeekFrom::Start(pos as u64)).await?;
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        self.buf.read_exact(buf.as_mut()).await?;
        buf.advance(8);
        let timestamp = buf.get_u32();
        buf.advance(4);
        Ok((timestamp, Bytes::from(buf)))
    }

    async fn flush(&mut self) -> Result<()> {
        self.buf.flush().await.map_err(Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    const PAYLOAD_SIZE: u32 = 100;
    const RECORDS: u32 = 100;
    const TEST100: &'static [u8; 7] = b"TEST100";
    const TEST101: &'static [u8; 8] = b"TEST1001";

    #[tokio::test]
    async fn test_segment() {
        tracing_subscriber::fmt::init();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let mut r = rand::thread_rng();
        let s = r.gen();

        let mut segment = Segment::new("/tmp/", s);
        let mut position = 0;
        for i in 0u32..RECORDS {
            let ret = segment
                .append(vec![8; PAYLOAD_SIZE as usize].as_slice())
                .await
                .unwrap();
            assert_eq!(i, ret.offset);
            assert_eq!(position, ret.position);
            assert_eq!(timestamp, ret.timestamp);
            position += HEADER_SIZE + PAYLOAD_SIZE;
        }
        assert_eq!(position, segment.size());

        segment.flush().await.unwrap();
        for i in (0..RECORDS).rev() {
            let (t, b) = segment
                .read(
                    HEADER_SIZE * i + PAYLOAD_SIZE * i,
                    HEADER_SIZE + PAYLOAD_SIZE,
                )
                .await
                .unwrap();
            assert_eq!(timestamp, t);
            assert_eq!(PAYLOAD_SIZE as usize, b.len());
            assert_eq!([8; PAYLOAD_SIZE as usize], b.as_ref());
        }

        let ret = segment.append(TEST100).await.unwrap();
        assert_eq!(100, ret.offset);
        assert_eq!(HEADER_SIZE * 100 + PAYLOAD_SIZE * 100, ret.position);
        assert_eq!(timestamp, ret.timestamp);
        let ret = segment.append(TEST101).await.unwrap();
        assert_eq!(101, ret.offset);
        assert_eq!(
            HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len() as u32,
            ret.position
        );
        assert_eq!(timestamp, ret.timestamp);

        segment.flush().await.unwrap();
        let (t, b) = segment
            .read(
                HEADER_SIZE * 100 + PAYLOAD_SIZE * 100,
                HEADER_SIZE + TEST100.len() as u32,
            )
            .await
            .unwrap();
        assert_eq!(timestamp, t);
        assert_eq!(TEST100, b.as_ref());
        let (t, b) = segment
            .read(
                HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len() as u32,
                HEADER_SIZE + TEST101.len() as u32,
            )
            .await
            .unwrap();
        assert_eq!(timestamp, t);
        assert_eq!(TEST101, b.as_ref());

        tokio::fs::remove_file(format!("/tmp/{:020}.log", s))
            .await
            .unwrap();
    }
}
