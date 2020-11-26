use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tracing::trace;

const HEADER_SIZE: usize = 8;

pub(crate) struct Segment {
    path: PathBuf,
    inner: Option<Inner>,
}

#[derive(Debug)]
struct Inner {
    last_offset: usize,
    writer: BufWriter<File>,
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

    pub async fn append(&mut self, timestamp: u32, payload: &[u8]) -> Result<usize> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.append(timestamp, payload).await
        } else {
            Err(anyhow!("not initialized segment"))
        }
    }

    async fn read(&mut self, offset: usize, size: usize) -> Result<(u32, Bytes)> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.read(offset, size).await
        } else {
            Err(anyhow!("not initialized segment"))
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
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .await?;
        let metadata = file.metadata().await?;
        trace!("segment log open: {:?}", file);

        let buf = BufWriter::new(file);

        Ok(Inner {
            last_offset: metadata.len() as usize,
            writer: buf,
        })
    }

    fn last_offset(&mut self, offset: usize) {
        self.last_offset = offset;
    }

    fn size(&self) -> usize {
        self.last_offset
    }

    async fn append(&mut self, timestamp: u32, payload: &[u8]) -> Result<usize> {
        self.writer
            .get_mut()
            .seek(SeekFrom::Start(self.last_offset as u64))
            .await?;
        trace!(
            "append offset:{}, timestamp:{}",
            self.last_offset,
            timestamp
        );
        let mut buf = BytesMut::with_capacity(HEADER_SIZE as usize + payload.len());
        buf.put_u32(timestamp);
        buf.put_u32(payload.len() as u32);
        buf.put(payload);
        self.writer.write_all(buf.as_ref()).await?;
        let position = self.last_offset;
        self.last_offset += HEADER_SIZE + payload.len();
        Ok(position)
    }

    async fn read(&mut self, offset: usize, size: usize) -> Result<(u32, Bytes)> {
        trace!("read: {} size: {}", offset, size);
        self.writer
            .get_mut()
            .seek(SeekFrom::Start(offset as u64))
            .await?;
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        self.writer.read_exact(buf.as_mut()).await?;
        let timestamp = buf.get_u32();
        buf.advance(4);
        Ok((timestamp, Bytes::from(buf)))
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await.map_err(Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    const PAYLOAD_SIZE: usize = 100;
    const RECORDS: usize = 100;
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
        for i in 0..RECORDS {
            let ret = segment
                .append(timestamp, vec![8; PAYLOAD_SIZE as usize].as_slice())
                .await
                .unwrap();
            assert_eq!(position, ret);
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

        let ret = segment.append(timestamp, TEST100).await.unwrap();
        assert_eq!(HEADER_SIZE * 100 + PAYLOAD_SIZE * 100, ret);
        let ret = segment.append(timestamp, TEST101).await.unwrap();
        assert_eq!(HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len(), ret);

        segment.flush().await.unwrap();
        let (t, b) = segment
            .read(
                HEADER_SIZE * 100 + PAYLOAD_SIZE * 100,
                HEADER_SIZE + TEST100.len(),
            )
            .await
            .unwrap();
        assert_eq!(timestamp, t);
        assert_eq!(TEST100, b.as_ref());
        let (t, b) = segment
            .read(
                HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len(),
                HEADER_SIZE + TEST101.len(),
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
