use std::path::Path;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter, SeekFrom};
use tracing::{error, trace};

const HEADER_SIZE: u32 = 8;

pub(crate) struct Segment {
    base: usize,
    path: PathBuf,
    inner: Option<Inner>,
}

#[derive(Debug)]
struct Inner {
    last_offset: u32,
    writer: BufWriter<File>,
}

impl Segment {
    pub fn new(path: impl AsRef<Path>, segment: usize) -> Self {
        let path = path.as_ref().join(format!("{:020}.log", segment));
        Segment {
            base: 0,
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

    pub async fn append(&mut self, timestamp: u32, payload: &[u8]) -> Result<u32> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.append(timestamp, payload).await
        } else {
            Err(anyhow!("not initialized segment"))
        }
    }

    pub async fn read(&mut self, offset: u32) -> Result<(u32, Bytes)> {
        self.init().await?;
        if let Some(inner) = self.inner.as_mut() {
            inner.read(offset).await
        } else {
            Err(anyhow!("not initialized segment"))
        }
    }

    pub fn size(&mut self) -> u32 {
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
            last_offset: metadata.len() as u32,
            writer: buf,
        })
    }

    fn last_offset(&mut self, offset: u32) {
        self.last_offset = offset;
    }

    fn size(&self) -> u32 {
        self.last_offset
    }

    async fn append(&mut self, timestamp: u32, payload: &[u8]) -> Result<u32> {
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
        self.last_offset += HEADER_SIZE + payload.len() as u32;
        Ok(position)
    }

    async fn read(&mut self, offset: u32) -> Result<(u32, Bytes)> {
        trace!("read: {}", offset);
        self.writer
            .get_mut()
            .seek(SeekFrom::Start(offset as u64))
            .await?;
        let timestamp = self.writer.read_u32().await?;
        let size = self.writer.read_u32().await?;
        let mut buf = BytesMut::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        self.writer.read_exact(buf.as_mut()).await?;
        Ok((timestamp, Bytes::from(buf)))
    }

    async fn flush(&mut self) {
        if let Err(err) = self.writer.flush().await {
            error!(cause = ?err, "flush error");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::Rng;

    use super::*;

    const PAYLOAD_SIZE: u32 = 100;
    const RECORDS: u32 = 100;
    const TEST100: &'static [u8; 7] = b"TEST100";
    const TEST101: &'static [u8; 8] = b"TEST1001";

    #[tokio::test]
    async fn test_segment() {
        //tracing_subscriber::fmt::init();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let mut r = rand::thread_rng();
        let s = r.gen();

        let mut segment = Segment::new("/tmp/", s);
        let mut position = 0;
        for _i in 0..RECORDS {
            let ret = segment
                .append(timestamp, vec![8; PAYLOAD_SIZE as usize].as_slice())
                .await
                .unwrap();
            assert_eq!(position, ret);
            position += HEADER_SIZE + PAYLOAD_SIZE;
        }
        assert_eq!(position, segment.size());

        segment.flush().await;
        for i in (0u32..RECORDS).rev() {
            let (t, b) = segment
                .read(HEADER_SIZE * i + PAYLOAD_SIZE * i)
                .await
                .unwrap();
            assert_eq!(timestamp, t);
            assert_eq!(PAYLOAD_SIZE as usize, b.len());
            assert_eq!([8; PAYLOAD_SIZE as usize], b.as_ref());
        }

        let ret = segment.append(timestamp, TEST100).await.unwrap();
        assert_eq!(HEADER_SIZE * 100 + PAYLOAD_SIZE * 100, ret);
        let ret = segment.append(timestamp, TEST101).await.unwrap();
        assert_eq!(
            HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len() as u32,
            ret
        );

        segment.flush().await;
        let (t, b) = segment
            .read(HEADER_SIZE * 100 + PAYLOAD_SIZE * 100)
            .await
            .unwrap();
        assert_eq!(timestamp, t);
        assert_eq!(TEST100, b.as_ref());
        let (t, b) = segment
            .read(HEADER_SIZE * 101 + PAYLOAD_SIZE * 100 + TEST100.len() as u32)
            .await
            .unwrap();
        assert_eq!(timestamp, t);
        assert_eq!(TEST101, b.as_ref());

        tokio::fs::remove_file(format!("/tmp/{:020}.log", s))
            .await
            .unwrap();
    }
}
