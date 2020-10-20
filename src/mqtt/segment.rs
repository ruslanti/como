use std::borrow::BorrowMut;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use crate::mqtt::topic::Message;

pub(crate) struct Segment {
    segment: usize,
    offset: u32,
    position: u32,
    index_table: Vec<u32>,
    index: BufWriter<File>,
    log: BufWriter<File>,
}

impl Segment {
    pub(crate) async fn new(partition: impl AsRef<Path>, segment: usize) -> Result<Self> {
        let mut path = partition.as_ref().to_owned();
        path.push(format!("{:020}", segment));
        Ok(Segment {
            segment,
            offset: 0,
            position: 0,
            index_table: vec![],
            index: BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path.with_extension("index"))
                    .await?,
            ),
            log: BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path.with_extension("log"))
                    .await?,
            ),
        })
    }

    pub(crate) async fn open(partition: impl AsRef<Path>, segment: usize) -> Result<Self> {
        let mut path = partition.as_ref().to_owned();
        path.push(format!("{:020}", segment));
        let index_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.with_extension("index"))
            .await?;
        let index_len = index_file.metadata().await?.len();
        let mut index = BufWriter::new(index_file);
        let index_table = Segment::load_index_table(index.borrow_mut(), index_len).await?;
        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.with_extension("log"))
            .await?;
        let position = log_file.metadata().await?.len() as u32;
        Ok(Segment {
            segment,
            offset: index_table.len() as u32,
            position,
            index_table,
            index,
            log: BufWriter::new(log_file),
        })
    }

    async fn load_index_table(file: &mut BufWriter<File>, len: u64) -> Result<Vec<u32>> {
        let mut table = Vec::with_capacity((len / 8) as usize);
        //println!("1index table: {:?} metadata: {:?}", table, file);
        while let Ok(offset) = file.read_u32().await {
            //  println!("2 offset: {:}", offset);
            if let Ok(position) = file.read_u32().await {
                // println!("2 position: {:}", position);
                table.insert(offset as usize, position);
            } else {
                return Err(anyhow!("wrong read of index file"));
            }
        }
        // println!("2index table: {:?}", table);
        Ok(table)
    }

    async fn push(&mut self, msg: Message) -> Result<usize> {
        let size = 16;
        self.index.write_u32(self.offset).await?;
        self.index.write_u32(self.position).await?;
        self.log.write_u32(self.offset).await?;
        self.log.write_u32(self.position).await?;
        self.log
            .write_u32(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32)
            .await?;
        self.log.write_u8(msg.retain as u8).await?;
        self.log.write_u8(msg.qos.into()).await?;
        //self.log.write_u16(msg.topic_name.len() as u16).await?;
        self.log.write_all(msg.topic_name.as_ref()).await?;
        self.log.write_all(msg.payload.as_ref()).await?;
        self.index.flush().await?;
        self.log.flush().await?;
        self.offset = self.offset + 1;
        self.position = size as u32;
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use bytes::Bytes;

    use crate::mqtt::proto::types::QoS;

    use super::*;

    //#[test]
    fn test_new() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut segment = Segment::new("/tmp/topic", 323).await.unwrap();
            let w = segment
                .push(Message {
                    ts: Instant::now(),
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic_name: Bytes::from("1234567890"),
                    properties: Default::default(),
                    payload: Bytes::from("Default::default()"),
                })
                .await
                .unwrap();
            let w = segment
                .push(Message {
                    ts: Instant::now(),
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic_name: Bytes::from("11111"),
                    properties: Default::default(),
                    payload: Bytes::from("Default::default()"),
                })
                .await
                .unwrap();
            ()
        })
    }

    #[test]
    fn test_open() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut segment = Segment::open("/tmp/topic", 323).await.unwrap();
            println!("index table len: {}", segment.index_table.len());
            println!("index table: {:?}", segment.index_table);
            println!("offset: {}, position: {}", segment.offset, segment.position);
            segment
                .push(Message {
                    ts: Instant::now(),
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic_name: Bytes::from("topic3"),
                    properties: Default::default(),
                    payload: Bytes::from("data3"),
                })
                .await
                .unwrap();
            ()
        })
    }
}
