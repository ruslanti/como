use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{ensure, Context, Result};
use tokio::fs;
use tokio::sync::RwLock;

use crate::mqtt::segment::Segment;

pub struct Record {}

struct Partition {
    start: usize,
    segment_size: usize,
    segments: Vec<RwLock<Segment>>,
}

impl Partition {
    async fn load(segment_size: usize) -> Self {
        Partition {
            start: 0,
            segment_size,
            segments: vec![],
        }
    }

    async fn store(&mut self, record: Record) -> Result<usize> {
        let mut segment = self
            .segments
            .last()
            .context("no data segment")?
            .write()
            .await;
        if segment.is_full() {
            self.segments.push(RwLock::new(Segment::new(0)));
            segment = self.segments.last().unwrap().write().await;
        }
        segment.store(record).await
    }

    async fn get(&self, pos: usize) -> Result<Record> {
        ensure!(self.start < pos, "invalid segment position");
        let index: usize = pos - self.start;
        let segment = self
            .segments
            .get(index / self.segment_size)
            .context("unknown segment")?;

        segment.read().await.load(index % self.segment_size).await
    }
}
