use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{trace, debug, error, instrument};
use anyhow::{anyhow, Result, Context};
use crate::mqtt::shutdown::Shutdown;

trait Publish {
    fn publish();
}

struct Topic{

}

#[derive(Debug)]
pub(crate) struct Message {
    retain: bool,
    topic: Bytes,
    payload: Bytes
}

pub(crate) struct TopicManager {
    publish_rx: mpsc::Receiver<(Message)>
}

impl Message {
    pub fn new(retain: bool, topic: Bytes, payload: Bytes) -> Self {
        Message { retain, topic, payload }
    }
}

impl TopicManager {
    pub fn new(publish_rx: mpsc::Receiver<(Message)>) -> Self {
        TopicManager {
            publish_rx
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        trace!("run start");
        while let Some(msg) = self.publish_rx.recv().await {
            debug!("MSG {:?}", msg)
        };
        trace!("run end");
        Ok(())
    }
}