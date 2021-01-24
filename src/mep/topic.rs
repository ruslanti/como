use std::borrow::Borrow;
use std::collections::HashMap;

use anyhow::{anyhow, Error, Result};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};
use tracing::{info, instrument, trace, warn};

use crate::mep::topic::TopicCommand::{Get, Push};
use crate::mep::Root;

#[derive(Debug)]
pub(crate) struct Message {}

#[derive(Debug)]
pub(crate) enum TopicCommand {
    Push(Message, Option<oneshot::Sender<()>>),
    Get(usize, mpsc::Sender<Message>),
}

pub(crate) type PublishSender = UnboundedSender<TopicCommand>;
type PublishReceiver = UnboundedReceiver<TopicCommand>;

pub struct Publisher {
    tx: PublishSender,
}

pub(in crate::mep) struct Topic {
    pub(crate) path: String,
    pub(crate) tx: PublishSender,
    pub(crate) notify: watch::Receiver<usize>,
    pub(crate) siblings: HashMap<String, Self>,
}

impl From<&Topic> for Publisher {
    fn from(topic: &Topic) -> Self {
        Self {
            tx: topic.tx.clone(),
        }
    }
}

impl Publisher {
    async fn publish(&self, msg: Message) -> Result<()> {
        self.tx.send(Push(msg, None)).map_err(Error::msg)
    }

    async fn publish_confirm(&self, msg: Message) -> Result<()> {
        let (confirm_tx, confirm_rx) = oneshot::channel();
        self.tx
            .send(Push(msg, Some(confirm_tx)))
            .map_err(Error::msg)?;
        confirm_rx.await.map_err(Error::msg)
    }
}

impl Topic {
    pub(crate) fn new(name: &str) -> Self {
        info!("create new topic \"{}\"", name);
        let (tx, rx) = mpsc::unbounded_channel();
        let (notify_tx, notify) = watch::channel(0usize);
        let path = name.to_owned();
        tokio::spawn(async move {
            Self::process_topic(path.to_owned(), rx, notify_tx).await;
        });
        Self {
            path: name.to_owned(),
            tx,
            notify,
            siblings: HashMap::new(),
        }
    }

    #[instrument(skip(rx))]
    async fn process_topic(path: String, mut rx: PublishReceiver, notify_tx: watch::Sender<usize>) {
        trace!("start");
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Push(msg, confirm_tx) => {
                    trace!("{:?}", msg);
                    //TODO append to log
                    if let Err(e) = notify_tx.send(0) {
                        warn!(cause = ?e, "topic notify error");
                    }
                }
                Get(index, response_tx) => {
                    // read log index
                }
            }
        }
        trace!("stop");
    }

    #[instrument(skip(self, parent, new_topic_fn))]
    pub(crate) fn get<F>(
        &mut self,
        parent: Root,
        topic_name: &str,
        new_topic_fn: F,
    ) -> Result<Publisher>
    where
        F: Fn(String, Publisher),
    {
        if topic_name.strip_prefix('$').is_none() {
            let mut s = topic_name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    let path = Topic::path(parent, prefix);
                    let topic = self.siblings.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.as_str());
                        new_topic_fn(path.to_owned(), Publisher::from(topic.borrow()));
                        topic
                    });
                    match s.next() {
                        Some(suffix) => topic.get(Root::Name(path.as_str()), suffix, new_topic_fn),
                        None => Ok(Publisher::from(topic.borrow())),
                    }
                }
                None => Ok(Publisher::from(self.borrow())),
            }
        } else {
            Err(anyhow!("Invalid leading $ character"))
        }
    }

    pub(crate) fn path(parent: Root, name: &str) -> String {
        match parent {
            Root::Name(parent) => {
                let mut path = parent.to_string();
                path.push('/');
                path.push_str(name);
                path
            }
            Root::Dollar => {
                let mut path = "$".to_string();
                path.push_str(name);
                path
            }
            Root::None => name.to_string(),
        }
    }
}
