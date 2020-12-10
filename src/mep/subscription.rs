use tokio::sync::watch;

use crate::mep::topic::{PublishSender, Topic};

pub(in crate::mep) struct Subscription {
    path: String,
    tx: PublishSender,
    notify: watch::Receiver<usize>,
}

impl From<&Topic> for Subscription {
    fn from(topic: &Topic) -> Self {
        Self {
            path: topic.path.to_owned(),
            tx: topic.tx.clone(),
            notify: topic.notify.clone(),
        }
    }
}

impl Subscription {
    async fn spawn(&mut self) {
        while let Ok(r) = self.notify.changed().await {
            let index = *self.notify.borrow();
        }
    }
}
