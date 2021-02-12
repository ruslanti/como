use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, field, instrument, trace, warn};

use crate::mqtt::proto::types::SubscriptionOptions;
use crate::mqtt::topic::{TopicMessage, TopicReceiver};

pub(crate) type SessionSender = mpsc::Sender<(String, SubscriptionOptions, TopicMessage)>;

#[derive(Debug)]
pub(crate) struct Subscription {
    session: String,
    option: SubscriptionOptions,
    session_tx: SessionSender,
}

impl Subscription {
    pub(crate) fn new(
        session: String,
        option: SubscriptionOptions,
        session_tx: SessionSender,
    ) -> Self {
        Self {
            session,
            option,
            session_tx,
        }
    }

    #[instrument(skip(self, stream, unsubscribe_rx), fields(session = field::display(& self.session)))]
    pub(crate) async fn subscribe(
        &self,
        topic: &str,
        stream: TopicReceiver,
        unsubscribe_rx: oneshot::Receiver<()>,
    ) {
        //   tokio::pin!(stream);
        trace!("start");

        tokio::select! {
            Err(err) = self.subscription_stream(topic, stream) => {
                warn!(cause = ?err, "subscription error");
            },
            _ = unsubscribe_rx => {
                debug!("unsubscribe");
            }
        }
        trace!("end");
    }

    async fn subscription_stream(&self, topic: &str, mut stream: TopicReceiver) -> Result<()> {
        while stream.changed().await.is_ok() {
            let event = stream.borrow().clone();
            match event {
                Some(msg) => {
                    let ret_msg = (topic.to_owned(), self.option.to_owned(), msg.to_owned());
                    trace!("subscription message {:?}", ret_msg);
                    self.session_tx.send(ret_msg).await?;
                }
                None => {
                    debug!("empty topic message");
                }
            }
        }
        Ok(())
    }
}
