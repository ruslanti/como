use anyhow::{bail, Result};
use futures::{Stream, StreamExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, field, instrument, trace, warn};

use crate::mqtt::proto::types::SubscriptionOptions;
use crate::mqtt::topic::TopicMessage;

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
    pub(crate) async fn subscribe<S>(
        &self,
        topic: String,
        stream: S,
        unsubscribe_rx: oneshot::Receiver<()>,
    ) where
        S: Stream<Item = Result<TopicMessage, RecvError>>,
    {
        tokio::pin!(stream);
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

    async fn subscription_stream<S>(&self, topic: String, mut stream: S) -> Result<()>
    where
        S: Stream<Item = Result<TopicMessage, RecvError>> + Unpin,
    {
        while let Some(event) = stream.next().await {
            match event {
                Ok(msg) => {
                    let ret_msg = (topic.to_owned(), self.option.to_owned(), msg);
                    trace!("subscription message {:?}", ret_msg);
                    self.session_tx.send(ret_msg).await?;
                }
                Err(Lagged(lag)) => {
                    warn!("subscription '{}' lagged: {}", topic, lag);
                }
                Err(err) => {
                    bail!(err);
                    //return Err(anyhow!(err));
                }
            }
        }
        Ok(())
    }
}
