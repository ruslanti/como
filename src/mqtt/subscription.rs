use tokio::stream::{Stream, StreamExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, field, instrument, warn};

use crate::mqtt::proto::types::SubscriptionOptions;
use crate::mqtt::topic::Message;

#[derive(Debug)]
pub(crate) struct Subscription {
    session: String,
    option: SubscriptionOptions,
    session_tx: mpsc::Sender<(String, SubscriptionOptions, Message)>,
    unsubscribe_rx: oneshot::Receiver<()>,
}

impl Subscription {
    pub(crate) fn new(
        session: String,
        option: SubscriptionOptions,
        session_tx: mpsc::Sender<(String, SubscriptionOptions, Message)>,
        unsubscribe_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            session,
            option,
            session_tx,
            unsubscribe_rx,
        }
    }

    async fn unsubscribe(&self) {
        let _ = self.unsubscribe_rx;
    }

    #[instrument(skip(self, stream), fields(session = field::display(& self.session)))]
    pub(crate) async fn subscribe<S>(&self, topic: String, stream: S)
    where
        S: Stream<Item = Result<Message, RecvError>>,
    {
        tokio::pin!(stream);
        loop {
            tokio::select! {
                Some(event) = stream.next() => {
                    match event {
                        Ok(msg) => {
                            let ret_msg = (topic.to_owned(), self.option.to_owned(), msg);
                            if let Err(err) = self.session_tx.send(ret_msg).await {
                                warn!(cause = ?err, "subscription '{}' error: ", topic);
                                break;
                            }
                        }
                        Err(Lagged(lag)) => {
                            warn!("subscription '{}' lagged: {}", topic, lag);
                        }
                        Err(err) => {
                            warn!(cause = ?err, "subscription '{}' error: ", topic);
                            break;
                        }
                    }
                }
                _ = self.unsubscribe() => {
                    debug!("unsubscribe '{}'", topic);
                    break;
                }
            }
        }
    }
}
