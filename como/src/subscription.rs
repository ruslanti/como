use std::borrow::Borrow;
use std::cmp::min;
use std::sync::Arc;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use sled::{Event, Subscriber};
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Semaphore};
use tracing::{debug, instrument, trace, warn};

use como_mqtt::v5::types::{QoS, SubscriptionOptions};

use crate::session::{SessionEvent, SubscriptionMessage};
use crate::topic::PubMessage;

#[instrument(skip(option, subscriber, session_event_tx))]
async fn subscription(
    option: SubscriptionOptions,
    session_event_tx: Sender<SessionEvent>,
    topic_name: String,
    mut subscriber: Subscriber,
    limit_client_publish: Arc<Semaphore>,
) -> anyhow::Result<()> {
    //   tokio::pin!(stream);
    trace!("start");
    while let Some(event) = (&mut subscriber).await {
        //debug!("receive subscription event {:?}", event);
        match event {
            Event::Insert { key, value } => match key.as_ref().read_u64::<BigEndian>() {
                Ok(id) => {
                    //debug!("id: {}, value: {:?}", id, value);
                    let msg: PubMessage =
                        bincode::deserialize(value.as_ref()).context("deserialize event")?;
                    let qos = min(msg.qos, option.qos);
                    limit_client(qos, limit_client_publish.borrow()).await;
                    session_event_tx
                        .send(SessionEvent::SubscriptionEvent(SubscriptionMessage::new(
                            id,
                            topic_name.to_owned(),
                            option.clone(),
                            msg,
                        )))
                        .await
                        .context("subscription send")?
                }
                Err(_) => warn!("invalid log id: {:#x?}", key),
            },
            Event::Remove { .. } => unreachable!(),
        }
    }

    trace!("end");
    Ok(())
}

pub(crate) async fn limit_client(qos: QoS, limit_client_publish: &Arc<Semaphore>) {
    if qos != QoS::AtMostOnce {
        if let Err(err) = limit_client_publish
            .acquire()
            .await
            .map(|permit| permit.forget())
        {
            warn!(cause = ?err, "limit client publish")
        }
    }
}

pub(crate) fn subscribe_topic(
    client_id: String,
    topic_name: String,
    options: SubscriptionOptions,
    subscription_tx: Sender<SessionEvent>,
    subscriber: Subscriber,
    limit_client_publish: Arc<Semaphore>,
) -> oneshot::Sender<()> {
    let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::select! {
            Err(err) = subscription(options, subscription_tx, topic_name.to_owned(), subscriber, limit_client_publish)
            => {
                warn!(cause = ? err, "subscription {} error", topic_name);
            },
            _ = unsubscribe_rx => {
                debug!("{:?} unsubscribe {}", client_id, topic_name);
            }
        }
    });
    unsubscribe_tx
}
