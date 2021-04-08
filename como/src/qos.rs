use std::cmp::min;

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{instrument, trace, warn};

use como_mqtt::v5::property::PublishProperties;
use como_mqtt::v5::string::MqttString;
use como_mqtt::v5::types::{ControlPacket, Publish, PublishResponse, QoS, ReasonCode};

use crate::session::QosEvent::{End, Response};
use crate::session::{PublishEvent, SessionEvent, SubscriptionMessage};

#[macro_use]
macro_rules! wait_response {
    ($rx: ident, $expected: ident, $packet_identifier: ident) => {
        match timeout(Duration::from_millis(500), $rx.recv()).await {
            Ok(Some(PublishEvent::$expected(m))) => {
                trace!(
                    "Received {}. End state for packet_identifier {} ",
                    m,
                    $packet_identifier
                );
                true
            }
            Ok(Some(m)) => {
                warn!("Invalid event: {:?}", m);
                true
            }
            Ok(None) => {
                trace!("No response");
                true
            }
            Err(_elapsed) => {
                trace!("Elapsed. Retransmit previous message");
                false
            }
        };
    };
}

#[instrument(skip(rx, session_event_tx), err)]
pub async fn qos_client(
    _client_id: String,
    event: SubscriptionMessage,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    session_event_tx: Sender<SessionEvent>,
) -> Result<()> {
    let msg = event.msg;
    let qos = min(msg.qos, event.option.qos);
    assert_ne!(qos, QoS::AtMostOnce);
    /*    let _semaphore = client_receive_maximum.acquire().await?;*/
    let topic_name = MqttString::from(event.topic_name);
    let payload = Bytes::from(msg.payload);
    let mut retry: usize = 0;

    loop {
        //todo make it configurable
        if retry > 10 {
            warn!("Send publish failed. Reached retry limit {}.", retry);
            break;
        }

        session_event_tx
            .send(SessionEvent::QosEvent(Response(ControlPacket::Publish(
                Publish {
                    dup: retry != 0,
                    qos,
                    retain: msg.retain,
                    topic_name: topic_name.clone(),
                    packet_identifier: Some(packet_identifier),
                    properties: PublishProperties::default(),
                    payload: payload.clone(),
                },
            ))))
            .await?;
        retry += 1;

        if QoS::AtLeastOnce == qos {
            if wait_response!(rx, Ack, packet_identifier) {
                // ACK response received
                break;
            }
        } else if wait_response!(rx, Rec, packet_identifier) {
            // REC response received
            loop {
                if retry > 10 {
                    warn!("Send PUBREL failed. Reached retry limit {}.", retry);
                    break;
                }

                session_event_tx
                    .send(SessionEvent::QosEvent(Response(ControlPacket::PubRel(
                        PublishResponse {
                            packet_identifier,
                            reason_code: ReasonCode::Success,
                            properties: Default::default(),
                        },
                    ))))
                    .await?;
                retry += 1;

                if wait_response!(rx, Comp, packet_identifier) {
                    // COMP response received
                    break;
                }
            }
            // end fsm
            break;
        };
    }

    session_event_tx
        .send(SessionEvent::QosEvent(End(packet_identifier)))
        .await?;
    Ok(())
}

#[instrument(skip(rx, response_tx), err)]
pub async fn exactly_once_server(
    client_id: String,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
    match rx.recv().await {
        Some(PublishEvent::Rel(rel)) => {
            trace!("{:?}", rel);
            //TODO discard packet identifier
            let comp = ControlPacket::PubComp(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            response_tx.send(comp).await?;
        }
        Some(event) => bail!("{:?} unknown event received: {:?}", client_id, event),
        None => bail!("{:?} channel closed", client_id),
    }
    Ok(())
}
