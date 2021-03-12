use std::sync::Arc;

use anyhow::{bail, Result};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{instrument, trace};

use como_mqtt::v5::types::{ControlPacket, MqttString, PublishResponse, ReasonCode};

use crate::session::PublishEvent;
use crate::topic::Topics;

#[instrument(skip(rx, response_tx), err)]
pub async fn exactly_once_client(
    session: MqttString,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::PubRec(_msg) = event {
            let rel = ControlPacket::PubRel(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            response_tx.send(rel).await?;

            match rx.recv().await {
                Some(PublishEvent::PubComp(comp)) => {
                    trace!("{:?}", comp);
                }
                Some(event) => bail!("{:?} unknown event received: {:?}", session, event),
                None => bail!("{:?} channel closed", session),
            }
        } else {
            bail!("{:?} unexpected pubrec event {:?}", session, event);
        }
    }
    Ok(())
}

#[instrument(skip(rx, response_tx, root), err)]
pub async fn exactly_once_server(
    session: MqttString,
    packet_identifier: u16,
    root: Arc<Topics>,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::Publish(msg) = event {
            //TODO handler error
            root.publish(msg).await?;
            let rec = ControlPacket::PubRec(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            response_tx.send(rec).await?;

            match rx.recv().await {
                Some(PublishEvent::PubRel(rel)) => {
                    trace!("{:?}", rel);
                    //TODO discard packet identifier
                    let comp = ControlPacket::PubComp(PublishResponse {
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: Default::default(),
                    });
                    response_tx.send(comp).await?;
                }
                Some(event) => bail!("{:?} unknown event received: {:?}", session, event),
                None => bail!("{:?} channel closed", session),
            }
        } else {
            bail!("{:?} unexpected publish event {:?}", session, event);
        }
    }
    Ok(())
}
