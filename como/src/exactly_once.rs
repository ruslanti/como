use anyhow::{bail, Result};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{instrument, trace};

use como_mqtt::v5::types::{ControlPacket, PublishResponse, ReasonCode};

use crate::session::PublishEvent;

#[instrument(skip(rx, response_tx), err)]
pub async fn exactly_once_client(
    session: String,
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

#[instrument(skip(rx, response_tx), err)]
pub async fn exactly_once_server(
    client_id: String,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
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
        Some(event) => bail!("{:?} unknown event received: {:?}", client_id, event),
        None => bail!("{:?} channel closed", client_id),
    }
    Ok(())
}
