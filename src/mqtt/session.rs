use std::io;
use std::borrow::BorrowMut;
use std::convert::TryInto;
use bytes::BytesMut;
use anyhow::{anyhow, Result};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{trace, debug, error, info, instrument};
use tokio::stream::StreamExt;
use futures::sink::SinkExt;
use crate::mqtt::proto::types::{MQTTCodec, ControlPacket, ReasonCode};
use crate::mqtt::proto::types::ControlPacket::ConnAck;
use crate::mqtt::proto::property::ConnAckProperties;

#[derive(Debug)]
pub struct Session {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut
}

impl Session {
    pub fn new(socket: TcpStream) -> Session {
        Session {
            stream: tokio::io::BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }


    pub async fn process(&mut self) -> Result<()> {
        let mut transport = Framed::new(&mut self.stream, MQTTCodec::new());
        while let Some(packet) = transport.next().await {
            match packet {
                Ok(packet) => {
                    trace!("frame: {:?}", packet);
                    let ack = ControlPacket::ConnAck {
                        session_present: false,
                        reason_code: ReasonCode::Success,
                        properties: ConnAckProperties{
                            session_expire_interval: None,
                            receive_maximum: None,
                            maximum_qos: None,
                            retain_available: None,
                            maximum_packet_size: None,
                            assigned_client_identifier: None,
                            topic_alias_maximum: None,
                            reason_string: None,
                            user_properties: vec![]
                        }
                    };
                    transport.send(ack).await?;
                }
                Err(e) => {
                    error!("error on process session: {}", e);
                    return Err(e.into())
                },
            }
        }
/*
        let mut transport = Framed::new(&mut self.stream, Mqtt);

        while let Some(request) = transport.next().await {
            match request {
                Ok(request) => {
                }
                Err(e) => return Err(e.into()),
            }
        }
*/
        Ok(())
    }

    pub async fn read_packet(&mut self) -> Result<()> {
        loop {
            match self.stream.read_buf(&mut self.buffer).await? {
                0 => {
                    if self.buffer.is_empty() {
                        return Ok(());
                    } else {
                        return Err(anyhow!("connection reset by peer"));
                    }
                },
                n => {
                    debug!("read {} bytes", n);
                }
            }
        }
    }
}
