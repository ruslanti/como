use std::borrow::{Borrow, BorrowMut};
use std::cmp::min;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, field, instrument, trace};
use uuid::Uuid;

use como_mqtt::v5::types::{Auth, Connect, ControlPacket, Disconnect, MqttCodec, ReasonCode};

use crate::context::SessionContext;
use crate::metric;
use crate::session::Session;
use crate::shutdown::Shutdown;

pub struct ConnectionHandler {
    proto: &'static str,
    pub(crate) peer: SocketAddr,
    limit_connections: Arc<Semaphore>,
    _shutdown_complete: mpsc::Sender<()>,
    keep_alive: Duration,
    context: SessionContext,
    session: Option<Session>,
}

impl ConnectionHandler {
    pub(crate) fn new(
        proto: &'static str,
        peer: SocketAddr,
        limit_connections: Arc<Semaphore>,
        shutdown_complete: mpsc::Sender<()>,
        context: SessionContext,
    ) -> Self {
        metric::ACTIVE_CONNECTIONS
            .with_label_values(&[proto, peer.ip().to_string().as_str()])
            .inc();
        ConnectionHandler {
            proto,
            peer,
            limit_connections,
            _shutdown_complete: shutdown_complete,
            keep_alive: Duration::from_millis(context.settings().connection.idle_keep_alive as u64),
            context,
            session: None,
        }
    }

    //#[instrument(skip(self, conn_tx, msg), err)]
    async fn prepare_session(
        &mut self,
        msg: &Connect,
        response_tx: Sender<ControlPacket>,
    ) -> Result<Session> {
        //trace!("{:?}", msg);
        let identifier = msg
            .client_identifier
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_simple().to_string().into());

        // If the Server sends a Server Keep Alive on the CONNACK packet, the Client MUST
        // use this value instead of the Keep Alive value the Client sent on CONNECT
        // [MQTT-3.2.2-21]. If the Server does not send the Server Keep Alive, the Server
        // MUST use the Keep Alive value set by the Client on CONNECT
        let client_keep_alive = (msg.keep_alive != 0).then(|| msg.keep_alive);
        let keep_alive =
            self.context
                .settings()
                .connection
                .server_keep_alive
                .map(|server_keep_alive| {
                    client_keep_alive.map_or(server_keep_alive, |client_keep_alive| {
                        min(server_keep_alive, client_keep_alive)
                    })
                });

        // within one and a half times the Keep Alive time period
        self.keep_alive = keep_alive
            .map(|d| Duration::from_secs(d as u64).mul_f32(1.5))
            .unwrap_or_else(|| Duration::from_micros(u64::MAX));
        trace!("keep_alive: {:?}", self.keep_alive);

        let session = Session::new(
            identifier.try_into()?,
            response_tx,
            self.peer,
            msg.properties.clone(),
            msg.will.clone(),
            self.context.clone(),
        );

        Ok(session)
    }

    //#[instrument(skip(self, response_tx, subscription_tx), err)]
    async fn recv(
        &mut self,
        msg: ControlPacket,
        response_tx: &Sender<ControlPacket>,
    ) -> Result<Option<ControlPacket>> {
        //trace!("{:?}", packet);
        let labels = [msg.borrow().into()];
        let _time = metric::RESPONSE_TIME
            .with_label_values(&labels)
            .start_timer();

        metric::PACKETS_RECEIVED.with_label_values(&labels).inc();

        match (self.session.borrow_mut(), msg) {
            (None, ControlPacket::Connect(connect)) => {
                let session = self.prepare_session(&connect, response_tx.clone()).await?;
                self.session
                    .get_or_insert(session)
                    .handle_msg(ControlPacket::Connect(connect))
                    .await
            }
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some(_), ControlPacket::Connect(_)) => todo!(
                "The Server MUST process a second \
            CONNECT packet sent from a Client as a Protocol Error and close the Network Connection"
            ),
            (Some(_), ControlPacket::PingReq) => Ok(Some(ControlPacket::PingResp)),
            (Some(session), ControlPacket::Disconnect(disconnect)) => {
                session
                    .handle_msg(ControlPacket::Disconnect(disconnect))
                    .await?;
                self.keep_alive = Duration::from_millis(0); // close socket and session
                Ok(None)
            }
            (Some(session), msg) => session.handle_msg(msg).await,
        }
    }

    async fn session(&mut self) -> Result<Option<ControlPacket>> {
        match self.session.borrow_mut() {
            None => Err(anyhow!("unacceptable event").context("subscription event")),
            Some(session) => session.session().await,
        }
    }

    async fn _process_auth(&self, msg: Auth) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, socket, shutdown), fields(peer = field::display(& self.peer)))]
    pub async fn client<S>(&mut self, socket: S, mut shutdown: Shutdown) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let framed = Framed::new(
            socket,
            MqttCodec::new(self.context.settings().connection.maximum_packet_size),
        );
        let (mut sink, mut stream) = framed.split::<ControlPacket>();
        let (response_tx, mut response_rx) = mpsc::channel::<ControlPacket>(32);

        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            //trace!("select with timeout {} ms", duration.as_millis());
            tokio::select! {
                res = timeout(self.keep_alive, stream.next()) => {
                    match res {
                        Ok(res) => {
                            if let Some(res) = res {
                                let p = res?;
                                debug!("received {:?}", p);

                                if let Some(msg) = self.recv(p, response_tx.borrow()).await? {
                                    send(sink.borrow_mut(), msg).await?;
                                }
                            } else {
                                debug!("Client disconnected");
                                break;
                            }
                        },
                        Err(_) if self.keep_alive.as_micros() == 0 => break,
                        Err(elapsed) => return Err(anyhow!(elapsed))
                    }
                }
                res = response_rx.recv() => {
                    if let Some(msg) = res {
                        send(sink.borrow_mut(), msg).await?;
                    } else {
                        debug!("None response. Disconnected");
                        break;
                    }
                }
                res = self.session(), if self.session.is_some() => {
                    trace!("session event: {:?}", res);
                    if let Some(msg) = res? {
                        if let ControlPacket::Disconnect(_) = msg {
                            self.keep_alive = Duration::from_millis(0); // close socket and session
                        }
                        send(sink.borrow_mut(), msg).await?;
                    }
                }
                _ = shutdown.recv() => {
                    debug!("server shutting down");
                    if let Some(mut session) = self.session.take() {
                        session.close_immediately().await;
                    }
                    let msg = ControlPacket::Disconnect(Disconnect {reason_code:
                    ReasonCode::ServerShuttingDown, properties: Default::default()});
                    send(sink.borrow_mut(), msg).await?;
                    break;
                }
            }
        }
        if let Some(mut session) = self.session.take() {
            tokio::spawn(async move { session.close().await });
        }
        Ok(())
    }
}

#[instrument(skip(sink))]
async fn send<S>(
    sink: &mut SplitSink<Framed<S, MqttCodec>, ControlPacket>,
    msg: ControlPacket,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    debug!("sending");
    metric::PACKETS_SENT
        .with_label_values(&[msg.borrow().into()])
        .inc();
    sink.send(msg).await.context("socket send error")
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        metric::ACTIVE_CONNECTIONS
            .with_label_values(&[self.proto, self.peer.ip().to_string().as_str()])
            .dec();
        self.limit_connections.add_permits(1);
    }
}
