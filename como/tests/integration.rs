#[macro_use]
extern crate claim;

use std::sync::{Arc, Once};

use anyhow::{Error, Result};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;

use como::service;
use como::settings::Settings;
use como_mqtt::client::MqttClient;
use como_mqtt::v5::types::{ControlPacket, QoS, ReasonCode};

static ONCE: Once = Once::new();

async fn start_test_broker() -> (u16, Sender<()>, JoinHandle<Result<(), Error>>) {
    ONCE.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    let port = rand::random::<u16>();
    let (shutdown_notify, shutdown) = oneshot::channel::<()>();
    let barrier = Arc::new(Barrier::new(2));

    let ready = barrier.clone();
    let handle = tokio::spawn(async move {
        let mut settings = Settings::default();
        settings.service.port = port;

        service::run_with_ready(Arc::new(settings), shutdown, ready).await
    });
    barrier.wait().await;
    (port, shutdown_notify, handle)
}

#[tokio::test]
async fn connect_clean_session() -> anyhow::Result<()> {
    //  println!("connect_clean_session start");
    let (port, shutdown_notify, handle) = start_test_broker().await;
    // println!("connect_clean_session end");

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    assert!(!ack.session_present);
    assert_some!(ack.properties.assigned_client_identifier);
    assert_none!(ack.properties.session_expire_interval);
    assert_none!(ack.properties.receive_maximum);
    assert_none!(ack.properties.maximum_qos);
    assert_none!(ack.properties.retain_available);
    assert_none!(ack.properties.maximum_packet_size);
    assert_none!(ack.properties.topic_alias_maximum);
    assert_none!(ack.properties.reason_string);
    assert_none!(ack.properties.wildcard_subscription_available);
    assert_none!(ack.properties.subscription_identifier_available);
    assert_none!(ack.properties.shared_subscription_available);
    assert_none!(ack.properties.server_keep_alive);
    assert_none!(ack.properties.response_information);
    assert_none!(ack.properties.server_reference);
    assert_none!(ack.properties.authentication_method);
    assert_none!(ack.properties.authentication_data);

    assert_none!(client.disconnect().await?);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn connect_existing_session() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;
    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("connect_existing_session")
        .session_expire_interval(10)
        .build()
        .await?;

    // Initial #1 connection
    let ack = assert_ok!(client.connect(false).await);
    assert!(!ack.session_present);
    assert_none!(ack.properties.assigned_client_identifier);
    assert_none!(ack.properties.session_expire_interval);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("connect_existing_session")
        .build()
        .await?;
    // #2 connection take over the session from #1
    let ack = assert_ok!(client2.connect(true).await);
    assert!(ack.session_present);
    assert_none!(ack.properties.assigned_client_identifier);
    assert_none!(ack.properties.session_expire_interval);
    // expected #1 disconnect
    assert_matches!(assert_ok!(client.recv().await), ControlPacket::Disconnect(d) if d.reason_code == ReasonCode::SessionTakenOver);
    assert_none!(client2.disconnect().await?);

    // #3 connection
    let mut client3 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("connect_existing_session")
        .session_expire_interval(10)
        .build()
        .await?;
    let ack = assert_ok!(client3.connect(false).await);
    assert!(!ack.session_present);

    assert_none!(client3.disconnect().await?);

    let mut client4 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("connect_existing_session")
        .session_expire_interval(10)
        .build()
        .await?;
    // #4 connection after #3 disconnect with 10 sec session_expire
    let ack = assert_ok!(client4.connect(false).await);
    assert!(ack.session_present);

    assert_none!(client4.disconnect().await?);

    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn publish_subscribe() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;
    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .session_expire_interval(10)
        .build()
        .await?;

    let ack = assert_ok!(client.connect(false).await);
    assert!(!ack.session_present);
    assert_some!(ack.properties.assigned_client_identifier);

    let ack = assert_ok!(
        client
            .subscribe(QoS::AtMostOnce, String::from("topic/A"))
            .await
    );
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    assert_ok!(
        client
            .publish_most_once(false, String::from("topic/A"), Vec::from("payload01"))
            .await
    );

    assert_ok!(
        client
            .publish_most_once(false, String::from("topic/A"), Vec::from("payload02"))
            .await
    );

    let msg = assert_ok!(client.recv().await);
    assert_matches!(msg, ControlPacket::Publish(_));

    assert_none!(client.disconnect().await?);
    drop(shutdown_notify);
    handle.await?
}
