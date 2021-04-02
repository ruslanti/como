#[macro_use]
extern crate claim;

use std::time::Duration;

use bytes::Bytes;

use como_mqtt::client::MqttClient;
use como_mqtt::v5::property::WillProperties;
use como_mqtt::v5::string::MqttString;
use como_mqtt::v5::types::{ControlPacket, Disconnect, Publish, QoS, ReasonCode, Will};

use crate::common::start_test_broker;

mod common;

#[tokio::test]
async fn will_message_close() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .session_expire_interval(2)
        .with_will(Will {
            qos: QoS::AtMostOnce,
            retain: false,
            properties: WillProperties {
                will_delay_interval: 1,
                payload_format_indicator: None,
                message_expire_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            },
            topic: MqttString::from("topic/will"),
            payload: Bytes::from("WILL"),
        })
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    assert!(!ack.session_present);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_timeout(Duration::from_secs(2))
        .build()
        .await?;
    // #2 connection take over the session from #1
    assert_ok!(client2.connect(true).await);
    let ack = assert_ok!(client2.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    //assert_none!(client.disconnect().await?);
    drop(client);
    /*assert_ok!(client2.recv().await);*/
    assert_matches!(assert_ok!(client2.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic_name: MqttString::from("topic/will"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("WILL"),
        }
    );
    assert_none!(client2.disconnect().await?);

    drop(client2);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn will_message_disconnect_failure() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_will(Will {
            qos: QoS::AtMostOnce,
            retain: false,
            properties: WillProperties {
                will_delay_interval: 0,
                payload_format_indicator: None,
                message_expire_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            },
            topic: MqttString::from("topic/will"),
            payload: Bytes::from("WILL"),
        })
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    assert!(!ack.session_present);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_timeout(Duration::from_secs(2))
        .build()
        .await?;

    assert_ok!(client2.connect(true).await);
    let ack = assert_ok!(client2.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    assert_none!(
        client
            .disconnect_with_reason(ReasonCode::DisconnectWithWill)
            .await?
    );
    /*assert_ok!(client2.recv().await);*/
    assert_matches!(assert_ok!(client2.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic_name: MqttString::from("topic/will"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("WILL"),
        }
    );
    assert_none!(client2.disconnect().await?);

    drop(client2);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn will_message_disconnect_success() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_will(Will {
            qos: QoS::AtMostOnce,
            retain: false,
            properties: WillProperties {
                will_delay_interval: 0,
                payload_format_indicator: None,
                message_expire_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            },
            topic: MqttString::from("topic/will"),
            payload: Bytes::from("WILL"),
        })
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    assert!(!ack.session_present);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_timeout(Duration::from_secs(1))
        .build()
        .await?;
    // #2 connection take over the session from #1
    assert_ok!(client2.connect(true).await);
    let ack = assert_ok!(client2.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    assert_none!(client.disconnect().await?);
    assert_none!(client2.disconnect().await?);

    drop(client2);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn broker_shutdown() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .session_expire_interval(2)
        .with_will(Will {
            qos: QoS::AtMostOnce,
            retain: false,
            properties: WillProperties {
                will_delay_interval: 1,
                payload_format_indicator: None,
                message_expire_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: vec![],
            },
            topic: MqttString::from("topic/will"),
            payload: Bytes::from("WILL"),
        })
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    assert!(!ack.session_present);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .with_timeout(Duration::from_secs(2))
        .build()
        .await?;
    // #2 connection take over the session from #1
    assert_ok!(client2.connect(true).await);
    let ack = assert_ok!(client2.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(shutdown_notify);

    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Disconnect(d) if d == Disconnect {
           reason_code: ReasonCode::ServerShuttingDown,
           properties: Default::default()
        }
    );

    let msg = assert_ok!(client2.timeout_recv().await);
    match msg {
        ControlPacket::Publish(p)
            if p == Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic_name: MqttString::from("topic/will"),
                packet_identifier: None,
                properties: Default::default(),
                payload: Bytes::from("WILL"),
            } => {}
        ControlPacket::Disconnect(d)
            if d == Disconnect {
                reason_code: ReasonCode::ServerShuttingDown,
                properties: Default::default(),
            } => {}
        _ => panic!("unexpected {:?}", msg),
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.await?
}
