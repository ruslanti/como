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
    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Disconnect(d) if d.reason_code == ReasonCode::SessionTakenOver);
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

    drop(client);
    drop(client2);
    drop(client3);
    drop(client4);
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

    let ack = assert_ok!(client.subscribe(QoS::AtMostOnce, "topic/A").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    assert_ok!(
        client
            .publish_most_once("topic/A", Vec::from("payload01"), false)
            .await
    );

    assert_ok!(
        client
            .publish_most_once("topic/A", Vec::from("payload02"), false)
            .await
    );

    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic_name: MqttString::from("topic/A"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("payload01"),
        }
    );
    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic_name: MqttString::from("topic/A"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("payload02"),
        }
    );

    assert_none!(client.disconnect().await?);
    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn retained_publish_subscribe() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker().await;
    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .build()
        .await?;

    let ack = assert_ok!(client.connect(false).await);
    assert!(!ack.session_present);
    assert_some!(ack.properties.assigned_client_identifier);

    assert_ok!(
        client
            .publish_most_once("topic/A", Vec::from("payload01a"), true)
            .await
    );

    assert_ok!(
        client
            .publish_most_once("topic/A", Vec::from("payload01b"), false)
            .await
    );

    assert_ok!(
        client
            .publish_most_once("topic/B", Vec::from("payload02"), false)
            .await
    );

    assert_ok!(
        client
            .publish_most_once("topic/C", Vec::from("payload03"), true)
            .await
    );

    let ack = assert_ok!(client.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);

    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic_name: MqttString::from("topic/A"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("payload01a"),
        }
    );
    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic_name: MqttString::from("topic/C"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("payload03"),
        }
    );

    // If the Payload contains zero bytes it is processed normally by the Server but any retained
    // message with the same topic name MUST be removed and any future subscribers for the topic
    // will not receive a retained message
    assert_ok!(client.publish_most_once("topic/C", vec![], true).await);

    let mut client2 = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .build()
        .await?;
    // #2 connection take over the session from #1
    assert_ok!(client2.connect(true).await);
    let ack = assert_ok!(client2.subscribe(QoS::AtMostOnce, "topic/+").await);
    assert_eq!(ack.reason_codes, vec![ReasonCode::Success]);
    assert_matches!(assert_ok!(client2.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic_name: MqttString::from("topic/A"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::from("payload01a"),
        }
    );
    assert_matches!(assert_ok!(client.timeout_recv().await), ControlPacket::Publish(p) if p == Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic_name: MqttString::from("topic/C"),
        packet_identifier: None,
        properties: Default::default(),
        payload: Bytes::new(),
        }
    );
    assert_none!(client2.disconnect().await?);

    assert_none!(client.disconnect().await?);

    drop(client);
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
