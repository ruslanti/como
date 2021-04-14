#[macro_use]
extern crate claim;

use test_case::test_case;

use como_mqtt::client::MqttClient;
use como_mqtt::v5::types::ReasonCode;

use crate::common::start_test_broker;

mod common;

#[tokio::test]
async fn allow_anonymous() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) =
        start_test_broker(vec![("allow_anonymous", "true")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("allowanonymous")
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn deny_anonymous() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) =
        start_test_broker(vec![("allow_anonymous", "false")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("denyanonymous")
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn bad_packet() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker(vec![]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("badpacket")
        .build()
        .await?;

    assert_ok!(client.publish_most_once("bad/packet", vec![], false).await);
    let error = assert_err!(client.timeout_recv().await);
    assert!(error.is::<&str>());
    assert_matches!(assert_ok!(error.downcast::<&str>()), s if s == "disconnected");
    //drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn duplicate_connect() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker(vec![]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("duplicateconnect")
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);

    let error = assert_err!(client.connect(true).await);
    assert!(error.is::<&str>());
    assert_matches!(assert_ok!(error.downcast::<&str>()), s if s == "disconnected");
    //drop(client);
    drop(shutdown_notify);
    handle.await?
}

/* The ClientID MUST be present and is the first field in the CONNECT packet Payload [MQTT-3.1.3-3].
 The ClientID MUST be a UTF-8 Encoded String as defined in section 1.5.4 [MQTT-3.1.3-4].
 The Server MUST allow ClientID’s which are between 1 and 23 UTF-8 encoded bytes in length, and
 that contain only the characters "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" [MQTT-3.1.3-5].
 The Server MAY allow ClientID’s that contain more than 23 encoded bytes. The Server MAY allow
 ClientID’s that contain characters not included in the list given above.
 If the Server rejects the ClientID it MAY respond to the CONNECT packet with a CONNACK using
 Reason Code 0x85 (Client Identifier not valid) as described in section 4.13 Handling errors,
 and then it MUST close the Network Connection [MQTT-3.1.3-8].
*/
#[test_case(""; "empty_client_id")]
#[test_case("0123a456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"; "big_client_id")]
#[test_case("@aaa"; "now_allowed_client_id")]
#[tokio::test]
async fn invalid_client_id(input: &'static str) -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker(vec![]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id(input)
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::ClientIdentifiersNotValid);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

/*
A Server MAY allow a Client to supply a ClientID that has a length of zero bytes, however if it
does so the Server MUST treat this as a special case and assign a unique ClientID to that Client [MQTT-3.1.3-6].
It MUST then process the CONNECT packet as if the Client had provided that unique ClientID,
and MUST return the Assigned Client Identifier in the CONNACK packet [MQTT-3.1.3-7].
*/
#[tokio::test]
async fn allow_empty_client_id() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker(vec![("allow_empty_id", "true")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::Success);
    let id = assert_some!(ack.properties.assigned_client_identifier);
    assert!(!id.is_empty());

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[tokio::test]
async fn invalid_reserved() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker(vec![("allow_empty_id", "true")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .build()
        .await?;

    let error = assert_err!(client.connect_reserved(false).await);
    assert!(error.is::<&str>());
    assert_matches!(assert_ok!(error.downcast::<&str>()), s if s == "disconnected");

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

#[test_case("false", Some("username"), Some(b"password"), ReasonCode::Success; "success")]
#[test_case("false", Some("username"), None, ReasonCode::BadUserNameOrPassword; "empty_password")]
#[test_case("false", None, Some(b"password"), ReasonCode::NotAuthorized; "bad_username")]
#[test_case("false", None, None, ReasonCode::NotAuthorized; "not_authorized")]
#[test_case("true", Some("username"), Some(b"password"), ReasonCode::Success;
"anonymous_success")]
#[test_case("true", Some("username"), None, ReasonCode::BadUserNameOrPassword;
"anonymous_empty_password")]
#[test_case("true", None, Some(b"password"), ReasonCode::NotAuthorized; "anonymous_bad_username")]
#[test_case("true", None, None, ReasonCode::Success; "anonymous_no_userpass")]
#[tokio::test]
async fn username_password_test(
    allow_anonymous: &'static str,
    username: Option<&'static str>,
    password: Option<&'static [u8]>,
    reason: ReasonCode,
) -> anyhow::Result<()> {
    let changes = vec![
        ("allow_empty_id", "true"),
        ("allow_anonymous", allow_anonymous),
    ];
    let (port, shutdown_notify, handle) = start_test_broker(changes).await;
    let address = format!("127.0.0.1:{}", port);
    let builder = MqttClient::builder(address.as_str());
    let builder = if let Some(username) = username {
        builder.username(username)
    } else {
        builder
    };
    let builder = if let Some(password) = password {
        builder.password(password)
    } else {
        builder
    };

    let mut client = builder.build().await?;

    let ack = assert_ok!(client.connect(false).await);
    assert_eq!(ack.reason_code, reason);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}
