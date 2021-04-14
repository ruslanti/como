#[macro_use]
extern crate claim;

use crate::common::start_test_broker;
use como_mqtt::client::MqttClient;
use como_mqtt::v5::types::ReasonCode;

mod common;

#[tokio::test]
async fn allow_anonymous() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) =
        start_test_broker(vec![("service.allow_anonymous", "true")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("allow_anonymous")
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
        start_test_broker(vec![("service.allow_anonymous", "false")]).await;

    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", port))
        .client_id("deny_anonymous")
        .build()
        .await?;

    let ack = assert_ok!(client.connect(true).await);
    assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}
