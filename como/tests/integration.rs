#[macro_use]
extern crate claim;

use std::sync::Arc;

use anyhow::{Error, Result};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use como::service;
use como::settings::Settings;
use como_mqtt::client::ClientBuilder;

#[tokio::test]
async fn basic_connect_clean_session() -> anyhow::Result<()> {
    let (port, shutdown_notify, handle) = start_test_broker();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut client = ClientBuilder::new(&format!("127.0.0.1:{}", port))
        .client()
        .await?;

    let res = client.connect(true).await?;
    println!("{:?}", res);

    assert_ok!(
        client
            .publish_least_once(false, String::from("topic"), vec![10; 10])
            .await
    );

    assert_none!(client.disconnect().await?);

    drop(client);
    drop(shutdown_notify);
    handle.await?
}

fn start_test_broker() -> (u16, Sender<()>, JoinHandle<Result<(), Error>>) {
    let port = rand::random::<u16>();
    let (shutdown_notify, shutdown) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        let mut settings = Settings::default();
        settings.service.port = port;
        tracing_subscriber::fmt::init();

        service::run(Arc::new(settings), shutdown).await
    });
    (port, shutdown_notify, handle)
}
