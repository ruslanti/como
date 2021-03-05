use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::Duration;

use como::service;
use como::settings::Settings;
use como_mqtt::client::ClientBuilder;

pub fn setup() {
    // setup code specific to your library's tests would go here
}

#[tokio::test]
async fn basic_connect_clean_session() {
    let port = rand::random::<u16>();
    let mut settings = Settings::default();
    settings.service.port = port;

    tracing_subscriber::fmt::init();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(service::run(Arc::new(settings), shutdown_rx));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut client = ClientBuilder::new(&format!("127.0.0.1:{}", port))
        .client()
        .await
        .unwrap();

    let res = client.connect(true).await.unwrap();
    println!("{:?}", res);

    client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(shutdown_tx); // send terminate signal
    tokio::time::sleep(Duration::from_millis(100)).await;
}
