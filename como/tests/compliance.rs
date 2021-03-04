use std::sync::Arc;

use tokio::signal;
use tokio::time::Duration;

use como::service;
use como::settings::Settings;
use como_mqtt::client::ClientBuilder;

pub fn setup() {
    // setup code specific to your library's tests would go here
    println!("SETUP");
    panic!("TTT")
}

#[tokio::test]
async fn basic_connect_clean_session() {
    let port = rand::random::<u16>();
    let mut settings = Settings::default();
    settings.service.port = port;

    tracing_subscriber::fmt::init();
    tokio::spawn(service::run(Arc::new(settings), signal::ctrl_c()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut client = ClientBuilder::new(&format!("127.0.0.1:{}", port))
        .client()
        .await
        .unwrap();

    let res = client.connect(true).await.unwrap();
    println!("{:?}", res);

    tokio::time::sleep(Duration::from_millis(1000)).await;
}
