use std::sync::{Arc, Once};

use anyhow::{Error, Result};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;

use como::context::SessionContext;
use como::service;
use como::settings::Settings;

static ONCE: Once = Once::new();

pub async fn start_test_broker() -> (u16, Sender<()>, JoinHandle<Result<(), Error>>) {
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
        let context = SessionContext::new(Arc::new(settings))?;

        service::run_with_ready(context, shutdown, ready).await
    });
    barrier.wait().await;
    (port, shutdown_notify, handle)
}
