use std::sync::{Arc, Once};

use anyhow::{Error, Result};
use tokio::sync::oneshot::Sender;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;

use como::context::SessionContext;
use como::service;
use config::Config;

static ONCE: Once = Once::new();

pub async fn start_test_broker(
    changes: Vec<(&str, &str)>,
) -> (u16, Sender<()>, JoinHandle<Result<(), Error>>) {
    ONCE.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    let port = rand::random::<u16>();
    let (shutdown_notify, shutdown) = oneshot::channel::<()>();
    let barrier = Arc::new(Barrier::new(2));

    let mut cfg = Config::new();
    cfg.set("service.port", port.to_string()).unwrap();
    for (key, value) in changes.into_iter() {
        cfg.set(key, value).unwrap();
    }

    let settings = cfg.try_into().unwrap();
    let ready = barrier.clone();
    let handle = tokio::spawn(async move {
        let context = SessionContext::new(Arc::new(settings))?;

        service::run_with_ready(context, shutdown, ready).await
    });
    barrier.wait().await;
    (port, shutdown_notify, handle)
}
