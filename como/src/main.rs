use std::sync::Arc;

use anyhow::Result;
use tokio::signal::unix::{signal, SignalKind};
use tracing::debug;
use tracing_subscriber::EnvFilter;
use warp::ws::WebSocket;
use warp::{Filter, Rejection, Reply};

use como::context::SessionContext;
use como::metric::metrics_handler;
use como::service;
use como::settings::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Arc::new(Settings::new()?);
    // a builder for `FmtSubscriber`.
    let (non_blocking, _guard) = if let Some(file) = settings.log.file.clone() {
        tracing_appender::non_blocking(tracing_appender::rolling::daily("logs", file))
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };

    /*    let field_formatter =
            // Construct a custom formatter for `Debug` fields
            format::debug_fn(|writer, field, value| write!(writer, "{}:{:?}", field, value))
                .delimited(", ");
    */
    let subscriber = tracing_subscriber::fmt()
        //.with_max_level(Level::from_str(settings.log.level.as_str())?)
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(true)
        .with_writer(non_blocking)
        //.pretty()
        .with_thread_ids(true)
        //.with_thread_names(true)
        .with_target(false)
        //.fmt_fields(field_formatter)
        //.compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("no global subscriber has been set");

    debug!("{:?}", settings);
    let context = SessionContext::new(settings)?;

    let metrics_route = warp::path!("metrics").and_then(metrics_handler);
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::path::param())
        .and_then(ws_handler);

    tokio::spawn(warp::serve(metrics_route.or(ws_route)).run(([0, 0, 0, 0], 8080)));

    let mut terminate = signal(SignalKind::terminate())?;
    service::run(context, terminate.recv()).await
}

async fn ws_handler(ws: warp::ws::Ws, id: String) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| client_connection(socket, id)))
}

async fn client_connection(_ws: WebSocket, _id: String) {
    /*let (_client_ws_sender, mut client_ws_rcv) = ws.split();

    CONNECTED_CLIENTS.inc();
    println!("{} connected", id);

    while let Some(result) = client_ws_rcv.next().await {
        match result {
            Ok(msg) => println!("received message: {:?}", msg),
            Err(e) => {
                eprintln!("error receiving ws message for id: {}): {}", id.clone(), e);
                break;
            }
        };
    }

    println!("{} disconnected", id);
    CONNECTED_CLIENTS.dec();*/
}
