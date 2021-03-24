use anyhow::Result;
use lazy_static::lazy_static;
use std::str::FromStr;
use std::sync::Arc;
use tokio::signal;
use tracing::{debug, error, Level};

use como::service;
use como::settings::Settings;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};
use warp::ws::WebSocket;
use warp::{Filter, Rejection, Reply};

/*use std::thread;
use std::time::Duration;
use tracing_subscriber::prelude::*;
 */

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

#[tokio::main]
async fn main() -> Result<()> {
    /*    let (tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_service_name("report_example")
            .install()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(opentelemetry)
            .try_init()?;

        let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
        let _enter = root.enter();

        //let work_result = expensive_work();

        span!(tracing::Level::INFO, "faster_work")
            .in_scope(|| thread::sleep(Duration::from_millis(10)));

        warn!("About to exit!");
    */
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
        .with_max_level(Level::from_str(settings.log.level.as_str())?)
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

    let metrics_route = warp::path!("metrics").and_then(metrics_handler);
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::path::param())
        .and_then(ws_handler);

    debug!("Started web on port 8080");
    /*    warp::serve(metrics_route.or(ws_route))
    .run(([0, 0, 0, 0], 8080))
    .await;*/

    //service::run(settings, signal::ctrl_c()).await
    let (_, warp) = warp::serve(metrics_route.or(ws_route))
        .bind_with_graceful_shutdown(([0, 0, 0, 0], 8080), async {
            signal::ctrl_c().await.unwrap()
        });

    tokio::spawn(warp);

    service::run(settings, signal::ctrl_c()).await
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        error!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}

async fn ws_handler(ws: warp::ws::Ws, id: String) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| client_connection(socket, id)))
}

async fn client_connection(ws: WebSocket, id: String) {
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
