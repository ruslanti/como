use std::collections::HashMap;
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{debug, Level};

use como::service;
use como::settings::Settings;

/*use std::thread;
use std::time::Duration;
use tracing_subscriber::prelude::*;
 */

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

    service::run(settings, signal::ctrl_c()).await
}
