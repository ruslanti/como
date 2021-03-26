use lazy_static::lazy_static;
use prometheus::{register_histogram_vec, register_int_counter_vec, register_int_gauge_vec};
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};
use tracing::error;
use warp::{Rejection, Reply};

lazy_static! {
    //
    // connections metric
    //
    pub static ref ACTIVE_CONNECTIONS: IntGaugeVec = register_int_gauge_vec!(
        "active_connections",
        "Active connected MQTT clients",
        &["proto", "peer"]
    )
    .expect("metric active_connections can be created");
    //
    // Sessions metrics
    //
    pub static ref ACTIVE_SESSIONS: IntGaugeVec =
        register_int_gauge_vec!("active_sessions", "Active MQTT sessions", &[])
            .expect("metric active_sessions can be created");
    //
    // Packet messages metrics
    //
    pub static ref PACKETS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "packets_received_totals",
        "Received MQTT packages",
        &["type"]
    )
    .expect("metric packets_received can be created");
    pub static ref PACKETS_SENT: IntCounterVec =
        register_int_counter_vec!("packets_sent_totals", "Sent MQTT packages", &["type"])
            .expect("metric packets_sent_totals can be created");
    pub static ref RESPONSE_TIME: HistogramVec = register_histogram_vec!(
        "packets_process_seconds",
        "Received MQTT packages time",
        &["type"]
    )
    .expect("metric can be created");
    //
    // Topics metrics
    //
    pub static ref TOPICS_NUMBER: IntCounterVec =
        register_int_counter_vec!("topics_number_totals", "Total number of topics", &[])
            .expect("metric topic_totals can be created");
    pub static ref TOPICS_SIZE: IntGaugeVec = register_int_gauge_vec!(
        "topics_bytes",
        "Size of all topics",
        &[]
    )
    .expect("metric topics_bytes can be created");
}

pub async fn metrics_handler() -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("could not encode metrics: {}", e);
    };
    let res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    Ok(res)
}
