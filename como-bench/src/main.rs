use std::borrow::BorrowMut;

use tokio::signal;
use tokio::sync::{broadcast, mpsc};

use como_mqtt::client::MqttClient;

use crate::scenario::{Publication, Scenario, Subscription};
use como::shutdown::Shutdown;
use como_mqtt::v5::types::{QoS, ReasonCode};
use lazy_static::lazy_static;
use leaky_bucket::{LeakyBucket, LeakyBuckets};
use prometheus::Encoder;
use prometheus::{register_histogram_vec, register_int_counter_vec, register_int_gauge_vec};
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};

use std::time::Duration;

mod scenario;

lazy_static! {
    //
    // connections metric
    //
    pub static ref ACTIVE_CONNECTIONS: IntGaugeVec = register_int_gauge_vec!(
        "bench_active_connections",
        "Active connected MQTT clients",
        &[]
    )
    .expect("metric active_connections can be created");

    //
    // Packet messages metrics
    //
    pub static ref PACKETS_RECEIVED: IntCounterVec = register_int_counter_vec!(
        "bench_packets_received_totals",
        "Received MQTT packages",
        &[]
    )
    .expect("metric packets_received can be created");

    pub static ref PACKETS_SENT: IntCounterVec =
        register_int_counter_vec!("bench_packets_sent_totals", "Sent MQTT packages", &[])
            .expect("metric packets_sent_totals can be created");

    pub static ref RESPONSE_TIME: HistogramVec = register_histogram_vec!(
        "bench_packets_process_seconds",
        "Received MQTT packages time",
        &[]
    )
    .expect("metric can be created");

}

#[derive(Clone)]
struct PublicationBatch {
    address: String,
    publication: Publication,
    shutdown_complete: mpsc::UnboundedSender<()>,
}

#[derive(Clone)]
struct SubscriptionBatch {
    address: String,
    subscription: Subscription,
    shutdown_complete: mpsc::UnboundedSender<()>,
}

async fn recv(client: &mut MqttClient) {
    loop {
        let _res = client.recv().await.unwrap();
        PACKETS_RECEIVED.with_label_values(&[]).inc();
    }
}

impl PublicationBatch {
    async fn run(&mut self, id: usize, rate: LeakyBucket, mut shutdown: Shutdown) {
        let payload = vec![0xFF; self.publication.payload_size];
        let mut client = MqttClient::builder(self.address.as_str())
            .build()
            .await
            .unwrap();
        // self.start.wait().await;
        if let Err(error) = client.connect(true).await {
            eprintln!("connect error: {:?}", error);
        }
        ACTIVE_CONNECTIONS.with_label_values(&[]).inc();
        let topic = format!("{}/{}", self.publication.topic_name, id);
        let topic = topic.as_str();

        while !shutdown.is_shutdown() {
            tokio::select! {
                _ = rate.acquire_one() => {
                    PACKETS_SENT.with_label_values(&[]).inc();
                    let _time = RESPONSE_TIME.with_label_values(&[]).start_timer();

                    match self.publication.qos {
                        QoS::AtMostOnce =>
                            if let Err(e) = client.publish_most_once(topic, payload.clone(), false).await {
                                eprintln!("publish_most_once error: {:?}", e);
                            },
                        QoS::AtLeastOnce =>  {
                            match client.publish_least_once(topic, payload.clone(), false).await {
                                Ok(res) => if res.reason_code != ReasonCode::Success {
                                    eprintln!("publish_most_once reason code: {:?}", res.reason_code)
                                }
                                Err(e) => eprintln!("{} publish_most_once error: {:?}", client, e)
                            }
                        },
                        QoS::ExactlyOnce => unimplemented!()


                    }
                }
                _ = shutdown.recv() => {
                }
            }
        }
        drop(rate);
        client.disconnect().await.unwrap();
        ACTIVE_CONNECTIONS.with_label_values(&[]).dec();
    }
}

impl SubscriptionBatch {
    async fn run(&mut self, mut shutdown: Shutdown) {
        println!("subscribe {}", self.subscription.topic_filter);
        let mut client = MqttClient::builder(self.address.as_str())
            .build()
            .await
            .unwrap();
        if let Err(error) = client.connect(true).await {
            eprintln!("connect error: {:?}", error);
        }
        ACTIVE_CONNECTIONS.with_label_values(&[]).inc();
        if let Err(error) = client
            .subscribe(QoS::AtLeastOnce, self.subscription.topic_filter.as_str())
            .await
        {
            eprintln!("subscribe error: {:?}", error);
        }

        tokio::select! {
            res = recv(client.borrow_mut()) => {
                println!("recv {:?}", res)
            },
            _ = shutdown.recv() => {
            }
        }
        client.disconnect().await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::init();
    let scenario = Scenario::new().unwrap();
    let address = scenario.address.as_str();

    let (sub_notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::unbounded_channel();

    start_subscribers(
        scenario.subscriptions,
        address,
        sub_notify_shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    start_publications(scenario.publications, address, shutdown_complete_tx).await;

    // wait a while to receive all subscriptions
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(sub_notify_shutdown);

    shutdown_complete_rx.recv().await;

    output_metrics();
}

fn output_metrics() {
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        eprintln!("could not encode metrics: {:?}", e);
    };
    let res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    println!("{}", res);
}

async fn start_publications(
    publications: Vec<Publication>,
    address: &str,
    shutdown_complete_tx: mpsc::UnboundedSender<()>,
) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let mut buckets = LeakyBuckets::new();

    for publication in publications.into_iter() {
        let rate = publication.rate;
        let pub_batch = PublicationBatch {
            address: address.to_owned(),
            publication,
            shutdown_complete: shutdown_complete_tx.clone(),
        };

        let rate = buckets
            .rate_limiter()
            .max(rate)
            .refill_interval(Duration::from_secs(1))
            .refill_amount(rate)
            .build()
            .unwrap();

        for id in 0..pub_batch.publication.clients {
            let mut batch = pub_batch.clone();
            let rate = rate.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());
            tokio::spawn(async move {
                batch.run(id, rate, shutdown).await;
            });
        }
    }

    tokio::select! {
        res = buckets.coordinate().unwrap() => {
            println!("buckets coordinate {:?}", res);
        }
        _ = signal::ctrl_c() => {
        }
    }
}

fn start_subscribers(
    subscriptions: Vec<Subscription>,
    address: &str,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::UnboundedSender<()>,
) {
    for subscription in subscriptions.into_iter() {
        let sub_batch = SubscriptionBatch {
            address: address.to_owned(),
            subscription,
            shutdown_complete: shutdown_complete_tx.clone(),
        };
        for _ in 0..sub_batch.subscription.clients {
            let mut batch = sub_batch.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());
            tokio::spawn(async move {
                batch.run(shutdown).await;
            });
        }
    }
}
