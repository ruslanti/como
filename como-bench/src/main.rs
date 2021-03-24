use std::borrow::BorrowMut;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tokio::sync::Barrier;

use como_mqtt::client::MqttClient;

use crate::scenario::{Publication, Scenario};

mod scenario;

#[derive(Clone)]
struct PublicationBatch {
    address: String,
    publication: Publication,
    start: Arc<Barrier>,
}

async fn publish(client: &mut MqttClient, topic: &str, payload: Vec<u8>) {
    loop {
        client
            .publish_most_once(topic, payload.clone(), false)
            .await
            .unwrap();
    }
}

impl PublicationBatch {
    async fn run(&mut self, id: usize, stop: impl Future) {
        println!("run {}", id);
        let payload = vec![0xFF; self.publication.payload_size];
        let mut client = MqttClient::builder(self.address.as_str())
            .build()
            .await
            .unwrap();
        self.start.wait().await;
        let _ack = client.connect(true).await.unwrap();
        let topic = format!("{}/{}", self.publication.topic_name, id);

        tokio::select! {
            _ = publish(client.borrow_mut(), topic.as_str(), payload)=> {
            },
            _ = stop => {
                println!("stop {}", id);
            }
        }

        println!("disconnect {}", id);
        client.disconnect().await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let scenario = Scenario::new().unwrap();
    let publication = scenario.publication;
    let subscription = scenario.subscription;
    let client_num = publication.clients;
    let barrier = Arc::new(Barrier::new(client_num));
    let pub_batch = PublicationBatch {
        address: scenario.address,
        publication,
        start: barrier,
    };
    for id in 0..pub_batch.publication.clients {
        let mut batch = pub_batch.clone();
        tokio::spawn(async move {
            batch.run(id, signal::ctrl_c()).await;
        });
    }

    /*    let subscriber = Builder::default().build(|| Histogram::new_with_max(1_000_000, 2).unwrap());
        let dispatch = Dispatch::new(subscriber);
        tracing::dispatcher::set_global_default(dispatch.clone())
            .expect("setting tracing default failed");

        tokio::spawn(async move {
            let mut rng = rand::thread_rng();
            println!("start");
            loop {
                trace_span!("foo").in_scope(|| {
                    trace!("foo_start");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    //trace_span!("bar").in_scope(|| {
                    trace!("bar_start");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    let y: f64 = rng.gen();
                    if y > 0.5 {
                        trace!("bar_mid1");
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    } else {
                        trace!("bar_mid2");
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                    trace!("bar_end");
                    //});
                    trace!("foo_end");
                })
            }
        });
    */
    /*    let n = 10;
    let barrier = Arc::new(Barrier::new(n));
    for _ in 0..n {
        let start_barrier = barrier.clone();
        tokio::spawn(async move {
            client(start_barrier).await;
        });
        println!("end client");
    }*/

    /*    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        println!("end sleep");
        dispatch
            .downcast_ref::<TimingSubscriber>()
            .unwrap()
            .with_histograms(|hs| {
                /*            assert_eq!(hs.len(), 1);
                let hs = &mut hs.get_mut("client").unwrap();

                for key in hs.keys() {
                    println!("{}", key);
                }
                println!("refresh1");*/
                let timeout = Duration::from_secs(1);
                /*            hs.get_mut("before connect")
                    .unwrap()
                    .refresh_timeout(timeout);
                hs.get_mut("after connect")
                    .unwrap()
                    .refresh_timeout(timeout);

                hs.get_mut("after push").unwrap().refresh_timeout(timeout);*/

                for (span_group, hs) in hs {
                    println!("span_group {}", span_group);
                    for (event_group, h) in hs {
                        //  println!("refresh {:?}:{:?}", event_group, h);
                        // make sure we see the latest samples:
                        h.refresh_timeout(timeout);
                        // print the median:
                        println!(
                            "{} -> {}: {}ns",
                            span_group,
                            event_group,
                            h.value_at_quantile(0.5)
                        )
                    }
                }
            });
    */
    println!("end report");
    signal::ctrl_c().await.unwrap();
}

//#[instrument]
async fn client(start: Arc<Barrier>) {
    let payload = vec![0xFF; 128];
    let mut client = MqttClient::builder(&format!("127.0.0.1:{}", 1883))
        .build()
        .await
        .unwrap();

    start.wait().await;

    //  trace!("before connect");
    let _ack = client.connect(true).await.unwrap();
    //  trace!("after connect");

    for _ in 0..100 {
        client
            .publish_most_once("topic", payload.clone(), false)
            .await
            .unwrap();
        //    trace!("after push");
    }
    client.disconnect().await.unwrap();
}
