use std::borrow::BorrowMut;
use std::future::Future;
use std::sync::Arc;

use tokio::signal;
use tokio::sync::{mpsc, Barrier};

use como_mqtt::client::MqttClient;

use crate::scenario::{Publication, Scenario, Subscription};
use como_mqtt::v5::types::QoS;
use leaky_bucket::LeakyBucket;
use std::time::Duration;

mod scenario;

#[derive(Clone)]
struct PublicationBatch {
    address: String,
    publication: Publication,
    start: Arc<Barrier>,
    stop: mpsc::Sender<()>,
}

#[derive(Clone)]
struct SubscriptionBatch {
    address: String,
    subscription: Subscription,
    stop: mpsc::Sender<()>,
}

async fn publish(rate: LeakyBucket, client: &mut MqttClient, topic: &str, payload: Vec<u8>) {
    loop {
        rate.acquire_one().await.unwrap();
        client
            .publish_most_once(topic, payload.clone(), false)
            .await
            .unwrap();
    }
}

async fn recv(client: &mut MqttClient) {
    loop {
        let _res = client.recv().await.unwrap();
        //println!("{:?}", res);
    }
}

impl PublicationBatch {
    async fn run(&mut self, rate: LeakyBucket, id: usize, stop: impl Future) {
        println!("run {}", id);
        let payload = vec![0xFF; self.publication.payload_size];
        let mut client = MqttClient::builder(self.address.as_str())
            .build()
            .await
            .unwrap();
        self.start.wait().await;
        println!("start {}", id);
        let _ack = client.connect(true).await.unwrap();
        let topic = format!("{}/{}", self.publication.topic_name, id);

        tokio::select! {
            _ = publish(rate, client.borrow_mut(), topic.as_str(), payload)=> {
            },
            _ = stop => {
                println!("stop {}", id);
            }
        }

        println!("disconnect {}", id);
        client.disconnect().await.unwrap();
    }
}

impl SubscriptionBatch {
    async fn run(&mut self, stop: impl Future) {
        println!("subscribe {}", self.subscription.topic_filter);
        let mut client = MqttClient::builder(self.address.as_str())
            .build()
            .await
            .unwrap();
        let _ack = client.connect(true).await.unwrap();
        let _ack = client
            .subscribe(QoS::AtLeastOnce, self.subscription.topic_filter.as_str())
            .await
            .unwrap();

        tokio::select! {
            res = recv(client.borrow_mut()) => {
                println!("res {:?}", res)
            },
            _ = stop => {
                println!("stop {}", self.subscription.topic_filter);
            }
        }

        client.disconnect().await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let scenario = Scenario::new().unwrap();
    let publications = scenario.publications;
    let client_num = publications.iter().map(|p| p.clients).sum();
    let address = scenario.address;

    let rate = LeakyBucket::builder()
        .max(scenario.pub_rate)
        .refill_interval(Duration::from_secs(1))
        .refill_amount(scenario.pub_rate)
        .build()
        .unwrap();

    let barrier = Arc::new(Barrier::new(client_num));
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    for publication in publications.into_iter() {
        let pub_batch = PublicationBatch {
            address: address.to_owned(),
            publication,
            start: barrier.clone(),
            stop: shutdown_complete_tx.clone(),
        };
        for id in 0..pub_batch.publication.clients {
            let mut batch = pub_batch.clone();
            let rate = rate.clone();
            tokio::spawn(async move {
                batch.run(rate, id, signal::ctrl_c()).await;
            });
        }
    }

    for subscription in scenario.subscriptions.into_iter() {
        let sub_batch = SubscriptionBatch {
            address: address.to_owned(),
            subscription,
            stop: shutdown_complete_tx.clone(),
        };
        for _ in 0..sub_batch.subscription.clients {
            let mut batch = sub_batch.clone();
            tokio::spawn(async move {
                batch.run(signal::ctrl_c()).await;
            });
        }
    }

    shutdown_complete_rx.recv().await;
}
