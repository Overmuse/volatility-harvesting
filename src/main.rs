use anyhow::{Context, Result};
use dotenv::dotenv;
use futures::prelude::*;
use log::info;
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, Consumer},
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    Message,
};
use std::env;
use std::time::Duration;
use volatility_harvesting::Algorithm;

// Couldn't figure out how to structure this as a closure due to the lifetime.
fn handle_message(
    msg: rdkafka::error::KafkaResult<BorrowedMessage<'_>>,
) -> Result<volatility_harvesting::Message, String> {
    let msg = msg.expect("Got error from Kafka").detach();
    Ok(
        serde_json::from_slice(msg.payload().expect("Failed to get payload"))
            .expect("Failed to deserialize message"),
    )
}

async fn run_async_processor(initial_equity: f64) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &env::var("GROUP_ID")?)
        .set("bootstrap.servers", &env::var("BOOTSTRAP_SERVERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &env::var("SASL_USERNAME")?)
        .set("sasl.password", &env::var("SASL_PASSWORD")?)
        .set("enable.ssl.certificate.verification", "false")
        .create()
        .context("Failed to create Kafka consumer")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &env::var("BOOTSTRAP_SERVERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &env::var("SASL_USERNAME")?)
        .set("sasl.password", &env::var("SASL_PASSWORD")?)
        .set("enable.ssl.certificate.verification", "false")
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create Kafka producer")?;

    let input_topics = env::var("INPUT_TOPICS")?;
    let input_topics: Vec<&str> = input_topics.split(',').collect();
    let output_topic = env::var("OUTPUT_TOPIC")?;

    consumer
        .subscribe(&input_topics)
        .context("Can't subscribe to specified topic")?;

    let algo = Algorithm::new(initial_equity);
    let (sender, mut receiver) = algo.split();

    tokio::spawn(async move {
        let mut stream = consumer.stream().map(handle_message);
        receiver
            .send_all(&mut stream)
            .await
            .expect("Failed to send message");
    });

    sender
        .for_each(|msg| {
            let producer = producer.clone();
            let output_topic = output_topic.clone();

            async move {
                producer
                    .send(
                        FutureRecord::to(&output_topic).key(&msg.ticker).payload(
                            &serde_json::to_string(&msg).expect("failed to serialize order intent"),
                        ),
                        Duration::from_secs(0),
                    )
                    .await
                    .expect("Failed to send to kafka");
            }
        })
        .await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv();
    env_logger::builder().format_timestamp_micros().init();
    let initial_equity = 1_000_000.0;
    info!("Starting strategy");

    run_async_processor(initial_equity).await
}
