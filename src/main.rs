use anyhow::Result;
use dotenv::dotenv;
use futures::prelude::*;
use kafka_settings::{consumer, producer};
use log::info;
use rdkafka::{message::BorrowedMessage, producer::FutureRecord, Message};
use std::env;
use std::time::Duration;
use volatility_harvesting::{Algorithm, Settings};

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

async fn run_async_processor(settings: Settings, initial_equity: f64) -> Result<()> {
    let consumer = consumer(&settings.kafka)?;
    let producer = producer(&settings.kafka)?;
    let output_topic = env::var("OUTPUT_TOPIC")?;

    let algo = Algorithm::new(initial_equity, Duration::from_secs(90));
    let (sender, mut receiver) = algo.split();

    tokio::spawn(async move {
        let latch_message = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(volatility_harvesting::Message::Latch)
        };
        tokio::pin!(latch_message);
        let latch_stream = futures::stream::once(latch_message);
        let data_stream = consumer.stream().map(handle_message);
        let mut stream = futures::stream::select(latch_stream, data_stream);
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
    let settings = Settings::new()?;
    let initial_equity = 1_000_000.0;
    info!("Starting strategy");

    run_async_processor(settings, initial_equity).await
}
