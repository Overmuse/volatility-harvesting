use anyhow::Result;
use dotenv::dotenv;
use futures::prelude::*;
use kafka_settings::{consumer, producer};
use log::info;
use rdkafka::{
    consumer::Consumer, message::BorrowedMessage, producer::FutureRecord,
    topic_partition_list::Offset, Message,
};
use std::time::Duration;
use volatility_harvesting::{Algorithm, Settings};

// Couldn't figure out how to structure this as a closure due to the lifetime.
fn handle_message(
    msg: rdkafka::error::KafkaResult<BorrowedMessage<'_>>,
) -> Result<volatility_harvesting::Message> {
    let msg = msg?;
    Ok(
        serde_json::from_slice(msg.payload().expect("Empty payload received"))
            .expect("Failed to deserialize message"),
    )
}

async fn run_async_processor(settings: Settings) -> Result<()> {
    let consumer = consumer(&settings.kafka)?;
    let producer = producer(&settings.kafka)?;

    let algo = Algorithm::new(
        settings.app.initial_equity,
        settings.app.internal_leverage,
        Duration::from_secs(settings.app.batch_seconds),
    );
    let (sender, mut receiver) = algo.split();

    tokio::spawn(async move {
        // We need to poll the consumer before seeking to the end. I couldn't find a way to do that
        // without specifically pulling one message, but since we're scrolling to the end anyway,
        // it doesn't really matter that we lose this one message.
        consumer.recv().await.unwrap();
        // TODO: Remove the hardcoded partition range here
        for partition in 0..=5 {
            consumer
                .seek("trades", partition, Offset::End, None)
                .unwrap();
        }
        let mut data_stream = consumer.stream().map(handle_message);
        receiver
            .send_all(&mut data_stream)
            .await
            .expect("Failed to send message");
    });

    sender
        .for_each(|msg| {
            let producer = producer.clone();

            async move {
                producer
                    .send(
                        // TODO: Make this part of settings
                        FutureRecord::to("position-intents")
                            .key(&msg.ticker)
                            .payload(
                                &serde_json::to_string(&msg)
                                    .expect("failed to serialize order intent"),
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
    info!("Starting strategy");

    run_async_processor(settings).await
}
