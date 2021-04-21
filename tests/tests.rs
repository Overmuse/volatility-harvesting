use env_logger;
use futures::prelude::*;
use polygon::ws::{PolygonMessage, Tape, Trade};
use rand::prelude::*;
use rand::seq::SliceRandom;
use rust_decimal::Decimal;
use std::time::Duration;
use tokio::time::sleep;
use volatility_harvesting::{Algorithm, Message, State};

fn random_trade() -> PolygonMessage {
    let mut rng = thread_rng();
    let tickers: Vec<String> = vec!["AAPL".into(), "TSLA".into(), "MSFT".into()];
    PolygonMessage::Trade(Trade {
        symbol: tickers.choose(&mut rng).unwrap().clone(),
        exchange_id: 0,
        trade_id: "TEST".into(),
        tape: Tape::A,
        price: Decimal::new(rng.gen_range(95, 105), 2),
        size: 1,
        conditions: Vec::new(),
        timestamp: 0,
    })
}

#[tokio::test]
async fn main() {
    let _ = env_logger::try_init();
    let algo = Algorithm::new(
        Decimal::new(1000000, 0),
        Decimal::new(1, 0),
        Duration::from_millis(200),
    );
    let (sender, mut receiver) = algo.split();
    tokio::spawn(async move {
        sender
            .for_each(|msg| async move { println!("{:?}", &msg) })
            .await;
    });
    for _ in 0..10 {
        sleep(Duration::from_millis(100)).await;
        receiver
            .send(Message::Polygon(random_trade()))
            .await
            .unwrap();
    }
    receiver
        // Markets are open
        .send(Message::State(State::Open { next_close: 22800 }))
        .await
        .unwrap();
    for _ in 0..10 {
        sleep(Duration::from_millis(100)).await;
        receiver
            .send(Message::Polygon(random_trade()))
            .await
            .unwrap();
    }
    receiver
        // Market closing in less than 10 minutes
        .send(Message::State(State::Open { next_close: 590 }))
        .await
        .unwrap();
    for _ in 0..10 {
        sleep(Duration::from_millis(100)).await;
        receiver
            .send(Message::Polygon(random_trade()))
            .await
            .unwrap();
    }
}
