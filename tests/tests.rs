use env_logger;
use futures::prelude::*;
use polygon::ws::{PolygonMessage, Tape, Trade};
use rand::prelude::*;
use rand::seq::SliceRandom;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tokio_stream::wrappers::IntervalStream;
use volatility_harvesting::{Algorithm, Message};

fn random_trade() -> PolygonMessage {
    let mut rng = thread_rng();
    let tickers: Vec<String> = vec!["AAPL".into(), "TLSA".into(), "MSFT".into()];
    PolygonMessage::Trade(Trade {
        symbol: tickers.choose(&mut rng).unwrap().clone(),
        exchange_id: 0,
        trade_id: "TEST".into(),
        tape: Tape::A,
        price: rng.gen_range(95.0, 105.0),
        size: 1,
        conditions: Vec::new(),
        timestamp: 0,
    })
}

#[tokio::test]
async fn main() {
    // TODO: Break infinite loop
    let _ = env_logger::try_init();
    let algo = Algorithm::new(1000000.0, 1.0, Duration::from_millis(200));
    let (sender, mut receiver) = algo.split();
    tokio::spawn(async move {
        let interval_fut = interval(Duration::from_millis(100));
        let interval_stream = IntervalStream::new(interval_fut);
        let trade_stream = interval_stream
            .map(|_| Ok(Message::Polygon(random_trade())))
            .take(30);
        let latch_message = async {
            sleep(Duration::from_secs(1)).await;
            Ok(Message::Latch)
        };
        tokio::pin!(latch_message);
        let latch_stream = futures::stream::once(latch_message);
        let mut stream = futures::stream::select(trade_stream, latch_stream);
        receiver.send_all(&mut stream).await.unwrap()
    });
    sender
        .for_each(|msg| async move { println!("{:?}", &msg) })
        .await;
}
