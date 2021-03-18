use core::pin::Pin;
use futures::prelude::*;
use futures::task::{Context, Poll, Waker};
use log::{debug, info, trace};
use polygon::ws::PolygonMessage;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(PartialEq, Debug, Serialize)]
pub struct Position {
    pub ticker: String,
    pub shares: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Polygon(PolygonMessage),
    Latch,
}

pub struct Sender {
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    outbox: Arc<Mutex<VecDeque<Position>>>,
}

impl Sender {
    fn new(wakers: Arc<Mutex<Vec<Option<Waker>>>>, outbox: Arc<Mutex<VecDeque<Position>>>) -> Self {
        Self { outbox, wakers }
    }
}

impl Stream for Sender {
    type Item = Position;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut outbox = self.outbox.lock().expect("Failed to lock Mutex");
        if outbox.is_empty() {
            trace!("Outbox empty");
            self.wakers
                .lock()
                .expect("Failed to lock Mutex")
                .push(Some(cx.waker().clone()));
            Poll::Pending
        } else {
            debug!("Message available");
            Poll::Ready(outbox.pop_front())
        }
    }
}

pub struct Receiver {
    starting_cash: f64,
    virtual_cash: f64,
    latched: bool,
    start_prices: BTreeMap<String, f64>,
    prev_prices: BTreeMap<String, f64>,
    current_prices: BTreeMap<String, f64>,
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    sender_outbox: Arc<Mutex<VecDeque<Position>>>,
}

impl Receiver {
    fn new(
        starting_cash: f64,
        wakers: Arc<Mutex<Vec<Option<Waker>>>>,
        sender_outbox: Arc<Mutex<VecDeque<Position>>>,
    ) -> Self {
        Self {
            starting_cash,
            virtual_cash: starting_cash,
            latched: false,
            start_prices: BTreeMap::new(),
            prev_prices: BTreeMap::new(),
            current_prices: BTreeMap::new(),
            sender_outbox,
            wakers,
        }
    }
}

impl Sink<Message> for Receiver {
    type Error = String;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        debug!("Item received: {:?}", &item);
        match item {
            Message::Polygon(msg) => {
                if let PolygonMessage::Trade { symbol, price, .. } = msg {
                    self.update_price(&symbol, price)
                }
            }
            Message::Latch => self.latch(),
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Receiver {
    fn latch(&mut self) {
        self.start_prices = self.current_prices.clone();
        self.latched = true;
        info!("Latching prices: {:?}", self.start_prices)
    }

    fn update_price(&mut self, ticker: &str, price: f64) {
        debug!("Price update. Ticker: {}, price: {}", &ticker, &price);
        let prev = self.current_prices.insert(ticker.into(), price);
        if let Some(prev_price) = prev {
            self.prev_prices.insert(ticker.into(), prev_price);
            if self.latched {
                self.virtual_cash *= 1.0 + (price / prev_price - 1.0) / self.num_tickers() as f64;
                debug!("Virtual cash: {}", self.virtual_cash);
            }
        }
        let positions = self.desired_positions();
        for position in positions {
            self.sender_outbox
                .lock()
                .expect("Failed to acquire Mutex")
                .push_back(position);
            for waker in self
                .wakers
                .lock()
                .expect("Failed to acquire Mutex")
                .iter_mut()
            {
                if let Some(waker) = waker.take() {
                    waker.wake()
                }
            }
        }
    }

    fn num_tickers(&self) -> usize {
        self.current_prices.len()
    }

    fn desired_positions(&self) -> Vec<Position> {
        let num_tickers = self.num_tickers();
        self.current_prices
            .iter()
            .map(move |(t, p)| {
                let p0 = self.start_prices.get(t).unwrap_or(p);
                let total = self.virtual_cash / num_tickers as f64
                    - (p * self.starting_cash) / (p0 * num_tickers as f64);
                debug!("Ticker: {}. Total desired: {}", t, total);
                Position {
                    ticker: t.clone(),
                    shares: total,
                }
            })
            .collect()
    }
}
pub struct Algorithm {
    sender: Sender,
    receiver: Receiver,
}

impl Algorithm {
    pub fn new(cash: f64) -> Self {
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let outbox = Arc::new(Mutex::new(VecDeque::new()));
        let sender = Sender::new(Arc::clone(&wakers), Arc::clone(&outbox));
        let receiver = Receiver::new(cash, wakers, outbox);
        Self { sender, receiver }
    }

    pub fn split(self) -> (Sender, Receiver) {
        (self.sender, self.receiver)
    }
}
