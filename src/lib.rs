use anyhow::Error;
use chrono::{DateTime, Utc};
use core::pin::Pin;
use futures::prelude::*;
use futures::task::{Context, Poll, Waker};
use log::{debug, info, trace, warn};
use polygon::ws::{PolygonMessage, Trade, TradeCondition};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{interval, Interval};

mod settings;
pub use settings::Settings;

lazy_static::lazy_static! {
    static ref ELEGIBLE_CONDITIONS: HashSet<TradeCondition> = {
        let mut hash = HashSet::new();
        hash.insert(TradeCondition::RegularSale);
        hash.insert(TradeCondition::Acquisition);
        hash.insert(TradeCondition::AutomaticExecution);
        hash.insert(TradeCondition::BunchedTrade);
        hash.insert(TradeCondition::ClosingPrints);
        hash.insert(TradeCondition::CrossTrade);
        hash.insert(TradeCondition::Distribution);
        hash.insert(TradeCondition::IntermarketSweep);
        hash.insert(TradeCondition::Rule155Trade);
        hash.insert(TradeCondition::OpeningPrints);
        hash.insert(TradeCondition::StoppedStockRegularTrade);
        hash.insert(TradeCondition::ReopeningPrints);
        hash.insert(TradeCondition::SoldLast);
        hash.insert(TradeCondition::SplitTrade);
        hash.insert(TradeCondition::YellowFlagRegularTrade);
        hash.insert(TradeCondition::CorrectedConsolidatedClose);
        hash
    };
}

// TODO: Define this struct in some shared schema registry
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum State {
    Open { next_close: usize },
    Closed { next_open: usize },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Polygon(PolygonMessage),
    State(State),
    Latch,
    Unlatch,
}

// TODO: Define this struct in some shared schema registry
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PositionIntent {
    pub strategy: String,
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub qty: i32,
}

pub struct Sender {
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    outbox: Arc<Mutex<VecDeque<PositionIntent>>>,
}

impl Sender {
    fn new(
        wakers: Arc<Mutex<Vec<Option<Waker>>>>,
        outbox: Arc<Mutex<VecDeque<PositionIntent>>>,
    ) -> Self {
        Self { outbox, wakers }
    }
}

impl Stream for Sender {
    type Item = PositionIntent;

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
            let msg = outbox.pop_front();
            debug!("Message available: {:?}", msg);
            Poll::Ready(msg)
        }
    }
}

pub struct Receiver {
    starting_cash: f64,
    virtual_cash: f64,
    leverage: f64,
    latched: bool,
    start_prices: BTreeMap<String, f64>,
    prev_prices: BTreeMap<String, f64>,
    current_prices: BTreeMap<String, f64>,
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    sender_outbox: Arc<Mutex<VecDeque<PositionIntent>>>,
    interval: Interval,
}

impl Receiver {
    fn new(
        starting_cash: f64,
        leverage: f64,
        wakers: Arc<Mutex<Vec<Option<Waker>>>>,
        sender_outbox: Arc<Mutex<VecDeque<PositionIntent>>>,
        batch_time: Duration,
    ) -> Self {
        Self {
            starting_cash,
            virtual_cash: starting_cash,
            leverage,
            latched: false,
            start_prices: BTreeMap::new(),
            prev_prices: BTreeMap::new(),
            current_prices: BTreeMap::new(),
            sender_outbox,
            wakers,
            interval: interval(batch_time),
        }
    }
}

impl Sink<Message> for Receiver {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        match item {
            Message::Polygon(msg) => {
                if let PolygonMessage::Trade(trade) = msg {
                    self.update_price(trade)
                }
            }
            Message::Latch => self.latch(),
            Message::Unlatch => self.unlatch(),
            Message::State(state) => match (self.latched, state) {
                (true, State::Open { next_close }) => {
                    if next_close < 600 {
                        self.unlatch()
                    }
                }
                (false, State::Open { next_close }) => {
                    if next_close > 600 {
                        self.latch()
                    }
                }
                (true, State::Closed { .. }) => {
                    warn!("Market closed before unlatching");
                    self.unlatch()
                }
                _ => (),
            },
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.interval.poll_tick(cx).is_ready() {
            let desired_positions = self.desired_positions();
            let mut outbox = self
                .sender_outbox
                .lock()
                .expect("Encountered poisoned lock");
            for position in desired_positions {
                outbox.push_back(position);
            }
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

    fn unlatch(&mut self) {
        self.start_prices.clear();
        self.latched = false;
        self.starting_cash = self.virtual_cash;
        info!("Unlatching prices")
    }

    fn update_price(&mut self, trade: Trade) {
        // Filter out any trades that don't have all-okay conditions
        if trade
            .conditions
            .iter()
            .all(|c| ELEGIBLE_CONDITIONS.contains(c))
        {
            let prev = self
                .current_prices
                .insert(trade.symbol.clone(), trade.price);
            if let Some(prev_price) = prev {
                self.prev_prices.insert(trade.symbol.clone(), prev_price);
                if self.latched {
                    self.virtual_cash *=
                        1.0 + (trade.price / prev_price - 1.0) / self.num_tickers() as f64;
                }
            }
        }
    }

    fn num_tickers(&self) -> usize {
        self.current_prices.len()
    }

    fn desired_positions(&self) -> Vec<PositionIntent> {
        let num_tickers = self.num_tickers();
        self.current_prices
            .iter()
            .map(move |(t, p)| {
                let p0 = self.start_prices.get(t).unwrap_or(p);
                let total = self.virtual_cash / num_tickers as f64
                    - (p * self.starting_cash) / (p0 * num_tickers as f64);
                let shares = self.leverage * total / *p as f64;
                debug!("Ticker: {}. Total desired: {}", t, total);
                PositionIntent {
                    ticker: t.clone(),
                    strategy: "volatility-harvesting".into(),
                    qty: shares as i32,
                    timestamp: Utc::now(),
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
    pub fn new(cash: f64, leverage: f64, batch_time: Duration) -> Self {
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let outbox = Arc::new(Mutex::new(VecDeque::new()));
        let sender = Sender::new(Arc::clone(&wakers), Arc::clone(&outbox));
        let receiver = Receiver::new(cash, leverage, wakers, outbox, batch_time);
        Self { sender, receiver }
    }

    pub fn split(self) -> (Sender, Receiver) {
        (self.sender, self.receiver)
    }
}
