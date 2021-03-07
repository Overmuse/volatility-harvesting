use alpaca::{
    common::Order,
    common::Side,
    orders::{AmountSpec, OrderIntent},
    AlpacaMessage, Event, OrderEvent,
};
use core::pin::Pin;
use futures::prelude::*;
use futures::task::{Context, Poll, Waker};
use log::{debug, info, trace, warn};
use polygon::ws::PolygonMessage;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(PartialEq, Debug)]
pub struct Position {
    pub ticker: String,
    pub shares: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Alpaca(AlpacaMessage),
    Polygon(PolygonMessage),
    Latch,
}

pub struct Sender {
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    outbox: Arc<Mutex<VecDeque<OrderIntent>>>,
}

impl Sender {
    fn new(
        wakers: Arc<Mutex<Vec<Option<Waker>>>>,
        outbox: Arc<Mutex<VecDeque<OrderIntent>>>,
    ) -> Self {
        Self { outbox, wakers }
    }
}

impl Stream for Sender {
    type Item = OrderIntent;

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
    cash: f64,
    virtual_cash: f64,
    latched: bool,
    start_prices: BTreeMap<String, f64>,
    prev_prices: BTreeMap<String, f64>,
    current_prices: BTreeMap<String, f64>,
    pending_orders: HashMap<String, OrderIntent>,
    positions: BTreeMap<String, Position>,
    wakers: Arc<Mutex<Vec<Option<Waker>>>>,
    sender_outbox: Arc<Mutex<VecDeque<OrderIntent>>>,
}

impl Receiver {
    fn new(
        cash: f64,
        wakers: Arc<Mutex<Vec<Option<Waker>>>>,
        sender_outbox: Arc<Mutex<VecDeque<OrderIntent>>>,
    ) -> Self {
        Self {
            starting_cash: cash,
            cash,
            virtual_cash: cash,
            latched: false,
            start_prices: BTreeMap::new(),
            prev_prices: BTreeMap::new(),
            current_prices: BTreeMap::new(),
            pending_orders: HashMap::new(),
            positions: BTreeMap::new(),
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
            Message::Alpaca(msg) => match msg {
                AlpacaMessage::TradeUpdates(order_event) => self.update_position(*order_event),
                _ => return Err("Unexpected alpaca message".into()),
            },
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

    fn update_position(&mut self, event: OrderEvent) {
        match event.event {
            Event::Canceled { .. } => self.fail_order(event.order),
            Event::Fill { price, qty, .. } => {
                let pos = self
                    .positions
                    .entry(event.order.symbol.clone())
                    // qty here is total position quantity, NOT just the qty that was filled with
                    // this order
                    .and_modify(|current_position| current_position.shares = qty as f64)
                    .or_insert(Position {
                        ticker: event.order.symbol.clone(),
                        shares: qty as f64,
                    });
                let oi = self
                    .pending_orders
                    .remove(&event.order.client_order_id.unwrap());
                if let Some(oi) = oi {
                    info!("Order filled: {:?}.", oi)
                }
                self.start_prices
                    .entry(event.order.symbol.clone())
                    .or_insert(price);
                if pos.shares == 0.0 {
                    self.positions.remove(&event.order.symbol);
                }
                self.cash -= price * event.order.filled_qty as f64;
            }
            Event::PartialFill { price, qty, .. } => {
                let pos = self
                    .positions
                    .entry(event.order.symbol.clone())
                    .and_modify(|current_position| current_position.shares = qty as f64)
                    .or_insert(Position {
                        ticker: event.order.symbol.clone(),
                        shares: qty as f64,
                    });
                self.pending_orders
                    .entry(event.order.client_order_id.clone().unwrap())
                    .and_modify(|oi| {
                        oi.amount = AmountSpec::Quantity(event.order.qty - event.order.filled_qty);
                    });
                info!("Order partially filled: {:?}.", event.order);
                self.start_prices
                    .entry(event.order.symbol.clone())
                    .or_insert(price);
                if pos.shares == 0.0 {
                    self.positions.remove(&event.order.symbol);
                }
                self.cash -= price * event.order.filled_qty as f64;
            }
            Event::Expired { .. } => self.fail_order(event.order),
            Event::Rejected { .. } => self.fail_order(event.order),
            _ => warn!("Don't know how to handle alpaca message: {:?}", event),
        }
    }

    fn fail_order(&mut self, order: Order) {
        let oi = self.pending_orders.remove(&order.client_order_id.unwrap());
        if let Some(oi) = oi {
            warn!("Order canceled: {:?}.", oi)
        }
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
        let trades = self.generate_trades();
        for trade in trades {
            self.sender_outbox
                .lock()
                .expect("Failed to acquire Mutex")
                .push_back(trade);
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
        // let portfolio_value: f64 = self
        //     .positions
        //     .iter()
        //     .map(|(ticker, pos)| match self.current_prices.get(ticker) {
        //         Some(price) => price * pos.shares,
        //         None => 0.0,
        //     })
        //     .sum();
        self.current_prices
            .iter()
            .map(move |(t, p)| {
                let p0 = self.start_prices.get(t).unwrap_or(p);
                let total = self.virtual_cash / num_tickers as f64
                    - (p * self.starting_cash) / (p0 * num_tickers as f64);
                let current = match self.positions.get(t) {
                    Some(pos) => pos.shares * p,
                    None => 0.0,
                };
                let pending: f64 = self
                    .pending_orders
                    .values()
                    .filter_map(|order| {
                        if &order.symbol == t {
                            match order.amount {
                                AmountSpec::Quantity(qty) => match order.side {
                                    Side::Buy => Some(qty),
                                    Side::Sell => Some(-qty),
                                },
                                AmountSpec::Notional(..) => {
                                    panic!("Notional quantity should never be specified")
                                }
                            }
                        } else {
                            None
                        }
                    })
                    .sum();
                debug!(
                    "Ticker: {}. Total desired: {}, current: {}, pending: {}",
                    t, total, current, pending
                );
                Position {
                    ticker: t.clone(),
                    shares: total - current - pending,
                }
            })
            .collect()
    }

    fn generate_trades(&mut self) -> Vec<OrderIntent> {
        self.desired_positions()
            .iter()
            .filter_map(|pos| {
                if pos.shares.abs() < 0.01 {
                    None
                } else {
                    let side = if pos.shares.is_sign_positive() {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let order_id = Uuid::new_v4().to_string();
                    let oi = OrderIntent::new(&pos.ticker)
                        .qty(pos.shares.abs())
                        .side(side)
                        .client_order_id(order_id.clone());
                    self.pending_orders.insert(order_id, oi.clone());
                    info!("Generated trade: {:?}", &oi);
                    Some(oi)
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

// #[cfg(test)]
// mod test {
//     use super::*;
//
//     #[test]
//     fn test_algo() {
//         let mut algo = Algorithm::new(1000000.0);

// // Adding in one instrument should not lead to trades
// algo.update_price("AAPL", 100.0);
// assert_eq!(algo.num_tickers(), 1);
// assert_eq!(algo.start_prices.get("AAPL".into()), None);
// assert_eq!(algo.current_prices.get("AAPL".into()), Some(&100.0));
// assert_eq!(
//     algo.desired_positions(),
//     vec![Position {
//         ticker: "AAPL".into(),
//         shares: 0.0
//     }]
// );

// // Adding new instrument should not lead to trades
// algo.update_price("TSLA", 30.0);
// assert_eq!(algo.num_tickers(), 2);
// assert_eq!(algo.start_prices.get("TSLA".into()), None);
// assert_eq!(algo.current_prices.get("TSLA".into()), Some(&30.0));
// assert_eq!(
//     algo.desired_positions(),
//     vec![
//         Position {
//             ticker: "AAPL".into(),
//             shares: 0.0
//         },
//         Position {
//             ticker: "TSLA".into(),
//             shares: 0.0
//         }
//     ]
// );

// // Purchasing an unwanted position should recommend selling it back
// algo.update_position("TSLA", 1.0, 30.0);
// assert_eq!(
//     algo.desired_positions(),
//     vec![
//         Position {
//             ticker: "AAPL".into(),
//             shares: 0.0
//         },
//         Position {
//             ticker: "TSLA".into(),
//             shares: -30.0
//         }
//     ]
// );
// assert_eq!(rx.recv().unwrap(), OrderIntent::new("TSLA"));
// algo.update_position("TSLA", -1.0, 30.0);
// assert_eq!(
//     algo.desired_positions(),
//     vec![
//         Position {
//             ticker: "AAPL".into(),
//             shares: 0.0
//         },
//         Position {
//             ticker: "TSLA".into(),
//             shares: 0.0
//         }
//     ]
// );

// // Change in price should lead to rebalancing
// algo.update_position("AAPL", -50.0, 100.0);
// assert_eq!(algo.start_prices.get("AAPL".into()), Some(&100.0));
// algo.update_price("AAPL", 101.0);
// assert_eq!(algo.start_prices.get("AAPL".into()), Some(&100.0));
// assert_eq!(algo.current_prices.get("AAPL".into()), Some(&101.0));
// assert_eq!(
//     algo.desired_positions(),
//     vec![
//         Position {
//             ticker: "AAPL".into(),
//             shares: 25.0
//         },
//         Position {
//             ticker: "TSLA".into(),
//             shares: -25.0
//         }
//     ]
// );

// // Purchasing recommended position should set recommendation back to 0
// algo.update_position("AAPL", 25.0 / 101.0, 101.0);
// algo.update_position("TSLA", -25.0 / 30.0, 30.0);
// assert_eq!(
//     algo.desired_positions(),
//     vec![
//         Position {
//             ticker: "AAPL".into(),
//             shares: 0.0
//         },
//         Position {
//             ticker: "TSLA".into(),
//             shares: 0.0
//         }
//     ]
// );

// // New asset when others have changed in price will lead to trading
// algo.update_price("MSFT", 200.0);
// assert_eq!(algo.num_tickers(), 3);
// assert_ne!(algo.desired_positions(), [0.0, 0.0, 0.0]);

// Same recommendation when trading assets that haven't been traded yet
// let mut algo2 = Algorithm::new(1000.0);
// algo2.update_price("AAPL", 10.0);
// algo2.update_price("TSLA", 20.0);

// let mut algo3 = Algorithm::new(1000.0);
// algo3.update_price("AAPL", 5.0);
// algo3.update_price("TSLA", 15.0);
// algo3.update_price("AAPL", 10.0);
// algo3.update_price("TSLA", 20.0);
// assert_eq!(algo2.desired_positions(), algo3.desired_positions());
//     }
// }
