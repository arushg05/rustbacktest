use std::{collections::HashMap, error::Error, fmt};

use backtest_alignment::ReplayEvent;
use backtest_signal::{InstrumentId, UnixNanos};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BacktestConfig {
    pub initial_cash: f64,
    pub long_threshold: f64,
    pub short_threshold: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BacktestResult {
    pub orders: Vec<Order>,
    pub fills: Vec<Fill>,
    pub final_equity: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Order {
    pub id: u64,
    pub instrument_id: InstrumentId,
    pub ts_init: UnixNanos,
    pub side: OrderSide,
    pub quantity: f64,
    pub target_position: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub order_id: u64,
    pub instrument_id: InstrumentId,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
    pub position_after: f64,
    pub cash_after: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EngineError {
    InvalidConfig(&'static str),
    MissingFillPrice {
        order_id: u64,
        instrument_id: String,
    },
}

pub fn run_backtest(
    events: &[ReplayEvent],
    config: &BacktestConfig,
) -> Result<BacktestResult, EngineError> {
    validate_config(config)?;

    let mut cash = config.initial_cash;
    let mut orders = Vec::new();
    let mut fills = Vec::new();
    let mut positions: HashMap<InstrumentId, f64> = HashMap::new();
    let mut pending: HashMap<InstrumentId, Order> = HashMap::new();
    let mut next_order_id = 1;

    for event in events {
        match event {
            ReplayEvent::Signal(signal) => {
                let desired = desired_position(signal, config);
                let current = positions
                    .get(&signal.instrument_id)
                    .copied()
                    .unwrap_or_default();

                if pending
                    .get(&signal.instrument_id)
                    .is_some_and(|order| order.target_position == desired)
                    || current == desired
                {
                    continue;
                }

                let quantity = desired - current;
                if quantity == 0.0 {
                    continue;
                }

                let order = Order {
                    id: next_order_id,
                    instrument_id: signal.instrument_id.clone(),
                    ts_init: signal.ts_init,
                    side: if quantity > 0.0 {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    quantity: quantity.abs(),
                    target_position: desired,
                };
                pending.insert(signal.instrument_id.clone(), order.clone());
                orders.push(order);
                next_order_id += 1;
            }
            ReplayEvent::Bar(bar) => {
                let Some(order) = pending.remove(&bar.instrument_id) else {
                    continue;
                };
                let price = bar.close;
                if !price.is_finite() || price <= 0.0 {
                    return Err(EngineError::MissingFillPrice {
                        order_id: order.id,
                        instrument_id: order.instrument_id.as_str().to_string(),
                    });
                }

                let signed_quantity = match order.side {
                    OrderSide::Buy => order.quantity,
                    OrderSide::Sell => -order.quantity,
                };
                cash -= signed_quantity * price;
                positions.insert(order.instrument_id.clone(), order.target_position);
                fills.push(Fill {
                    order_id: order.id,
                    instrument_id: order.instrument_id.clone(),
                    ts_event: bar.ts_event,
                    ts_init: bar.ts_init,
                    side: order.side,
                    quantity: order.quantity,
                    price,
                    position_after: order.target_position,
                    cash_after: cash,
                });
            }
        }
    }

    Ok(BacktestResult {
        orders,
        fills,
        final_equity: cash + mark_to_market(&positions, events)?,
    })
}

fn validate_config(config: &BacktestConfig) -> Result<(), EngineError> {
    for (field, value) in [
        ("initial_cash", config.initial_cash),
        ("long_threshold", config.long_threshold),
        ("short_threshold", config.short_threshold),
    ] {
        if !value.is_finite() {
            return Err(EngineError::InvalidConfig(field));
        }
    }
    if config.long_threshold <= config.short_threshold {
        return Err(EngineError::InvalidConfig("thresholds"));
    }
    Ok(())
}

fn desired_position(signal: &backtest_signal::UserSignal, config: &BacktestConfig) -> f64 {
    if let Some(target_position) = signal.target_position {
        return target_position;
    }

    let signal_value = signal.signal.unwrap_or_default();
    if signal_value >= config.long_threshold {
        1.0
    } else if signal_value <= config.short_threshold {
        -1.0
    } else {
        0.0
    }
}

fn mark_to_market(
    positions: &HashMap<InstrumentId, f64>,
    events: &[ReplayEvent],
) -> Result<f64, EngineError> {
    let mut last_close: HashMap<InstrumentId, f64> = HashMap::new();
    for event in events {
        let ReplayEvent::Bar(bar) = event else {
            continue;
        };
        if !bar.close.is_finite() || bar.close <= 0.0 {
            return Err(EngineError::MissingFillPrice {
                order_id: 0,
                instrument_id: bar.instrument_id.as_str().to_string(),
            });
        }
        last_close.insert(bar.instrument_id.clone(), bar.close);
    }

    let mut value = 0.0;
    for (instrument_id, position) in positions {
        if *position == 0.0 {
            continue;
        }
        let Some(price) = last_close.get(instrument_id) else {
            return Err(EngineError::MissingFillPrice {
                order_id: 0,
                instrument_id: instrument_id.as_str().to_string(),
            });
        };
        value += position * price;
    }
    Ok(value)
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(field) => write!(f, "invalid backtest config field `{field}`"),
            Self::MissingFillPrice {
                order_id,
                instrument_id,
            } => write!(
                f,
                "order {order_id} for instrument `{instrument_id}` cannot fill without a finite positive bar close"
            ),
        }
    }
}

impl Error for EngineError {}

#[cfg(feature = "nautilus")]
#[derive(Clone, Debug, PartialEq)]
pub struct NautilusSignalData {
    pub instrument_id: String,
    pub ts_event: u64,
    pub ts_init: u64,
    pub signal: Option<f64>,
    pub confidence: Option<f64>,
    pub target_position: Option<f64>,
    pub source_row_id: Option<u64>,
}

#[cfg(feature = "nautilus")]
impl From<&backtest_signal::UserSignal> for NautilusSignalData {
    fn from(signal: &backtest_signal::UserSignal) -> Self {
        Self {
            instrument_id: signal.instrument_id.as_str().to_string(),
            ts_event: signal.ts_event.as_u64(),
            ts_init: signal.ts_init.as_u64(),
            signal: signal.signal,
            confidence: signal.confidence,
            target_position: signal.target_position,
            source_row_id: signal.source_row_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backtest_market_data::{Bar, RawBar};
    use backtest_signal::{RawSignal, UserSignal};

    fn id() -> InstrumentId {
        InstrumentId::new("AAPL.XNAS").expect("valid instrument")
    }

    fn config() -> BacktestConfig {
        BacktestConfig {
            initial_cash: 1_000.0,
            long_threshold: 0.5,
            short_threshold: -0.5,
        }
    }

    fn signal(value: f64, target_position: Option<f64>, ts_init: u64) -> ReplayEvent {
        ReplayEvent::Signal(
            UserSignal::try_from(RawSignal {
                timestamp: UnixNanos::new(ts_init),
                available_at: Some(UnixNanos::new(ts_init)),
                instrument_id: id(),
                signal: Some(value),
                confidence: None,
                target_position,
                valid_until: None,
                source_row_id: None,
            })
            .expect("valid signal"),
        )
    }

    fn bar(close: f64, ts_init: u64) -> ReplayEvent {
        ReplayEvent::Bar(
            Bar::try_from(RawBar {
                instrument_id: id(),
                ts_event: UnixNanos::new(ts_init - 1),
                ts_init: UnixNanos::new(ts_init),
                open: close,
                high: close,
                low: close,
                close,
                volume: 100.0,
            })
            .expect("valid bar"),
        )
    }

    #[test]
    fn long_signal_fills_on_next_bar_close() {
        let result = run_backtest(&[signal(0.8, None, 10), bar(101.0, 11)], &config())
            .expect("backtest runs");

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].side, OrderSide::Buy);
        assert_eq!(result.fills[0].price, 101.0);
        assert_eq!(result.final_equity, 1_000.0);
    }

    #[test]
    fn repeated_same_target_does_not_churn_orders() {
        let result = run_backtest(
            &[signal(0.8, None, 10), signal(0.9, None, 10), bar(101.0, 11)],
            &config(),
        )
        .expect("backtest runs");

        assert_eq!(result.orders.len(), 1);
        assert_eq!(result.fills.len(), 1);
    }

    #[test]
    fn thresholds_drive_long_flat_short_targets() {
        let result = run_backtest(
            &[
                signal(0.8, None, 10),
                bar(100.0, 11),
                signal(0.0, None, 12),
                bar(102.0, 13),
                signal(-0.8, None, 14),
                bar(101.0, 15),
            ],
            &config(),
        )
        .expect("backtest runs");

        assert_eq!(
            result
                .fills
                .iter()
                .map(|fill| fill.position_after)
                .collect::<Vec<_>>(),
            vec![1.0, 0.0, -1.0]
        );
    }

    #[test]
    fn missing_price_prevents_impossible_fill() {
        let error =
            run_backtest(&[signal(0.8, None, 10), bar(0.0, 11)], &config()).expect_err("bad price");

        assert!(matches!(error, EngineError::MissingFillPrice { .. }));
    }

    #[cfg(feature = "nautilus")]
    #[test]
    fn nautilus_feature_converts_user_signal_to_adapter_payload() {
        let ReplayEvent::Signal(signal) = signal(0.8, Some(1.0), 10) else {
            panic!("signal event");
        };

        let data = NautilusSignalData::from(&signal);

        assert_eq!(data.instrument_id, "AAPL.XNAS");
        assert_eq!(data.ts_event, 10);
        assert_eq!(data.ts_init, 10);
        assert_eq!(data.target_position, Some(1.0));
    }
}
