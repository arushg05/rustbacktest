use std::{collections::HashMap, error::Error, fmt};

use backtest_market_data::Bar;
use backtest_signal::{InstrumentId, SignalError, UnixNanos, UserSignal, validate_signal_stream};

#[derive(Clone, Debug, PartialEq)]
pub enum ReplayEvent {
    Bar(Bar),
    Signal(UserSignal),
}

impl ReplayEvent {
    #[must_use]
    pub const fn ts_init(&self) -> UnixNanos {
        match self {
            Self::Bar(bar) => bar.ts_init,
            Self::Signal(signal) => signal.ts_init,
        }
    }

    fn instrument_id(&self) -> &InstrumentId {
        match self {
            Self::Bar(bar) => &bar.instrument_id,
            Self::Signal(signal) => &signal.instrument_id,
        }
    }

    const fn priority(&self) -> u8 {
        match self {
            // Bars are replayed before same-time signals so a decision at T can see market data
            // that became available at T, but never market data available after T.
            Self::Bar(_) => 0,
            Self::Signal(_) => 1,
        }
    }
}

/// Builds the deterministic point-in-time replay order.
///
/// # Errors
///
/// Returns an error when the signal stream is invalid, a bar exposes future data, or a signal has
/// no same-instrument bar available at or before its decision time.
pub fn build_replay_events(
    signals: Vec<UserSignal>,
    bars: Vec<Bar>,
) -> Result<Vec<ReplayEvent>, AlignmentError> {
    validate_signal_stream(&signals).map_err(AlignmentError::Signal)?;
    validate_bars(&bars)?;
    validate_market_coverage(&signals, &bars)?;

    let mut events = Vec::with_capacity(signals.len() + bars.len());
    events.extend(bars.into_iter().map(ReplayEvent::Bar));
    events.extend(signals.into_iter().map(ReplayEvent::Signal));
    events.sort_by(|left, right| {
        (left.ts_init(), left.priority(), left.instrument_id()).cmp(&(
            right.ts_init(),
            right.priority(),
            right.instrument_id(),
        ))
    });
    Ok(events)
}

fn validate_bars(bars: &[Bar]) -> Result<(), AlignmentError> {
    for bar in bars {
        if bar.ts_event > bar.ts_init {
            return Err(AlignmentError::FutureMarketData {
                instrument_id: bar.instrument_id.as_str().to_string(),
                bar_ts_event: bar.ts_event,
                bar_ts_init: bar.ts_init,
            });
        }
    }
    Ok(())
}

fn validate_market_coverage(signals: &[UserSignal], bars: &[Bar]) -> Result<(), AlignmentError> {
    let mut latest_available_bar: HashMap<&InstrumentId, UnixNanos> = HashMap::new();
    for bar in bars {
        latest_available_bar
            .entry(&bar.instrument_id)
            .and_modify(|latest| *latest = (*latest).max(bar.ts_init))
            .or_insert(bar.ts_init);
    }

    for signal in signals {
        if !bars
            .iter()
            .any(|bar| bar.instrument_id == signal.instrument_id && bar.ts_init <= signal.ts_init)
        {
            return Err(AlignmentError::MissingMarketCoverage {
                instrument_id: signal.instrument_id.as_str().to_string(),
                signal_ts_init: signal.ts_init,
                latest_available_bar: latest_available_bar.get(&signal.instrument_id).copied(),
                source_row_id: signal.source_row_id,
            });
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlignmentError {
    Signal(SignalError),
    FutureMarketData {
        instrument_id: String,
        bar_ts_event: UnixNanos,
        bar_ts_init: UnixNanos,
    },
    MissingMarketCoverage {
        instrument_id: String,
        signal_ts_init: UnixNanos,
        latest_available_bar: Option<UnixNanos>,
        source_row_id: Option<u64>,
    },
}

impl fmt::Display for AlignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signal(error) => write!(f, "{error}"),
            Self::FutureMarketData {
                instrument_id,
                bar_ts_event,
                bar_ts_init,
            } => write!(
                f,
                "bar for `{instrument_id}` has event time {} after available time {}",
                bar_ts_event.as_u64(),
                bar_ts_init.as_u64()
            ),
            Self::MissingMarketCoverage {
                instrument_id,
                signal_ts_init,
                latest_available_bar,
                source_row_id,
            } => write!(
                f,
                "signal row {source_row_id:?} for `{instrument_id}` at {} has no market data available at or before that time; latest bar was {:?}",
                signal_ts_init.as_u64(),
                latest_available_bar.map(UnixNanos::as_u64)
            ),
        }
    }
}

impl Error for AlignmentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Signal(error) => Some(error),
            Self::FutureMarketData { .. } | Self::MissingMarketCoverage { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backtest_market_data::RawBar;
    use backtest_signal::{RawSignal, UserSignal};

    fn instrument_id() -> InstrumentId {
        InstrumentId::new("AAPL.XNAS").expect("valid instrument")
    }

    fn bar(ts_event: u64, ts_init: u64) -> Bar {
        Bar::try_from(RawBar {
            instrument_id: instrument_id(),
            ts_event: UnixNanos::new(ts_event),
            ts_init: UnixNanos::new(ts_init),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1_000.0,
        })
        .expect("valid bar")
    }

    fn signal(ts_event: u64, ts_init: u64, source_row_id: u64) -> UserSignal {
        UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(ts_event),
            available_at: Some(UnixNanos::new(ts_init)),
            instrument_id: instrument_id(),
            signal: Some(1.0),
            confidence: None,
            target_position: None,
            valid_until: None,
            source_row_id: Some(source_row_id),
        })
        .expect("valid signal")
    }

    #[test]
    fn signal_before_next_bar_orders_correctly() {
        let events = build_replay_events(vec![signal(10, 12, 1)], vec![bar(9, 9), bar(13, 13)])
            .expect("aligned events");

        assert!(matches!(events[0], ReplayEvent::Bar(_)));
        assert!(matches!(events[1], ReplayEvent::Signal(_)));
        assert!(matches!(events[2], ReplayEvent::Bar(_)));
    }

    #[test]
    fn same_timestamp_replays_bar_before_signal() {
        let events =
            build_replay_events(vec![signal(10, 10, 1)], vec![bar(10, 10)]).expect("aligned");

        assert!(matches!(events[0], ReplayEvent::Bar(_)));
        assert!(matches!(events[1], ReplayEvent::Signal(_)));
    }

    #[test]
    fn missing_market_coverage_fails() {
        let error = build_replay_events(vec![signal(10, 10, 7)], vec![bar(11, 11)])
            .expect_err("signal before first available bar must fail");

        assert_eq!(
            error,
            AlignmentError::MissingMarketCoverage {
                instrument_id: "AAPL.XNAS".to_string(),
                signal_ts_init: UnixNanos::new(10),
                latest_available_bar: Some(UnixNanos::new(11)),
                source_row_id: Some(7)
            }
        );
    }

    #[test]
    fn future_market_data_fails() {
        let leaking_bar = Bar {
            instrument_id: instrument_id(),
            ts_event: UnixNanos::new(11),
            ts_init: UnixNanos::new(10),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1_000.0,
        };

        let error = build_replay_events(vec![signal(10, 10, 1)], vec![leaking_bar])
            .expect_err("future bar must fail");

        assert!(matches!(error, AlignmentError::FutureMarketData { .. }));
    }
}
