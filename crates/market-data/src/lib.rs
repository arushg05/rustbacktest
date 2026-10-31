use std::{collections::HashMap, error::Error, fmt};

pub use backtest_signal::{InstrumentId, UnixNanos};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub symbol: String,
    pub venue: String,
}

impl Instrument {
    /// Creates a market data instrument.
    ///
    /// # Errors
    ///
    /// Returns an error when `symbol` or `venue` is empty.
    pub fn new(
        id: InstrumentId,
        symbol: impl Into<String>,
        venue: impl Into<String>,
    ) -> Result<Self, MarketDataError> {
        let symbol = symbol.into();
        let venue = venue.into();
        if symbol.trim().is_empty() {
            return Err(MarketDataError::EmptyInstrumentField { field: "symbol" });
        }
        if venue.trim().is_empty() {
            return Err(MarketDataError::EmptyInstrumentField { field: "venue" });
        }
        Ok(Self { id, symbol, venue })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawBar {
    pub instrument_id: InstrumentId,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Bar {
    pub instrument_id: InstrumentId,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl TryFrom<RawBar> for Bar {
    type Error = MarketDataError;

    fn try_from(raw: RawBar) -> Result<Self, Self::Error> {
        if raw.ts_init < raw.ts_event {
            return Err(MarketDataError::AvailableBeforeEvent {
                timestamp: raw.ts_event,
                available_at: raw.ts_init,
            });
        }

        for (field, value) in [
            ("open", raw.open),
            ("high", raw.high),
            ("low", raw.low),
            ("close", raw.close),
            ("volume", raw.volume),
        ] {
            if !value.is_finite() {
                return Err(MarketDataError::NonFinite { field });
            }
        }

        Ok(Self {
            instrument_id: raw.instrument_id,
            ts_event: raw.ts_event,
            ts_init: raw.ts_init,
            open: raw.open,
            high: raw.high,
            low: raw.low,
            close: raw.close,
            volume: raw.volume,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct MarketCatalog {
    instruments: HashMap<InstrumentId, Instrument>,
    bars: HashMap<InstrumentId, Vec<Bar>>,
}

impl MarketCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_instrument(&mut self, instrument: Instrument) {
        self.instruments.insert(instrument.id.clone(), instrument);
    }

    /// Adds a validated bar to the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error when the bar references an instrument not present in the catalog.
    pub fn add_bar(&mut self, bar: Bar) -> Result<(), MarketDataError> {
        if !self.instruments.contains_key(&bar.instrument_id) {
            return Err(MarketDataError::UnknownInstrument {
                instrument_id: bar.instrument_id.as_str().to_string(),
            });
        }

        let bars = self.bars.entry(bar.instrument_id.clone()).or_default();
        let index = bars
            .binary_search_by_key(&(bar.ts_event, bar.ts_init), |bar| {
                (bar.ts_event, bar.ts_init)
            })
            .unwrap_or_else(|index| index);
        bars.insert(index, bar);
        Ok(())
    }

    #[must_use]
    pub fn bars(
        &self,
        instrument_id: &InstrumentId,
        start: UnixNanos,
        end: UnixNanos,
    ) -> Vec<&Bar> {
        self.bars
            .get(instrument_id)
            .into_iter()
            .flatten()
            .filter(|bar| start <= bar.ts_event && bar.ts_event <= end)
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketDataError {
    AvailableBeforeEvent {
        timestamp: UnixNanos,
        available_at: UnixNanos,
    },
    EmptyInstrumentField {
        field: &'static str,
    },
    NonFinite {
        field: &'static str,
    },
    UnknownInstrument {
        instrument_id: String,
    },
}

impl fmt::Display for MarketDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AvailableBeforeEvent {
                timestamp,
                available_at,
            } => write!(
                f,
                "bar ts_init {} cannot be before ts_event {}",
                available_at.as_u64(),
                timestamp.as_u64()
            ),
            Self::EmptyInstrumentField { field } => write!(f, "instrument {field} cannot be empty"),
            Self::NonFinite { field } => write!(f, "{field} must be finite"),
            Self::UnknownInstrument { instrument_id } => {
                write!(f, "unknown instrument `{instrument_id}`")
            }
        }
    }
}

impl Error for MarketDataError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument_id() -> InstrumentId {
        InstrumentId::new("AAPL.XNAS").expect("valid instrument")
    }

    fn instrument(id: InstrumentId) -> Instrument {
        Instrument::new(id, "AAPL", "XNAS").expect("valid instrument")
    }

    fn bar(instrument_id: InstrumentId, ts_event: u64) -> Bar {
        Bar::try_from(RawBar {
            instrument_id,
            ts_event: UnixNanos::new(ts_event),
            ts_init: UnixNanos::new(ts_event + 1),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 10_000.0,
        })
        .expect("valid bar")
    }

    #[test]
    fn range_query_includes_start_and_end_boundaries() {
        let id = instrument_id();
        let mut catalog = MarketCatalog::new();
        catalog.add_instrument(instrument(id.clone()));

        for ts_event in [9, 10, 15, 20, 21] {
            catalog
                .add_bar(bar(id.clone(), ts_event))
                .expect("known instrument");
        }

        let bars = catalog.bars(&id, UnixNanos::new(10), UnixNanos::new(20));

        assert_eq!(
            bars.iter()
                .map(|bar| bar.ts_event.as_u64())
                .collect::<Vec<_>>(),
            vec![10, 15, 20]
        );
    }

    #[test]
    fn unknown_instrument_query_is_empty() {
        let catalog = MarketCatalog::new();
        let id = instrument_id();

        assert!(
            catalog
                .bars(&id, UnixNanos::new(1), UnixNanos::new(2))
                .is_empty()
        );
    }

    #[test]
    fn rejects_bar_available_before_event() {
        let error = Bar::try_from(RawBar {
            instrument_id: instrument_id(),
            ts_event: UnixNanos::new(10),
            ts_init: UnixNanos::new(9),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 10_000.0,
        })
        .expect_err("ts_init before ts_event must fail");

        assert!(matches!(
            error,
            MarketDataError::AvailableBeforeEvent { .. }
        ));
    }

    #[test]
    fn rejects_empty_instrument_symbol() {
        let error =
            Instrument::new(instrument_id(), " ", "XNAS").expect_err("empty symbol must fail");

        assert_eq!(
            error,
            MarketDataError::EmptyInstrumentField { field: "symbol" }
        );
    }
}
