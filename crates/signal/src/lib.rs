use std::{collections::HashMap, error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnixNanos(u64);

impl UnixNanos {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct InstrumentId(String);

impl InstrumentId {
    /// Creates an instrument id from a platform-normalized symbol.
    ///
    /// # Errors
    ///
    /// Returns an error when the symbol is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, SignalError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SignalError::EmptyInstrumentId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawSignal {
    pub timestamp: UnixNanos,
    pub available_at: Option<UnixNanos>,
    pub instrument_id: InstrumentId,
    pub signal: Option<f64>,
    pub confidence: Option<f64>,
    pub target_position: Option<f64>,
    pub valid_until: Option<UnixNanos>,
    pub source_row_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserSignal {
    pub instrument_id: InstrumentId,
    pub signal: Option<f64>,
    pub confidence: Option<f64>,
    pub target_position: Option<f64>,
    pub valid_until: Option<UnixNanos>,
    pub source_row_id: Option<u64>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl TryFrom<RawSignal> for UserSignal {
    type Error = SignalError;

    fn try_from(raw: RawSignal) -> Result<Self, Self::Error> {
        validate_optional_finite("signal", raw.signal)?;
        validate_optional_finite("confidence", raw.confidence)?;
        validate_optional_finite("target_position", raw.target_position)?;

        if raw.signal.is_none() && raw.target_position.is_none() {
            return Err(SignalError::MissingExecutableIntent);
        }

        if let Some(confidence) = raw.confidence {
            if !(0.0..=1.0).contains(&confidence) {
                return Err(SignalError::ConfidenceOutOfRange(confidence.to_string()));
            }
        }

        let ts_init = raw.available_at.unwrap_or(raw.timestamp);
        if ts_init < raw.timestamp {
            return Err(SignalError::AvailableBeforeEvent {
                timestamp: raw.timestamp,
                available_at: ts_init,
            });
        }

        if let Some(valid_until) = raw.valid_until {
            if valid_until < ts_init {
                return Err(SignalError::ExpiredBeforeAvailable {
                    available_at: ts_init,
                    valid_until,
                });
            }
        }

        Ok(Self {
            instrument_id: raw.instrument_id,
            signal: raw.signal,
            confidence: raw.confidence,
            target_position: raw.target_position,
            valid_until: raw.valid_until,
            source_row_id: raw.source_row_id,
            ts_event: raw.timestamp,
            ts_init,
        })
    }
}

fn validate_optional_finite(field: &'static str, value: Option<f64>) -> Result<(), SignalError> {
    if value.is_some_and(|value| !value.is_finite()) {
        return Err(SignalError::NonFinite { field });
    }
    Ok(())
}

/// Validates deterministic replay preconditions for a normalized signal stream.
///
/// # Errors
///
/// Returns an error when signals are not sorted by `(ts_init, instrument_id)` or when duplicate
/// `(ts_event, instrument_id)` signals exist.
pub fn validate_signal_stream(signals: &[UserSignal]) -> Result<(), SignalError> {
    let mut previous: Option<(UnixNanos, InstrumentId, Option<u64>)> = None;
    let mut seen = HashMap::new();

    for signal in signals {
        if let Some((previous_ts_init, previous_instrument_id, previous_source_row_id)) = &previous
        {
            if (&signal.ts_init, &signal.instrument_id) < (previous_ts_init, previous_instrument_id)
            {
                return Err(SignalError::SignalStreamOutOfOrder {
                    previous_source_row_id: *previous_source_row_id,
                    source_row_id: signal.source_row_id,
                });
            }
        }

        let duplicate_key = (signal.ts_event, signal.instrument_id.clone());
        if let Some(first_source_row_id) = seen.insert(duplicate_key, signal.source_row_id) {
            return Err(SignalError::DuplicateSignal {
                timestamp: signal.ts_event,
                instrument_id: signal.instrument_id.as_str().to_string(),
                first_source_row_id,
                source_row_id: signal.source_row_id,
            });
        }

        previous = Some((
            signal.ts_init,
            signal.instrument_id.clone(),
            signal.source_row_id,
        ));
    }

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SignalError {
    EmptyInstrumentId,
    MissingExecutableIntent,
    NonFinite {
        field: &'static str,
    },
    ConfidenceOutOfRange(String),
    AvailableBeforeEvent {
        timestamp: UnixNanos,
        available_at: UnixNanos,
    },
    ExpiredBeforeAvailable {
        available_at: UnixNanos,
        valid_until: UnixNanos,
    },
    SignalStreamOutOfOrder {
        previous_source_row_id: Option<u64>,
        source_row_id: Option<u64>,
    },
    DuplicateSignal {
        timestamp: UnixNanos,
        instrument_id: String,
        first_source_row_id: Option<u64>,
        source_row_id: Option<u64>,
    },
}

impl fmt::Display for SignalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInstrumentId => f.write_str("instrument_id cannot be empty"),
            Self::MissingExecutableIntent => f.write_str("signal or target_position is required"),
            Self::NonFinite { field } => write!(f, "{field} must be finite"),
            Self::ConfidenceOutOfRange(value) => {
                write!(f, "confidence must be between 0.0 and 1.0, was {value}")
            }
            Self::AvailableBeforeEvent {
                timestamp,
                available_at,
            } => write!(
                f,
                "available_at {} cannot be before timestamp {}",
                available_at.as_u64(),
                timestamp.as_u64()
            ),
            Self::ExpiredBeforeAvailable {
                available_at,
                valid_until,
            } => write!(
                f,
                "valid_until {} cannot be before available_at {}",
                valid_until.as_u64(),
                available_at.as_u64()
            ),
            Self::SignalStreamOutOfOrder {
                previous_source_row_id,
                source_row_id,
            } => write!(
                f,
                "signal stream must be sorted by ts_init then instrument_id; row {source_row_id:?} follows row {previous_source_row_id:?}"
            ),
            Self::DuplicateSignal {
                timestamp,
                instrument_id,
                first_source_row_id,
                source_row_id,
            } => write!(
                f,
                "duplicate signal for timestamp {} and instrument `{instrument_id}` at rows {first_source_row_id:?} and {source_row_id:?}",
                timestamp.as_u64()
            ),
        }
    }
}

impl Error for SignalError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> InstrumentId {
        InstrumentId::new("AAPL.XNAS").expect("valid instrument")
    }

    fn signal(ts_event: u64, ts_init: u64, instrument_id: &str, source_row_id: u64) -> UserSignal {
        UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(ts_event),
            available_at: Some(UnixNanos::new(ts_init)),
            instrument_id: InstrumentId::new(instrument_id).expect("valid instrument"),
            signal: Some(1.0),
            confidence: None,
            target_position: None,
            valid_until: None,
            source_row_id: Some(source_row_id),
        })
        .expect("valid signal")
    }

    #[test]
    fn normalizes_default_availability_to_event_time() {
        let signal = UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(10),
            available_at: None,
            instrument_id: instrument(),
            signal: Some(1.0),
            confidence: Some(0.8),
            target_position: None,
            valid_until: None,
            source_row_id: Some(7),
        })
        .expect("valid signal");

        assert_eq!(signal.ts_event, UnixNanos::new(10));
        assert_eq!(signal.ts_init, UnixNanos::new(10));
        assert_eq!(signal.source_row_id, Some(7));
    }

    #[test]
    fn rejects_future_leakage_by_available_time() {
        let error = UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(10),
            available_at: Some(UnixNanos::new(9)),
            instrument_id: instrument(),
            signal: Some(1.0),
            confidence: None,
            target_position: None,
            valid_until: None,
            source_row_id: None,
        })
        .expect_err("available_at before event must fail");

        assert!(matches!(error, SignalError::AvailableBeforeEvent { .. }));
    }

    #[test]
    fn rejects_rows_without_executable_intent() {
        let error = UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(10),
            available_at: None,
            instrument_id: instrument(),
            signal: None,
            confidence: None,
            target_position: None,
            valid_until: None,
            source_row_id: None,
        })
        .expect_err("intent is required");

        assert_eq!(error, SignalError::MissingExecutableIntent);
    }

    #[test]
    fn sorted_signal_stream_passes() {
        let signals = [
            signal(10, 10, "AAPL.XNAS", 1),
            signal(10, 10, "MSFT.XNAS", 2),
            signal(12, 12, "AAPL.XNAS", 3),
        ];

        validate_signal_stream(&signals).expect("sorted stream");
    }

    #[test]
    fn out_of_order_signal_stream_fails_with_source_rows() {
        let signals = [
            signal(10, 12, "MSFT.XNAS", 1),
            signal(11, 12, "AAPL.XNAS", 2),
        ];

        let error = validate_signal_stream(&signals).expect_err("unordered stream");

        assert_eq!(
            error,
            SignalError::SignalStreamOutOfOrder {
                previous_source_row_id: Some(1),
                source_row_id: Some(2)
            }
        );
    }

    #[test]
    fn duplicate_signal_stream_fails_with_source_rows() {
        let signals = [
            signal(10, 10, "AAPL.XNAS", 1),
            signal(10, 11, "AAPL.XNAS", 2),
        ];

        let error = validate_signal_stream(&signals).expect_err("duplicate stream");

        assert_eq!(
            error,
            SignalError::DuplicateSignal {
                timestamp: UnixNanos::new(10),
                instrument_id: "AAPL.XNAS".to_string(),
                first_source_row_id: Some(1),
                source_row_id: Some(2)
            }
        );
    }
}
