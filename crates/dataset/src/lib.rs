use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    io::Read,
};

use backtest_signal::{InstrumentId, RawSignal, SignalError, UnixNanos, UserSignal};

mod parquet;

pub use parquet::{inspect_parquet, read_signals_parquet};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ColumnRole {
    Timestamp,
    AvailableAt,
    Symbol,
    Signal,
    Confidence,
    TargetPosition,
    ValidUntil,
    Feature,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub role: ColumnRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetSchema {
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetReport {
    pub schema: DatasetSchema,
    pub row_count: usize,
    pub signal_count: usize,
    pub feature_count: usize,
    pub warnings: Vec<DatasetWarning>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatasetWarning {
    MissingOptionalColumn(&'static str),
    EmptyFeatureColumn {
        column: String,
    },
    SparseFeatureColumn {
        column: String,
        present: usize,
        total: usize,
    },
}

impl DatasetSchema {
    /// Classifies raw column names using the MVP signal contract.
    ///
    /// # Errors
    ///
    /// Returns an error when required executable columns are missing.
    pub fn infer(
        column_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatasetError> {
        let columns = column_names
            .into_iter()
            .map(|name| {
                let name = name.into();
                let role = classify_column(&name);
                Column { name, role }
            })
            .collect::<Vec<_>>();

        let roles = columns
            .iter()
            .map(|column| column.role)
            .collect::<HashSet<_>>();
        if !roles.contains(&ColumnRole::Timestamp) {
            return Err(DatasetError::MissingColumn("timestamp"));
        }
        if !roles.contains(&ColumnRole::Symbol) {
            return Err(DatasetError::MissingColumn("symbol"));
        }
        if !roles.contains(&ColumnRole::Signal) && !roles.contains(&ColumnRole::TargetPosition) {
            return Err(DatasetError::MissingExecutableColumn);
        }

        Ok(Self { columns })
    }
}

/// Reads a CSV dataset and returns normalized executable signals.
///
/// Feature columns are classified but ignored by this function; they stay research context.
///
/// # Errors
///
/// Returns an error for malformed CSV, invalid schema, invalid values, or duplicate executable
/// signals for the same `(timestamp, symbol)`.
pub fn read_signals_csv(reader: impl Read) -> Result<Vec<UserSignal>, DatasetError> {
    let mut reader = csv::Reader::from_reader(reader);
    let headers = reader
        .headers()
        .map_err(|error| DatasetError::Csv(error.to_string()))?
        .clone();
    let schema = DatasetSchema::infer(headers.iter())?;
    let indexes = ColumnIndexes::new(&schema)?;
    let mut seen = HashSet::new();
    let mut signals = Vec::new();

    for (row_index, record) in reader.records().enumerate() {
        let record = record.map_err(|error| DatasetError::Csv(error.to_string()))?;
        let source_row_id = row_index as u64 + 1;
        let row = SignalRow {
            timestamp: parse_required_u64(&record, indexes.timestamp, "timestamp", source_row_id)?,
            available_at: parse_optional_u64(
                &record,
                indexes.available_at,
                "available_at",
                source_row_id,
            )?,
            symbol: parse_required_string(&record, indexes.symbol, "symbol", source_row_id)?,
            signal: parse_optional_f64(&record, indexes.signal, "signal", source_row_id)?,
            confidence: parse_optional_f64(
                &record,
                indexes.confidence,
                "confidence",
                source_row_id,
            )?,
            target_position: parse_optional_f64(
                &record,
                indexes.target_position,
                "target_position",
                source_row_id,
            )?,
            valid_until: parse_optional_u64(
                &record,
                indexes.valid_until,
                "valid_until",
                source_row_id,
            )?,
            source_row_id: Some(source_row_id),
        };

        if !seen.insert((row.timestamp, row.symbol.clone())) {
            return Err(DatasetError::DuplicateSignal {
                timestamp: row.timestamp,
                symbol: row.symbol,
            });
        }

        signals.push(row.normalize()?);
    }

    Ok(signals)
}

/// Inspects a CSV dataset and returns schema, counts, and non-fatal warnings.
///
/// # Errors
///
/// Returns an error for malformed CSV, invalid schema, invalid executable values, or rows without
/// executable intent.
pub fn inspect_signals_csv(reader: impl Read) -> Result<DatasetReport, DatasetError> {
    let mut reader = csv::Reader::from_reader(reader);
    let headers = reader
        .headers()
        .map_err(|error| DatasetError::Csv(error.to_string()))?
        .clone();
    let schema = DatasetSchema::infer(headers.iter())?;
    let indexes = ColumnIndexes::new(&schema)?;
    let mut warnings = optional_column_warnings(&indexes);
    let mut feature_stats = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.role == ColumnRole::Feature)
        .map(|(index, column)| FeatureStats {
            index,
            column: column.name.clone(),
            present: 0,
        })
        .collect::<Vec<_>>();
    let mut row_count = 0;
    let mut signal_count = 0;

    for (row_index, record) in reader.records().enumerate() {
        let record = record.map_err(|error| DatasetError::Csv(error.to_string()))?;
        let source_row_id = row_index as u64 + 1;
        row_count += 1;

        parse_required_u64(&record, indexes.timestamp, "timestamp", source_row_id)?;
        parse_required_string(&record, indexes.symbol, "symbol", source_row_id)?;
        let signal = parse_optional_f64(&record, indexes.signal, "signal", source_row_id)?;
        parse_optional_f64(&record, indexes.confidence, "confidence", source_row_id)?;
        let target_position = parse_optional_f64(
            &record,
            indexes.target_position,
            "target_position",
            source_row_id,
        )?;
        parse_optional_u64(&record, indexes.available_at, "available_at", source_row_id)?;
        parse_optional_u64(&record, indexes.valid_until, "valid_until", source_row_id)?;

        if signal.is_none() && target_position.is_none() {
            return Err(SignalError::MissingExecutableIntent.into());
        }
        signal_count += 1;

        for feature in &mut feature_stats {
            if record
                .get(feature.index)
                .map(str::trim)
                .is_some_and(|value| !value.is_empty())
            {
                feature.present += 1;
            }
        }
    }

    warnings.extend(feature_stats.iter().filter_map(|feature| {
        if feature.present == 0 {
            Some(DatasetWarning::EmptyFeatureColumn {
                column: feature.column.clone(),
            })
        } else if feature.present < row_count {
            Some(DatasetWarning::SparseFeatureColumn {
                column: feature.column.clone(),
                present: feature.present,
                total: row_count,
            })
        } else {
            None
        }
    }));

    Ok(DatasetReport {
        feature_count: feature_stats.len(),
        row_count,
        schema,
        signal_count,
        warnings,
    })
}

#[derive(Clone, Debug)]
struct FeatureStats {
    index: usize,
    column: String,
    present: usize,
}

fn optional_column_warnings(indexes: &ColumnIndexes) -> Vec<DatasetWarning> {
    [
        (indexes.available_at, "available_at"),
        (indexes.confidence, "confidence"),
        (indexes.valid_until, "valid_until"),
    ]
    .into_iter()
    .filter_map(|(index, column)| {
        index
            .is_none()
            .then_some(DatasetWarning::MissingOptionalColumn(column))
    })
    .collect()
}

#[derive(Clone, Debug)]
struct ColumnIndexes {
    timestamp: usize,
    available_at: Option<usize>,
    symbol: usize,
    signal: Option<usize>,
    confidence: Option<usize>,
    target_position: Option<usize>,
    valid_until: Option<usize>,
}

impl ColumnIndexes {
    fn new(schema: &DatasetSchema) -> Result<Self, DatasetError> {
        let mut indexes = HashMap::new();
        for (index, column) in schema.columns.iter().enumerate() {
            if column.role == ColumnRole::Feature {
                continue;
            }
            if indexes.insert(column.role, index).is_some() {
                return Err(DatasetError::DuplicateColumnRole(column.role));
            }
        }

        Ok(Self {
            timestamp: required_index(&indexes, ColumnRole::Timestamp, "timestamp")?,
            available_at: indexes.get(&ColumnRole::AvailableAt).copied(),
            symbol: required_index(&indexes, ColumnRole::Symbol, "symbol")?,
            signal: indexes.get(&ColumnRole::Signal).copied(),
            confidence: indexes.get(&ColumnRole::Confidence).copied(),
            target_position: indexes.get(&ColumnRole::TargetPosition).copied(),
            valid_until: indexes.get(&ColumnRole::ValidUntil).copied(),
        })
    }
}

fn required_index(
    indexes: &HashMap<ColumnRole, usize>,
    role: ColumnRole,
    column: &'static str,
) -> Result<usize, DatasetError> {
    indexes
        .get(&role)
        .copied()
        .ok_or(DatasetError::MissingColumn(column))
}

fn classify_column(name: &str) -> ColumnRole {
    match normalized_name(name).as_str() {
        "timestamp" | "ts_event" => ColumnRole::Timestamp,
        "available_at" | "ts_init" => ColumnRole::AvailableAt,
        "symbol" | "instrument" | "instrument_id" => ColumnRole::Symbol,
        "signal" | "prediction" | "alpha" => ColumnRole::Signal,
        "confidence" => ColumnRole::Confidence,
        "target_position" | "target_weight" | "target_qty" => ColumnRole::TargetPosition,
        "valid_until" => ColumnRole::ValidUntil,
        _ => ColumnRole::Feature,
    }
}

fn normalized_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(' ', "_")
}

fn parse_required_string(
    record: &csv::StringRecord,
    index: usize,
    field: &'static str,
    row: u64,
) -> Result<String, DatasetError> {
    let value = record
        .get(index)
        .ok_or(DatasetError::MissingValue { row, field })?
        .trim();
    if value.is_empty() {
        return Err(DatasetError::MissingValue { row, field });
    }
    Ok(value.to_string())
}

fn parse_required_u64(
    record: &csv::StringRecord,
    index: usize,
    field: &'static str,
    row: u64,
) -> Result<u64, DatasetError> {
    let value = parse_required_string(record, index, field, row)?;
    value
        .parse::<u64>()
        .map_err(|_| DatasetError::InvalidU64 { row, field, value })
}

fn parse_optional_u64(
    record: &csv::StringRecord,
    index: Option<usize>,
    field: &'static str,
    row: u64,
) -> Result<Option<u64>, DatasetError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let Some(value) = record
        .get(index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| DatasetError::InvalidU64 {
            row,
            field,
            value: value.to_string(),
        })
}

fn parse_optional_f64(
    record: &csv::StringRecord,
    index: Option<usize>,
    field: &'static str,
    row: u64,
) -> Result<Option<f64>, DatasetError> {
    let Some(index) = index else {
        return Ok(None);
    };
    let Some(value) = record
        .get(index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<f64>()
        .map(Some)
        .map_err(|_| DatasetError::InvalidF64 {
            row,
            field,
            value: value.to_string(),
        })
}

#[derive(Clone, Debug, PartialEq)]
pub struct SignalRow {
    pub timestamp: u64,
    pub available_at: Option<u64>,
    pub symbol: String,
    pub signal: Option<f64>,
    pub confidence: Option<f64>,
    pub target_position: Option<f64>,
    pub valid_until: Option<u64>,
    pub source_row_id: Option<u64>,
}

impl SignalRow {
    /// Converts an already-parsed dataset row into the executable signal contract.
    ///
    /// # Errors
    ///
    /// Returns an error when signal validation fails.
    pub fn normalize(self) -> Result<UserSignal, DatasetError> {
        Ok(UserSignal::try_from(RawSignal {
            timestamp: UnixNanos::new(self.timestamp),
            available_at: self.available_at.map(UnixNanos::new),
            instrument_id: InstrumentId::new(self.symbol)?,
            signal: self.signal,
            confidence: self.confidence,
            target_position: self.target_position,
            valid_until: self.valid_until.map(UnixNanos::new),
            source_row_id: self.source_row_id,
        })?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatasetError {
    Csv(String),
    DuplicateColumnRole(ColumnRole),
    DuplicateSignal {
        timestamp: u64,
        symbol: String,
    },
    InvalidF64 {
        row: u64,
        field: &'static str,
        value: String,
    },
    InvalidU64 {
        row: u64,
        field: &'static str,
        value: String,
    },
    MissingColumn(&'static str),
    MissingExecutableColumn,
    MissingValue {
        row: u64,
        field: &'static str,
    },
    Parquet(String),
    Signal(SignalError),
    UnsupportedParquetColumn {
        column: String,
        expected: &'static str,
    },
}

impl fmt::Display for DatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Csv(error) => write!(f, "csv error: {error}"),
            Self::DuplicateColumnRole(role) => {
                write!(f, "duplicate executable column role `{role:?}`")
            }
            Self::DuplicateSignal { timestamp, symbol } => {
                write!(
                    f,
                    "duplicate signal for timestamp {timestamp} and symbol `{symbol}`"
                )
            }
            Self::InvalidF64 { row, field, value } => {
                write!(f, "row {row} field `{field}` is not a valid f64: `{value}`")
            }
            Self::InvalidU64 { row, field, value } => {
                write!(f, "row {row} field `{field}` is not a valid u64: `{value}`")
            }
            Self::MissingColumn(column) => write!(f, "missing required column `{column}`"),
            Self::MissingExecutableColumn => {
                f.write_str("missing required executable column `signal` or `target_position`")
            }
            Self::MissingValue { row, field } => {
                write!(f, "row {row} field `{field}` is required")
            }
            Self::Parquet(error) => write!(f, "parquet error: {error}"),
            Self::Signal(error) => write!(f, "{error}"),
            Self::UnsupportedParquetColumn { column, expected } => {
                write!(f, "parquet column `{column}` must be {expected}")
            }
        }
    }
}

impl Error for DatasetError {}

impl From<SignalError> for DatasetError {
    fn from(error: SignalError) -> Self {
        Self::Signal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_inference_separates_features_from_signal_contract() {
        let schema = DatasetSchema::infer([
            "timestamp",
            "symbol",
            "feature_1",
            "confidence",
            "target_position",
        ])
        .expect("schema is valid");

        assert_eq!(schema.columns[2].role, ColumnRole::Feature);
        assert!(
            schema
                .columns
                .iter()
                .any(|column| column.role == ColumnRole::TargetPosition)
        );
    }

    #[test]
    fn row_normalization_uses_available_at_as_engine_visible_time() {
        let signal = SignalRow {
            timestamp: 10,
            available_at: Some(12),
            symbol: "AAPL.XNAS".to_string(),
            signal: Some(1.0),
            confidence: Some(0.5),
            target_position: None,
            valid_until: Some(20),
            source_row_id: Some(3),
        }
        .normalize()
        .expect("valid row");

        assert_eq!(signal.ts_event.as_u64(), 10);
        assert_eq!(signal.ts_init.as_u64(), 12);
        assert_eq!(signal.source_row_id, Some(3));
    }

    #[test]
    fn csv_ingestion_normalizes_only_executable_contract_columns() {
        let csv = "\
timestamp,symbol,feature_1,signal,confidence,available_at
10,AAPL.XNAS,999,1,0.7,12
20,MSFT.XNAS,888,-1,0.4,
";

        let signals = read_signals_csv(csv.as_bytes()).expect("csv is valid");

        assert_eq!(signals.len(), 2);
        assert_eq!(signals[0].ts_event.as_u64(), 10);
        assert_eq!(signals[0].ts_init.as_u64(), 12);
        assert_eq!(signals[0].signal, Some(1.0));
        assert_eq!(signals[0].source_row_id, Some(1));
        assert_eq!(signals[1].ts_init.as_u64(), 20);
    }

    #[test]
    fn csv_ingestion_rejects_duplicate_executable_signals() {
        let csv = "\
timestamp,symbol,signal
10,AAPL.XNAS,1
10,AAPL.XNAS,-1
";

        let error = read_signals_csv(csv.as_bytes()).expect_err("duplicate must fail");

        assert!(matches!(error, DatasetError::DuplicateSignal { .. }));
    }

    #[test]
    fn csv_inspection_reports_counts_for_signals_and_features() {
        let csv = "\
timestamp,symbol,signal,confidence,feature_full,feature_sparse,feature_empty
10,AAPL.XNAS,1,0.7,11,21,
20,MSFT.XNAS,-1,0.4,12,,
";

        let report = inspect_signals_csv(csv.as_bytes()).expect("csv is valid");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.signal_count, 2);
        assert_eq!(report.feature_count, 3);
        assert!(
            report
                .warnings
                .contains(&DatasetWarning::SparseFeatureColumn {
                    column: "feature_sparse".to_string(),
                    present: 1,
                    total: 2,
                })
        );
        assert!(
            report
                .warnings
                .contains(&DatasetWarning::EmptyFeatureColumn {
                    column: "feature_empty".to_string(),
                })
        );
    }

    #[test]
    fn csv_inspection_warns_when_optional_columns_are_missing() {
        let csv = "\
timestamp,symbol,signal
10,AAPL.XNAS,1
";

        let report = inspect_signals_csv(csv.as_bytes()).expect("csv is valid");

        assert!(
            report
                .warnings
                .contains(&DatasetWarning::MissingOptionalColumn("confidence"))
        );
        assert!(
            report
                .warnings
                .contains(&DatasetWarning::MissingOptionalColumn("available_at"))
        );
        assert!(
            report
                .warnings
                .contains(&DatasetWarning::MissingOptionalColumn("valid_until"))
        );
    }

    #[test]
    fn csv_inspection_fails_without_required_execution_fields() {
        let missing_timestamp = "symbol,signal\nAAPL.XNAS,1\n";
        let missing_symbol = "timestamp,signal\n10,1\n";
        let missing_intent_column = "timestamp,symbol,confidence\n10,AAPL.XNAS,0.7\n";
        let missing_intent_value = "timestamp,symbol,signal\n10,AAPL.XNAS,\n";

        assert!(matches!(
            inspect_signals_csv(missing_timestamp.as_bytes()),
            Err(DatasetError::MissingColumn("timestamp"))
        ));
        assert!(matches!(
            inspect_signals_csv(missing_symbol.as_bytes()),
            Err(DatasetError::MissingColumn("symbol"))
        ));
        assert!(matches!(
            inspect_signals_csv(missing_intent_column.as_bytes()),
            Err(DatasetError::MissingExecutableColumn)
        ));
        assert!(matches!(
            inspect_signals_csv(missing_intent_value.as_bytes()),
            Err(DatasetError::Signal(SignalError::MissingExecutableIntent))
        ));
    }
}
