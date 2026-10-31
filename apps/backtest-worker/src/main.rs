use std::{
    env,
    error::Error,
    fmt,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use backtest_alignment::build_replay_events;
use backtest_dataset::{DatasetError, read_signals_csv};
use backtest_engine_adapter::{BacktestConfig, BacktestResult, EngineError, run_backtest};
use backtest_market_data::{Bar, InstrumentId, MarketDataError, RawBar, UnixNanos};
use serde_json::{Value, json};

fn main() -> Result<(), WorkerError> {
    let args = Args::parse(env::args().skip(1))?;
    let signal_file = File::open(&args.signals)?;
    let market_file = File::open(&args.market)?;
    let result = run_from_readers(signal_file, market_file)?;
    write_json_result(&args.output, &result)?;
    Ok(())
}

fn run_from_readers(
    signal_reader: impl Read,
    market_reader: impl Read,
) -> Result<BacktestResult, WorkerError> {
    let signals = read_signals_csv(signal_reader)?;
    let bars = read_market_csv(market_reader)?;
    let events = build_replay_events(signals, bars)?;
    Ok(run_backtest(&events, &default_config())?)
}

fn default_config() -> BacktestConfig {
    BacktestConfig {
        initial_cash: 100_000.0,
        long_threshold: 0.5,
        short_threshold: -0.5,
    }
}

fn read_market_csv(reader: impl Read) -> Result<Vec<Bar>, WorkerError> {
    let mut reader = csv::Reader::from_reader(reader);
    let headers = reader.headers()?.clone();
    let columns = MarketColumns::new(&headers)?;
    let mut bars = Vec::new();

    for (row_index, record) in reader.records().enumerate() {
        let record = record?;
        let row = row_index as u64 + 1;
        let symbol = required_string(&record, columns.symbol, "symbol", row)?;
        bars.push(Bar::try_from(RawBar {
            instrument_id: InstrumentId::new(symbol.clone()).map_err(|error| {
                WorkerError::InvalidMarketSymbol {
                    row,
                    value: symbol,
                    error: error.to_string(),
                }
            })?,
            ts_event: UnixNanos::new(required_u64(&record, columns.timestamp, "timestamp", row)?),
            ts_init: UnixNanos::new(required_u64(
                &record,
                columns.available_at,
                "available_at",
                row,
            )?),
            open: required_f64(&record, columns.open, "open", row)?,
            high: required_f64(&record, columns.high, "high", row)?,
            low: required_f64(&record, columns.low, "low", row)?,
            close: required_f64(&record, columns.close, "close", row)?,
            volume: required_f64(&record, columns.volume, "volume", row)?,
        })?);
    }

    Ok(bars)
}

#[derive(Clone, Copy, Debug)]
struct MarketColumns {
    timestamp: usize,
    available_at: usize,
    symbol: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
}

impl MarketColumns {
    fn new(headers: &csv::StringRecord) -> Result<Self, WorkerError> {
        Ok(Self {
            timestamp: required_header(headers, "timestamp")?,
            available_at: required_header(headers, "available_at")?,
            symbol: required_header(headers, "symbol")?,
            open: required_header(headers, "open")?,
            high: required_header(headers, "high")?,
            low: required_header(headers, "low")?,
            close: required_header(headers, "close")?,
            volume: required_header(headers, "volume")?,
        })
    }
}

fn required_header(headers: &csv::StringRecord, name: &'static str) -> Result<usize, WorkerError> {
    headers
        .iter()
        .position(|header| header.trim() == name)
        .ok_or(WorkerError::MissingMarketColumn(name))
}

fn required_string(
    record: &csv::StringRecord,
    index: usize,
    field: &'static str,
    row: u64,
) -> Result<String, WorkerError> {
    let value = record
        .get(index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(WorkerError::MissingMarketValue { row, field })?;
    Ok(value.to_string())
}

fn required_u64(
    record: &csv::StringRecord,
    index: usize,
    field: &'static str,
    row: u64,
) -> Result<u64, WorkerError> {
    let value = required_string(record, index, field, row)?;
    value
        .parse()
        .map_err(|_| WorkerError::InvalidMarketU64 { row, field, value })
}

fn required_f64(
    record: &csv::StringRecord,
    index: usize,
    field: &'static str,
    row: u64,
) -> Result<f64, WorkerError> {
    let value = required_string(record, index, field, row)?;
    value
        .parse()
        .map_err(|_| WorkerError::InvalidMarketF64 { row, field, value })
}

fn write_json_result(path: &Path, result: &BacktestResult) -> Result<(), WorkerError> {
    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, &result_json(result))?;
    file.write_all(b"\n")?;
    Ok(())
}

fn result_json(result: &BacktestResult) -> Value {
    json!({
        "final_equity": result.final_equity,
        "orders": result.orders.iter().map(|order| {
            json!({
                "id": order.id,
                "instrument_id": order.instrument_id.as_str(),
                "ts_init": order.ts_init.as_u64(),
                "side": format!("{:?}", order.side),
                "quantity": order.quantity,
                "target_position": order.target_position,
            })
        }).collect::<Vec<_>>(),
        "fills": result.fills.iter().map(|fill| {
            json!({
                "order_id": fill.order_id,
                "instrument_id": fill.instrument_id.as_str(),
                "ts_event": fill.ts_event.as_u64(),
                "ts_init": fill.ts_init.as_u64(),
                "side": format!("{:?}", fill.side),
                "quantity": fill.quantity,
                "price": fill.price,
                "position_after": fill.position_after,
                "cash_after": fill.cash_after,
            })
        }).collect::<Vec<_>>(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Args {
    signals: PathBuf,
    market: PathBuf,
    output: PathBuf,
}

impl Args {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, WorkerError> {
        let mut signals = None;
        let mut market = None;
        let mut output = None;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            let slot = match arg.as_str() {
                "--signals" => &mut signals,
                "--market" => &mut market,
                "--output" => &mut output,
                _ => return Err(WorkerError::Cli(format!("unknown argument `{arg}`"))),
            };
            let value = args
                .next()
                .ok_or_else(|| WorkerError::Cli(format!("missing value for `{arg}`")))?;
            *slot = Some(PathBuf::from(value));
        }

        Ok(Self {
            signals: signals.ok_or(WorkerError::Cli("missing --signals".to_string()))?,
            market: market.ok_or(WorkerError::Cli("missing --market".to_string()))?,
            output: output.ok_or(WorkerError::Cli("missing --output".to_string()))?,
        })
    }
}

#[derive(Debug)]
enum WorkerError {
    Alignment(backtest_alignment::AlignmentError),
    Cli(String),
    Csv(csv::Error),
    Dataset(DatasetError),
    Engine(EngineError),
    Io(io::Error),
    Json(serde_json::Error),
    Market(MarketDataError),
    MissingMarketColumn(&'static str),
    MissingMarketValue {
        row: u64,
        field: &'static str,
    },
    InvalidMarketF64 {
        row: u64,
        field: &'static str,
        value: String,
    },
    InvalidMarketU64 {
        row: u64,
        field: &'static str,
        value: String,
    },
    InvalidMarketSymbol {
        row: u64,
        value: String,
        error: String,
    },
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Alignment(error) => write!(f, "{error}"),
            Self::Cli(error) => write!(f, "{error}"),
            Self::Csv(error) => write!(f, "market csv error: {error}"),
            Self::Dataset(error) => write!(f, "{error}"),
            Self::Engine(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "json error: {error}"),
            Self::Market(error) => write!(f, "{error}"),
            Self::MissingMarketColumn(column) => {
                write!(f, "missing required market column `{column}`")
            }
            Self::MissingMarketValue { row, field } => {
                write!(f, "market row {row} field `{field}` is required")
            }
            Self::InvalidMarketF64 { row, field, value } => {
                write!(
                    f,
                    "market row {row} field `{field}` is not a valid f64: `{value}`"
                )
            }
            Self::InvalidMarketU64 { row, field, value } => {
                write!(
                    f,
                    "market row {row} field `{field}` is not a valid u64: `{value}`"
                )
            }
            Self::InvalidMarketSymbol { row, value, error } => {
                write!(f, "market row {row} symbol `{value}` is invalid: {error}")
            }
        }
    }
}

impl Error for WorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Alignment(error) => Some(error),
            Self::Csv(error) => Some(error),
            Self::Dataset(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Market(error) => Some(error),
            Self::Cli(_)
            | Self::MissingMarketColumn(_)
            | Self::MissingMarketValue { .. }
            | Self::InvalidMarketF64 { .. }
            | Self::InvalidMarketU64 { .. }
            | Self::InvalidMarketSymbol { .. } => None,
        }
    }
}

impl From<backtest_alignment::AlignmentError> for WorkerError {
    fn from(error: backtest_alignment::AlignmentError) -> Self {
        Self::Alignment(error)
    }
}

impl From<csv::Error> for WorkerError {
    fn from(error: csv::Error) -> Self {
        Self::Csv(error)
    }
}

impl From<DatasetError> for WorkerError {
    fn from(error: DatasetError) -> Self {
        Self::Dataset(error)
    }
}

impl From<EngineError> for WorkerError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<io::Error> for WorkerError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for WorkerError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<MarketDataError> for WorkerError {
    fn from(error: MarketDataError) -> Self {
        Self::Market(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_market_csv_happy_path() {
        let csv = "\
timestamp,available_at,symbol,open,high,low,close,volume
10,10,AAPL.XNAS,100,101,99,100.5,1000
";

        let bars = read_market_csv(csv.as_bytes()).expect("market csv is valid");

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].instrument_id.as_str(), "AAPL.XNAS");
        assert_eq!(bars[0].ts_event.as_u64(), 10);
        assert_eq!(bars[0].close, 100.5);
    }

    #[test]
    fn run_fails_when_signal_has_no_market_data() {
        let signals = "\
timestamp,symbol,signal
10,AAPL.XNAS,1
";
        let market = "\
timestamp,available_at,symbol,open,high,low,close,volume
";

        let error =
            run_from_readers(signals.as_bytes(), market.as_bytes()).expect_err("coverage fails");

        assert!(matches!(error, WorkerError::Alignment(_)));
    }
}
