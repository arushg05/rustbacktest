# Development

This project starts contract-first:

```text
dataset rows -> normalized UserSignal -> engine adapter
```

Keep user feature columns out of execution code. Features are research context; `UserSignal` is the
only executable dataset contract.

## Development Slices

Each slice should leave the workspace shippable: formatted, clippy-clean, tested, and with one
small integration check when behavior crosses crates.

### Slice 0 - Foundation, Done

Objective: create the new Rust workspace and executable signal contract.

Delivered:

- `crates/signal`: `UserSignal`, `RawSignal`, `UnixNanos`, `InstrumentId`.
- `crates/dataset`: schema role inference and parsed row normalization.
- CSV ingestion through `read_signals_csv`.

Acceptance:

- Feature columns are classified as `Feature`, not execution inputs.
- Rows normalize into `UserSignal`.
- `available_at < timestamp` is rejected.
- Duplicate `(timestamp, symbol)` CSV signals are rejected.

Tests:

- signal validation tests,
- dataset schema inference test,
- CSV normalization test,
- duplicate CSV signal test.

### Slice 1 - Dataset Diagnostics, Done

Objective: return an inspection report before normalization so the UI/API can explain an upload.

Files:

- `crates/dataset/src/lib.rs`
- optional split once it grows: `crates/dataset/src/diagnostics.rs`

Interfaces:

```rust
pub struct DatasetReport {
    pub schema: DatasetSchema,
    pub row_count: u64,
    pub signal_count: u64,
    pub feature_count: usize,
    pub warnings: Vec<DatasetWarning>,
}

pub enum DatasetWarning {
    MissingOptionalColumn { column: &'static str },
    EmptyFeatureColumn { column: String },
    SparseFeatureColumn { column: String, missing: u64 },
}
```

Acceptance:

- CSV can be inspected without producing signals.
- Report counts feature columns separately from executable signal fields.
- Missing optional columns are warnings, not errors.
- Missing required executable columns still fail.

Tests:

- report counts rows/features/signals,
- missing optional fields produce warnings,
- missing `timestamp`, `symbol`, or signal intent fails.

### Slice 2 - Signal Ordering And Quality Gates, Done

Objective: enforce deterministic replay preconditions before any engine adapter sees data.

Delivered:

- `validate_signal_stream`.
- Row-aware out-of-order stream errors.
- Row-aware duplicate `(ts_event, instrument_id)` errors.

Files:

- `crates/signal/src/lib.rs`
- `crates/dataset/src/lib.rs`

Interfaces:

```rust
pub fn validate_signal_stream(signals: &[UserSignal]) -> Result<(), SignalError>;
```

Rules:

- stream must be sorted by `ts_init`, then instrument id,
- `valid_until < ts_init` remains invalid,
- non-finite executable values remain invalid,
- duplicate `(ts_event, instrument_id)` remains invalid.

Acceptance:

- unordered signal vectors are rejected or sorted by an explicit function, not silently accepted,
- validation error tells the offending row/source id when available.

Tests:

- sorted stream passes,
- out-of-order stream fails,
- duplicate stream fails with source row id.

### Slice 3 - Parquet Signal Ingestion, Done

Objective: add columnar ingestion for production-sized datasets.

Delivered:

- `read_signals_parquet`.
- `inspect_parquet`.
- Integer nanosecond timestamp support through Arrow `UInt64`/`Int64`.
- Float64 executable values and Utf8 symbols.
- Feature columns counted for diagnostics but ignored for execution.

Files:

- `crates/dataset/Cargo.toml`
- `crates/dataset/src/parquet.rs`
- `crates/dataset/src/lib.rs`

Dependencies:

- Use Arrow/Parquet crates rather than custom parsing.

Interfaces:

```rust
pub fn read_signals_parquet(path: impl AsRef<Path>) -> Result<Vec<UserSignal>, DatasetError>;
pub fn inspect_parquet(path: impl AsRef<Path>) -> Result<DatasetReport, DatasetError>;
```

Acceptance:

- Reads the same signal contract as CSV.
- Feature columns are counted but not converted into strategy code.
- Timestamp columns support integer nanoseconds first.
- String timestamp parsing is explicitly deferred unless needed.

Tests:

- tiny fixture Parquet file normalizes to signals,
- feature-only extra columns remain diagnostics,
- invalid executable values fail.

### Slice 4 - Market Data Core Model, Done

Objective: create engine-neutral OHLCV/instrument types and a local in-memory market catalog.

Delivered:

- `crates/market-data`.
- `Instrument`, `RawBar`, `Bar`, and `MarketCatalog`.
- Bar validation for availability and finite OHLCV values.

Files:

- new crate `crates/market-data`
- workspace `Cargo.toml`

Interfaces:

```rust
pub struct Instrument {
    pub id: String,
    pub symbol: String,
    pub venue: String,
}

pub struct Bar {
    pub instrument_id: String,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

pub trait MarketDataCatalog {
    fn instruments(&self) -> &[Instrument];
    fn bars(&self, instrument_id: &str, start: UnixNanos, end: UnixNanos) -> Vec<Bar>;
}
```

Acceptance:

- No Nautilus dependency yet.
- Bars can be queried by instrument and time range.
- `ts_init`/availability semantics match signal semantics.

Tests:

- range query includes/excludes correct boundaries,
- unknown instrument returns empty result,
- bar with `ts_init < ts_event` is rejected.

### Slice 5 - Point-In-Time Alignment, Done

Objective: join signals and market data into a deterministic replay plan.

Delivered:

- `crates/alignment`.
- `ReplayEvent`.
- `build_replay_events`.
- Bar-before-signal tie ordering at the same `ts_init`.
- Market coverage and future-data validation.

Files:

- new crate `crates/alignment`
- `crates/market-data`
- `crates/signal`

Interfaces:

```rust
pub enum ReplayEvent {
    Bar(Bar),
    Signal(UserSignal),
}

pub fn build_replay_events(
    signals: Vec<UserSignal>,
    bars: Vec<Bar>,
) -> Result<Vec<ReplayEvent>, AlignmentError>;
```

Acceptance:

- Events sorted by `ts_init`.
- Same-time tie ordering is explicit.
- Signals with no market data coverage are diagnostics.
- No future market data is visible at decision time.

Tests:

- signal before next bar orders correctly,
- same timestamp tie follows documented priority,
- missing market coverage emits diagnostic.

### Slice 6 - Minimal Engine Adapter, Done

Objective: create a dependency-free simulated engine that proves signal-to-order flow before
Nautilus integration.

Delivered:

- `crates/engine-adapter`.
- `run_backtest`.
- Orders, fills, and final equity.
- Next-bar-close fill rule.

Files:

- new crate `crates/engine-adapter`
- `crates/alignment`

Interfaces:

```rust
pub struct BacktestConfig {
    pub initial_cash: f64,
    pub long_threshold: f64,
    pub short_threshold: f64,
}

pub struct BacktestResult {
    pub orders: Vec<Order>,
    pub fills: Vec<Fill>,
    pub final_equity: f64,
}

pub fn run_backtest(events: &[ReplayEvent], config: &BacktestConfig) -> Result<BacktestResult, EngineError>;
```

Acceptance:

- Market orders fill on the next available bar close or open, choose one and document it.
- Long/flat/short threshold behavior is deterministic.
- No custom user strategy code.

Tests:

- one long signal creates one buy order/fill,
- repeated same target does not churn orders,
- missing price prevents impossible fill.

### Slice 7 - Result Digest And Reproducibility, Done

Objective: make identical runs compare by stable result digest.

Delivered:

- `crates/analytics`.
- Stable deterministic result digest over orders, fills, and final equity.

Files:

- new crate `crates/analytics` or `crates/result`
- `crates/engine-adapter`

Interfaces:

```rust
pub fn result_digest(result: &BacktestResult) -> String;
```

Acceptance:

- Same inputs/config produce same digest.
- Different fill price/order sequence changes digest.
- Digest excludes wall-clock timing.

Tests:

- identical run digest equality,
- changed signal changes digest,
- changed config changes digest.

### Slice 8 - Nautilus Spike, Done

Objective: prove that the platform contract can feed Nautilus without moving product code into the
Nautilus checkout.

Delivered:

- `nautilus` feature flag in `crates/engine-adapter`.
- `NautilusSignalData` adapter payload converted from `UserSignal`.
- Feature-enabled smoke test.

Files:

- `crates/engine-adapter`
- optional feature flag `nautilus`

Rules:

- Keep the existing minimal engine as the fast deterministic test double.
- Nautilus adapter is behind a feature flag.
- No broad Nautilus fork.

Acceptance:

- One `UserSignal` converts into Nautilus-compatible custom data or equivalent adapter input.
- One tiny replay run completes.
- Existing dependency-free tests still pass without the feature.

Tests:

- feature-disabled workspace tests,
- feature-enabled Nautilus smoke test if dependency setup is available.

### Slice 9 - Worker And CLI, Done

Objective: run a local backtest from files without an HTTP API.

Delivered:

- `apps/backtest-worker`.
- CSV signal and market-bar inputs.
- Alignment plus engine run.
- JSON result output.

Files:

- `apps/backtest-worker`
- workspace `Cargo.toml`

Command:

```powershell
cargo run -p backtest-worker -- --signals path/to/signals.csv --market path/to/bars.csv
```

Acceptance:

- Reads CSV signals and local market bars.
- Writes result JSON.
- Returns non-zero exit code on validation errors.

Tests:

- CLI happy-path fixture,
- CLI duplicate signal failure,
- CLI missing market data failure.

### Slice 10 - API Skeleton, Done

Objective: expose upload/run/status shape without building UI or auth yet.

Delivered:

- `crates/api`.
- `apps/api-server`.
- `POST /datasets/inspect`.
- `POST /backtests` with in-memory placeholder completed job.
- `GET /backtests/{id}`.

Files:

- new crate `crates/api`
- `apps/api-server`

Endpoints:

```text
POST /datasets/inspect
POST /backtests
GET  /backtests/{id}
```

Acceptance:

- In-memory job store only.
- Uses the same dataset and engine crates as CLI.
- No database until local flow is useful.

Tests:

- inspect endpoint returns report,
- create backtest returns id,
- result endpoint returns completed result.

## Current Next Slice

Wire `POST /backtests` to the same real signal + market-data execution path used by
`backtest-worker`, then add an end-to-end fixture that exercises CSV upload through result JSON.

## Checks

Run from `rust-backtest-platform/`:

```powershell
cargo fmt --check
cargo clippy --workspace --all-targets
cargo test
```

Use third-party dependencies only when they replace code we should not own, such as CSV or Parquet
parsing.
