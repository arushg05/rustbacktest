# Rust Backtest Platform

New Rust workspace for a user-dataset-driven backtesting platform.

NautilusTrader is a reference/integration target, not this project's source tree.

First invariant:

```text
uploaded features stay research context
normalized signals become executable input
```

## Current Crates

- `signal`: executable signal contract and validation.
- `dataset`: schema classification and row normalization into `signal`.

## Local Checks

```powershell
cargo test
cargo clippy --workspace --all-targets
cargo fmt --check
```

## Daily S&P 500 Research Data

`apps/market-ingest/update.py` maintains `data/sp500.sqlite` separately from the Rust execution
crates. It stores daily raw OHLCV, adjusted close, source and fetch time for the current S&P 500
constituents and `^GSPC`. The constituent snapshot is recorded by fetch date; it is **not** a
historical point-in-time constituent universe. Backtests must not treat today's membership as
historical membership or a fetched bar as available at its session open.

Install Python dependencies with `py -3 -m pip install -r apps/market-ingest/requirements.txt`.
Run once with `py -3 apps/market-ingest/update.py`. On this Windows machine, the scheduled task
`RustBacktestSP500Daily` runs `apps/market-ingest/run-daily.ps1` at 06:00 IST every day, after
the US regular close. It logs to `data/market-ingest.log`. Repeated runs upsert overlapping days;
failed downloads return a nonzero exit status and retry on the next scheduled run.

The SQLite cache is not wired into the CLI yet. The CLI currently consumes market CSV. Data from
this cache will need an explicit availability-time mapping before replay.

For an offline full-history snapshot of the current constituents, run
`py -3 apps/market-ingest/update.py --full-history --db data/sp500_full_history.sqlite`.
Completed symbols are checkpointed so a failed run can resume. This file contains each ticker's
earliest **Yahoo-available** daily bar through the last completed session at download time; Yahoo
does not necessarily provide prices from the company's founding or first listing. For example,
some long-lived stocks start in 1962. It is a static snapshot and is not updated by the daily task.

