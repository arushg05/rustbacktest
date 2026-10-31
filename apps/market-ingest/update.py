"""Refresh a local research cache of current S&P 500 constituents and daily bars."""

import argparse
import io
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf

UNIVERSE_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
INDEX_SYMBOL = "^GSPC"
NEW_YORK = ZoneInfo("America/New_York")


def constituents() -> list[tuple[str, str]]:
    response = requests.get(
        UNIVERSE_URL,
        headers={"User-Agent": "RustBacktestResearch/0.1 (local personal research)"},
        timeout=30,
    )
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text), match="Symbol")
    table = next(table for table in tables if {"Symbol", "Security"} <= set(table.columns))
    stocks = [(str(row.Symbol).strip(), str(row.Security).strip()) for row in table.itertuples()]
    if not 400 <= len(stocks) <= 550 or len({symbol for symbol, _ in stocks}) != len(stocks):
        raise ValueError("unexpected S&P 500 constituent list; refusing to update database")
    return stocks


def init_db(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS universe_snapshots (
            snapshot_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            fetched_at_utc TEXT NOT NULL,
            PRIMARY KEY (snapshot_date, symbol)
        );
        CREATE TABLE IF NOT EXISTS daily_bars (
            symbol TEXT NOT NULL,
            session_date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            adj_close REAL,
            volume INTEGER NOT NULL,
            fetched_at_utc TEXT NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (symbol, session_date)
        );
        CREATE TABLE IF NOT EXISTS full_history_complete (
            symbol TEXT PRIMARY KEY,
            completed_at_utc TEXT NOT NULL
        );
    """)


def market_end(now: datetime) -> str:
    local = now.astimezone(NEW_YORK)
    # Wait two hours after the regular close before accepting today's daily bar.
    end_day = local.date() + timedelta(days=1) if local.hour >= 18 else local.date()
    return end_day.isoformat()


def update(db_path: Path, *, symbols: list[tuple[str, str]] | None = None,
           now: datetime | None = None, pause: float = 2.0,
           full_history: bool = False) -> tuple[int, list[str]]:
    now = now or datetime.now(timezone.utc)
    symbols = symbols if symbols is not None else constituents()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path, timeout=30)
    try:
        init_db(db)
        fetched_at = now.isoformat()
        snapshot_date = now.astimezone(NEW_YORK).date().isoformat()
        with db:
            db.executemany(
                "INSERT OR REPLACE INTO universe_snapshots VALUES (?,?,?,?,?)",
                [(snapshot_date, symbol, name, UNIVERSE_URL, fetched_at)
                 for symbol, name in symbols],
            )
        tickers = [symbol.replace(".", "-") for symbol, _ in symbols] + [INDEX_SYMBOL]
        if full_history:
            completed = {row[0] for row in db.execute("SELECT symbol FROM full_history_complete")}
            tickers = [symbol for symbol in tickers if symbol not in completed]
        end = market_end(now)
        rows_written = 0
        failures = []
        for offset in range(0, len(tickers), 25):
            batch = tickers[offset:offset + 25]
            latest = dict(db.execute(
                f"SELECT symbol, MAX(session_date) FROM daily_bars WHERE symbol IN ({','.join('?' for _ in batch)}) GROUP BY symbol",
                batch,
            ))
            start = "1900-01-01" if full_history else min(
                (datetime.fromisoformat(latest[symbol]).date() - timedelta(days=7)
                 if symbol in latest else now.astimezone(NEW_YORK).date() - timedelta(days=365))
                for symbol in batch
            ).isoformat()
            if start >= end:
                continue
            try:
                frame = yf.download(
                    batch, start=start, end=end, interval="1d", group_by="ticker",
                    auto_adjust=False, actions=False, progress=False, threads=False,
                    timeout=20, multi_level_index=True,
                )
            except Exception as exc:
                failures.extend(batch)
                print(f"batch {offset // 25 + 1}: {exc}", file=sys.stderr)
                continue
            records = []
            successful = []
            for symbol in batch:
                if frame.empty or symbol not in frame.columns.get_level_values(0):
                    failures.append(symbol)
                    continue
                prices = frame[symbol].dropna(subset=["Open", "High", "Low", "Close"])
                if prices.empty:
                    failures.append(symbol)
                    continue
                successful.append(symbol)
                for date, bar in prices.iterrows():
                    if date.date().isoformat() >= end:
                        continue
                    values = [float(bar[key]) for key in ("Open", "High", "Low", "Close")]
                    if not all(pd.notna(value) and value > 0 for value in values):
                        continue
                    volume = bar.get("Volume")
                    if pd.isna(volume) or int(volume) < 0:
                        continue
                    adj = bar.get("Adj Close")
                    records.append((symbol, date.date().isoformat(), *values,
                                    None if pd.isna(adj) else float(adj), int(volume),
                                    fetched_at, "yfinance"))
            with db:
                db.executemany("INSERT OR REPLACE INTO daily_bars VALUES (?,?,?,?,?,?,?,?,?,?)", records)
                if full_history:
                    db.executemany("INSERT OR REPLACE INTO full_history_complete VALUES (?,?)",
                                   [(symbol, fetched_at) for symbol in successful])
            rows_written += len(records)
            if pause and offset + 25 < len(tickers):
                time.sleep(pause)
        return rows_written, failures
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parents[2] / "data" / "sp500.sqlite")
    parser.add_argument("--full-history", action="store_true", help="Backfill each ticker from its earliest available daily bar; resumable")
    args = parser.parse_args()
    try:
        count, failures = update(args.db, full_history=args.full_history)
    except Exception as exc:
        print(f"update failed: {exc}", file=sys.stderr)
        return 1
    print(f"stored {count} daily bars in {args.db}; failed symbols: {len(failures)}")
    if failures:
        print(", ".join(failures), file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
