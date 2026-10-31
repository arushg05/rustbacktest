import importlib.util
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

SPEC = importlib.util.spec_from_file_location("market_update", Path(__file__).with_name("update.py"))
market_update = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(market_update)


class UpdateTests(unittest.TestCase):
    def test_daily_upsert_and_universe_snapshot(self):
        dates = pd.DatetimeIndex(["2026-10-28", "2026-10-29"])
        frame = pd.DataFrame({
            ("AAPL", "Open"): [100.0, 101.0],
            ("AAPL", "High"): [102.0, 103.0],
            ("AAPL", "Low"): [99.0, 100.0],
            ("AAPL", "Close"): [101.0, 102.0],
            ("AAPL", "Adj Close"): [101.0, 102.0],
            ("AAPL", "Volume"): [1000, 1100],
            ("^GSPC", "Open"): [5000.0, 5001.0],
            ("^GSPC", "High"): [5002.0, 5003.0],
            ("^GSPC", "Low"): [4999.0, 5000.0],
            ("^GSPC", "Close"): [5001.0, 5002.0],
            ("^GSPC", "Adj Close"): [5001.0, 5002.0],
            ("^GSPC", "Volume"): [0, 0],
        }, index=dates)
        now = datetime(2026, 10, 30, 3, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory, patch.object(market_update.yf, "download", return_value=frame) as download:
            path = Path(directory) / "market.sqlite"
            for _ in range(2):
                count, failures = market_update.update(path, symbols=[("AAPL", "Apple")], now=now, pause=0)
                self.assertEqual((count, failures), (4, []))
            self.assertEqual(download.call_count, 2)
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0], 4)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM universe_snapshots").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT close FROM daily_bars WHERE symbol='AAPL' AND session_date='2026-10-29'").fetchone()[0], 102.0)

    def test_current_session_is_excluded_before_evening(self):
        self.assertEqual(market_update.market_end(datetime(2026, 10, 30, 20, tzinfo=timezone.utc)), "2026-10-30")

    def test_full_history_requests_earliest_range_and_resumes(self):
        frame = pd.DataFrame({
            ("^GSPC", "Open"): [100.0], ("^GSPC", "High"): [101.0],
            ("^GSPC", "Low"): [99.0], ("^GSPC", "Close"): [100.0],
            ("^GSPC", "Adj Close"): [100.0], ("^GSPC", "Volume"): [0],
        }, index=pd.DatetimeIndex(["1950-01-03"]))
        with tempfile.TemporaryDirectory() as directory, patch.object(market_update.yf, "download", return_value=frame) as download:
            path = Path(directory) / "history.sqlite"
            market_update.update(path, symbols=[], full_history=True, pause=0)
            market_update.update(path, symbols=[], full_history=True, pause=0)
            self.assertEqual(download.call_count, 1)
            self.assertEqual(download.call_args.kwargs["start"], "1900-01-01")


if __name__ == "__main__":
    unittest.main()
