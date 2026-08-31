import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from gigamoney.config import Settings
from gigamoney.runner import (
    TradingRunner, command_is_stale, command_plan, holdings_snapshot, nullable_number,
)


class FakeMessage:
    def partition(self):
        return 0

    def offset(self):
        return 123


class FakeProducer:
    def __init__(self, *, error=None, remaining=0, report=True):
        self.error = error
        self.remaining = remaining
        self.report = report
        self.key = None
        self.value = None

    def produce(self, topic, *, key, value, on_delivery):
        self.key = key
        self.value = json.loads(value)
        if self.report:
            on_delivery(self.error, FakeMessage())

    def flush(self, timeout):
        return self.remaining


class RunnerTests(unittest.TestCase):
    def settings(self):
        root = Path(tempfile.mkdtemp())
        return Settings({
            "gigamoney": {"accountId": "GIGA-A", "accountNumId": 7},
        }, root / "config.json", root)

    def test_routing_and_guards(self):
        market = command_plan({
            "type": "MARKET_ORDER", "symbol": "bili", "side": "BUY", "qty_shares": 2,
        })
        self.assertEqual(market.arguments["symbol"], "BILI")
        self.assertEqual(market.arguments["quantity"], "2")
        fok = command_plan({
            "type": "LIMIT_ORDER", "time_in_force": "FOK", "symbol": "BILI",
            "side": "SELL", "qty_shares": 3, "limit_price": 17.4,
        })
        self.assertTrue(fok.arguments["kill"])
        with self.assertRaises(ValueError):
            command_plan({
                "type": "MARKET_ORDER", "symbol": "BILI", "side": "BUY",
                "notional_usd": 100,
            })

    def test_snapshot_contract(self):
        snapshot = holdings_snapshot({
            "queriedAt": "2026-07-10T12:00:00Z",
            "cash": {"balances": [{"currency": "USD", "balance": "$1,234.50"}]},
            "positions": [{
                "symbol": "bili", "quantity": "12.5", "cost": "$17.25",
                "marketPrice": "$18.50", "marketValue": "$231.25",
                "dailyPL": "($15.00)", "dailyPLPercent": "6.94%",
            }],
        }, self.settings(), True)
        self.assertEqual(set(snapshot), {
            "account_id", "account_num_id", "cash", "cash_by_currency", "positions", "ts",
            "trading_enabled",
        })
        self.assertEqual(snapshot["cash"], 1234.5)
        self.assertEqual(snapshot["positions"][0]["avg_price"], 17.25)
        self.assertEqual(snapshot["positions"][0]["dailyPL"], -15.0)

    def test_staleness_and_number_cleaning(self):
        now = datetime.now(timezone.utc)
        self.assertTrue(command_is_stale({}, 300, now=now))
        self.assertTrue(command_is_stale({"ts": (now - timedelta(seconds=301)).isoformat()}, 300, now=now))
        self.assertFalse(command_is_stale({"ts": now.isoformat()}, 300, now=now))
        self.assertEqual(nullable_number("($1,234.50)"), -1234.5)

    def test_publish_requires_successful_delivery_report(self):
        snapshot = {"account_id": "A", "positions": []}
        producer = FakeProducer()
        self.assertEqual(TradingRunner._publish_snapshot(producer, "account-details", snapshot), (0, 123))
        self.assertEqual(producer.key, "")
        self.assertEqual(producer.value, snapshot)

        with self.assertRaisesRegex(RuntimeError, "rejected"):
            TradingRunner._publish_snapshot(
                FakeProducer(error="Topic authorization failed"), "account-details", snapshot
            )
        with self.assertRaisesRegex(RuntimeError, "remain queued"):
            TradingRunner._publish_snapshot(
                FakeProducer(remaining=1, report=False), "account-details", snapshot
            )
        with self.assertRaisesRegex(RuntimeError, "no delivery report"):
            TradingRunner._publish_snapshot(
                FakeProducer(report=False), "account-details", snapshot
            )


if __name__ == "__main__":
    unittest.main()
