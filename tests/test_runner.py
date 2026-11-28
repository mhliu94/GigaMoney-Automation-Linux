from gigamoney.account_delta import AccountReplica
from tests.manual_holdings_clock import ManualTimer
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
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
            "account_id", "account_num_id", "cash", "cash_by_currency", "available_cash_by_currency", "cash_basis", "positions", "ts",
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


class RunnerLoopTests(unittest.TestCase):
    @staticmethod
    def command_results(logs):
        prefix = "Command finished "
        return [json.loads(record.getMessage()[len(prefix):]) for record in logs.records
                if record.getMessage().startswith(prefix)]

    def run_command(
        self, command, *, result=None, trading_enabled=True, commit_failed=True,
        refresh_error=None, delivery_error=None, later_polls=(),
        query_seconds=0, delivery_seconds=0, holdings_interval=120, backend="android",
        order_delivery_error=None,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings({
                "gigamoney": {
                    "accountId": "GIGA-A", "tradingEnabledDefault": trading_enabled, "backend": backend,
                },
                "kafka": {
                    "bootstrapServers": "unused:9092", "commandTopic": "commands",
                    "accountDetailsTopic": "account-details", "consumerGroupId": "test",
                    "commitFailedCommands": commit_failed,
                },
            }, root / "config.json", root)
            if holdings_interval is not None:
                settings.data["kafka"]["holdingsIntervalSeconds"] = holdings_interval
            runner = TradingRunner(settings)
            command = {
                "account_id": "GIGA-A", "command_id": "test-order",
                "ts": datetime.now(timezone.utc).isoformat(),
                "symbol": "BILI", "side": "BUY", "qty_shares": 2, "limit_price": 17.4,
                **command,
            }
            message = Mock()
            message.error.return_value = None
            message.value.return_value = json.dumps(command).encode()
            now = 0
            polls = iter([(10, message), *[(when, None) for when in later_polls]])
            events = []
            snapshots = []
            account_replica = AccountReplica()
            queries = 0
            producer = FakeProducer()
            original_publish_orders = runner._publish_orders

            def publish_orders(*args):
                if order_delivery_error is not None:
                    raise order_delivery_error
                return original_publish_orders(*args)

            def poll(timeout):
                nonlocal now
                events.append("poll")
                item = next(polls, None)
                if item is None:
                    runner.running = False
                    return None
                when, value = item
                now = max(now, when)
                runner.holdings_timer.advance()
                return value

            def operation(kind, arguments):
                nonlocal queries, now
                events.append(kind)
                if kind == "recover":
                    return {"ok": True, "recovered": 0}
                if kind != "holdings":
                    if isinstance(result, Exception):
                        raise result
                    return result if result is not None else {"ok": True, "submitted": True}
                queries += 1
                now += query_seconds
                if queries > 1:
                    producer.error = delivery_error
                    if refresh_error:
                        raise refresh_error
                return {"positions": [{"symbol": "BILI", "quantity": queries, "cost": 17.4}]}

            def publish(producer, topic, snapshot, **kwargs):
                nonlocal now
                events.append("publish")
                snapshots.append(account_replica.apply(snapshot) if "account_update_version" in snapshot else snapshot)
                self.assertEqual(topic, "account-details")
                now += delivery_seconds
                return TradingRunner._publish_snapshot(producer, topic, snapshot)

            def sleep(seconds):
                nonlocal now
                events.append(("sleep", seconds))
                now += seconds

            consumer = Mock()
            consumer.poll.side_effect = poll
            consumer.commit.side_effect = lambda **kwargs: events.append("commit")
            kafka = SimpleNamespace(
                Consumer=Mock(return_value=consumer), Producer=Mock(return_value=producer),
                KafkaException=RuntimeError,
            )
            with (
                patch("gigamoney.runner.HoldingsTimer", ManualTimer),
                patch.dict(sys.modules, {"confluent_kafka": kafka}),
                patch("gigamoney.runner.signal.signal"),
                patch("gigamoney.runner.time.monotonic", side_effect=lambda: now),
                patch("gigamoney.runner.time.sleep", side_effect=sleep),
                patch.object(runner, "_operation", side_effect=operation) as operations,
                patch.object(runner, "_publish_snapshot", side_effect=publish),
                patch.object(runner, "_publish_orders", side_effect=publish_orders),
            ):
                runner.run()
            consumer.commit.assert_called_once_with(message=message, asynchronous=False)
            consumer.close.assert_called_once_with()
            return events, snapshots, operations

    def test_successful_orders_do_not_query_holdings_before_next_poll(self):
        for cmd in ("MARKET_ORDER","LIMIT_ORDER","LIMIT_ORDER_FOK","CANCEL_OPEN_ORDERS"):
            with self.subTest(cmd=cmd):
                events, snapshots, _ = self.run_command({"type":cmd})
                self.assertEqual(events.count("holdings"),1)
                self.assertEqual(events.count("commit"),1)
                self.assertEqual(len(snapshots),1)

    def test_unsuccessful_or_unsubmitted_orders_do_not_refresh(self):
        for result in ({"ok": False, "submitted": True}, {"ok": True, "submitted": False}):
            with self.subTest(result=result):
                events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, result=result)
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(events.count("holdings"), 1)

    def test_api_orders_and_cancel_have_no_reporting_sleep(self):
        for command in ("MARKET_ORDER","LIMIT_ORDER","CANCEL_OPEN_ORDERS"):
            events, _, _ = self.run_command({"type":command}, backend="api")
            self.assertEqual(events.count("holdings"),1)
            self.assertFalse(any(isinstance(e,tuple) and e[0]=="sleep" for e in events))

    def test_order_does_not_reset_the_holdings_timer(self):
        events,snapshots,_=self.run_command({"type":"MARKET_ORDER"},backend="api",holdings_interval=30,later_polls=(29,30,59,60))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(len(snapshots),3)

    def test_failed_order_does_not_refresh(self):
        with self.assertLogs("gigamoney", level="ERROR"):
            events, snapshots, _ = self.run_command(
                {"type": "LIMIT_ORDER_FOK"}, result=RuntimeError("Order failed"),
            )
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(events.count("holdings"), 1)

    def test_ambiguous_cancel_keeps_recovery_separate_from_holdings(self):
        from gigamoney.api_trading import OrderUncertain
        events,_,_=self.run_command({"type":"LIMIT_ORDER_FOK"}, result=OrderUncertain("unconfirmed",submitted=True), backend="api")
        self.assertEqual(events.count("limit"),1)
        self.assertEqual(events.count("holdings"),1)
        self.assertGreaterEqual(events.count("recover"),2)

    def test_other_commands_and_ignored_orders_do_not_refresh(self):
        for command in (
            {"type": "CANCEL_OPEN_ORDERS"},
            {"type": "SET_TRADING_ENABLED", "trading_enabled": False},
            {"type": "UNSUPPORTED"},
            {"type": "MARKET_ORDER", "account_id": "another-account"},
            {"type": "MARKET_ORDER", "ts": "2000-01-01T00:00:00Z"},
        ):
            with self.subTest(command=command):
                events, snapshots, _ = self.run_command(command)
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(events.count("holdings"), 1)
        events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, trading_enabled=False)
        self.assertNotIn("market", events)
        self.assertEqual(len(snapshots), 1)

    def test_timer_read_failure_does_not_fail_or_repeat_successful_order(self):
        with self.assertLogs("gigamoney",level="ERROR"):
            events,_,_=self.run_command({"type":"MARKET_ORDER"},refresh_error=RuntimeError("offline"),holdings_interval=30,later_polls=(30,))
        self.assertEqual(events.count("market"),1)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(events.count("holdings"),2)

    def test_periodic_checkpoint_after_order(self):
        events,snapshots,_=self.run_command({"type":"MARKET_ORDER"},holdings_interval=30,query_seconds=2,delivery_seconds=1,
            later_polls=(29,30,59,60))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),3)

    def test_order_delivery_failure_preserves_independent_checkpoints(self):
        with self.assertLogs("gigamoney",level="ERROR") as logs:
            events,snapshots,_=self.run_command({"type":"MARKET_ORDER"},backend="api",order_delivery_error=RuntimeError("offline"),holdings_interval=30,later_polls=(30,))
        self.assertEqual(events.count("market"),1)
        self.assertEqual(len(snapshots),2)
        self.assertTrue(any("Order status refresh failed" in s for s in logs.output))

    def test_slow_reads_do_not_move_the_next_deadline(self):
        events,snapshots,_=self.run_command({"type":"UNSUPPORTED"},holdings_interval=20,query_seconds=2,delivery_seconds=1,
            later_polls=(19,20,39,40))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),3)

    def test_timer_publication_keeps_command_result_independent(self):
        with self.assertLogs("gigamoney",level="INFO") as logs:
            events,snapshots,_=self.run_command({"type":"MARKET_ORDER"},holdings_interval=30,later_polls=(30,))
        self.assertEqual(self.command_results(logs)[0]["outcome"],"success")
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),2)

    def test_limit_order_preserves_one_minute_timer(self):
        events,snapshots,_=self.run_command({"type":"LIMIT_ORDER"},holdings_interval=60,query_seconds=2,delivery_seconds=1,
            later_polls=(59,60,119,120))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),3)

    def test_idle_startup_and_thirty_second_timer(self):
        events,snapshots,_=self.run_command({"type":"UNSUPPORTED"},holdings_interval=30,query_seconds=2,delivery_seconds=1,
            later_polls=(29,30,59,60))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),3)

    def test_failed_checkpoint_does_not_replay_and_next_tick_retries(self):
        events,snapshots,_=self.run_command({"type":"MARKET_ORDER"},holdings_interval=25,query_seconds=2,delivery_seconds=1,
            later_polls=(24,25,49,50), refresh_error=RuntimeError("read failed"))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),1)

    def test_ignored_commands_are_distinct_from_successful_actions(self):
        for command, options, reason in (
            ({"type": "MARKET_ORDER", "ts": "2000-01-01T00:00:00Z"}, {}, "stale"),
            ({"type": "MARKET_ORDER"}, {"trading_enabled": False}, "trading_disabled"),
            ({"type": "UNSUPPORTED"}, {}, "unsupported_type"),
        ):
            with self.subTest(reason=reason), self.assertLogs("gigamoney", level="INFO") as logs:
                self.run_command(command, **options)
            commands = self.command_results(logs)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["outcome"], "ignored")
            self.assertEqual(commands[0]["reason"], reason)
            self.assertTrue(commands[0]["committed"])

    def test_periodic_publication_failure_preserves_successful_command(self):
        with self.assertLogs("gigamoney",level="INFO") as logs:
            events,_,_=self.run_command({"type":"MARKET_ORDER"},delivery_error="delivery unavailable",holdings_interval=30,later_polls=(30,))
        self.assertEqual(self.command_results(logs)[0]["outcome"],"success")
        self.assertEqual(events.count("commit"),1)
        self.assertTrue(any("Holdings publication failed" in s for s in logs.output))

    def test_idle_clock_uses_configured_interval(self):
        events,snapshots,_=self.run_command({"type":"UNSUPPORTED"},holdings_interval=45,query_seconds=2,delivery_seconds=1,
            later_polls=(44,45,89,90))
        self.assertEqual(events.count("holdings"),3)
        self.assertEqual(events.count("commit"),1)
        self.assertEqual(len(snapshots),3)


if __name__ == "__main__":
    unittest.main()
