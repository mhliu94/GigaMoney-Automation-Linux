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
                snapshots.append(snapshot)
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

    def test_successful_orders_publish_fresh_holdings_before_next_poll(self):
        cases = [
            ({"type": "MARKET_ORDER"}, "market", None),
            ({"type": "LIMIT_ORDER"}, "limit", False),
            ({"type": "LIMIT_ORDER_FOK"}, "limit", True),
            ({"type": "LIMIT_ORDER", "time_in_force": "FOK"}, "limit", True),
            ({"type": "LIMIT_ORDER", "cancel_unfilled": True}, "limit", True),
        ]
        for command, kind, kill in cases:
            with self.subTest(command=command):
                events, snapshots, operations = self.run_command(command)
                self.assertEqual(events, [
                    "holdings", "publish", "poll", kind, "commit", "holdings", "publish", "poll",
                ])
                self.assertEqual([snapshot["positions"][0]["qty"] for snapshot in snapshots], [1, 2])
                self.assertTrue(snapshots[-1]["trading_enabled"])
                if kill is not None:
                    self.assertEqual(operations.call_args_list[1].args[1]["kill"], kill)

    def test_unsuccessful_or_unsubmitted_orders_do_not_refresh(self):
        for result in ({"ok": False, "submitted": True}, {"ok": True, "submitted": False}):
            with self.subTest(result=result):
                events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, result=result)
                self.assertEqual(len(snapshots), 1)
                self.assertEqual(events.count("holdings"), 1)

    def test_api_post_action_delay_includes_cancel_and_acknowledged_failures(self):
        from gigamoney.api_trading import OrderUncertain
        for kind, operation, result in (
            ("MARKET_ORDER", "market", None),
            ("LIMIT_ORDER", "limit", None),
            ("LIMIT_ORDER_FOK", "limit", OrderUncertain("Cancel unconfirmed", submitted=True)),
            ("CANCEL_OPEN_ORDERS", "cancel", {"dryRun": False, "canceledCount": 1}),
            ("CANCEL_OPEN_ORDERS", "cancel", OrderUncertain("Some orders remain open")),
        ):
            with self.subTest(kind=kind, result=result):
                events, _, _ = self.run_command({"type": kind}, backend="api", result=result)
                self.assertEqual(events, ["recover", "holdings", "publish", "poll", operation,
                    "commit", ("sleep", 2), "holdings", "publish", "poll"])

    def test_api_delayed_publication_resets_periodic_interval_from_delivery(self):
        events, snapshots, _ = self.run_command({"type": "MARKET_ORDER"}, backend="api",
            query_seconds=4, delivery_seconds=2, holdings_interval=30, later_polls=(47, 48))
        self.assertEqual(events, ["recover", "holdings", "publish", "poll", "market", "commit",
            ("sleep", 2), "holdings", "publish", "poll", "poll", "holdings", "publish", "poll"])
        self.assertEqual(len(snapshots), 3)

    def test_failed_order_does_not_refresh(self):
        with self.assertLogs("gigamoney", level="ERROR"):
            events, snapshots, _ = self.run_command(
                {"type": "LIMIT_ORDER_FOK"}, result=RuntimeError("Order failed"),
            )
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(events.count("holdings"), 1)

    def test_acknowledged_api_order_still_refreshes_if_cancel_confirmation_fails(self):
        from gigamoney.api_trading import OrderUncertain
        with self.assertLogs("gigamoney", level="ERROR"):
            events, snapshots, _ = self.run_command({"type": "LIMIT_ORDER_FOK"},
                result=OrderUncertain("Cancellation unconfirmed", submitted=True))
        self.assertEqual(len(snapshots), 2)
        self.assertEqual(events, ["holdings", "publish", "poll", "limit", "commit", "holdings", "publish", "poll"])

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

    def test_refresh_failure_does_not_fail_or_repeat_successful_order(self):
        for failure in (
            {"refresh_error": RuntimeError("Holdings query failed")},
            {"delivery_error": "Topic authorization failed"},
        ):
            with self.subTest(failure=failure), self.assertLogs("gigamoney", level="ERROR") as logs:
                events, _, _ = self.run_command(
                    {"type": "MARKET_ORDER"}, commit_failed=False, **failure,
                )
                self.assertEqual(events.count("market"), 1)
                self.assertEqual(events.count("holdings"), 2)
                self.assertEqual(events[:6], [
                    "holdings", "publish", "poll", "market", "commit", "holdings",
                ])
                self.assertTrue(any("Holdings publication failed" in line for line in logs.output))
                self.assertFalse(any("Command processing failed" in line for line in logs.output))

    def test_periodic_publication_continues_after_immediate_refresh(self):
        events, snapshots, _ = self.run_command(
            {"type": "MARKET_ORDER"}, later_polls=(129, 130),
        )
        self.assertEqual(events, [
            "holdings", "publish", "poll", "market", "commit", "holdings", "publish",
            "poll", "poll", "holdings", "publish", "poll",
        ])
        self.assertEqual(len(snapshots), 3)

    def test_order_publication_failure_preserves_holdings_and_never_replays_command(self):
        with self.assertLogs("gigamoney", level="ERROR") as logs:
            events, snapshots, _ = self.run_command(
                {"type": "MARKET_ORDER"}, backend="api", commit_failed=False,
                order_delivery_error=RuntimeError("Order status delivery failed"),
            )
        self.assertEqual(events.count("market"), 1)
        self.assertEqual(events.count("commit"), 1)
        self.assertEqual(len(snapshots), 2)
        self.assertTrue(any("Order status refresh failed" in message for message in logs.output))
        self.assertFalse(any("Holdings publication failed" in message for message in logs.output))

    def test_every_update_resets_interval_after_query_and_delivery_finish(self):
        # Startup publishes at t=6. The order refresh publishes at t=16,
        # so periodic queries should start at t=136 and then t=262.
        events, snapshots, _ = self.run_command(
            {"type": "MARKET_ORDER"}, query_seconds=4, delivery_seconds=2,
            later_polls=(130, 135, 136, 256, 261, 262),
        )
        self.assertEqual(events, [
            "holdings", "publish", "poll", "market", "commit", "holdings", "publish",
            "poll", "poll", "poll", "holdings", "publish",
            "poll", "poll", "poll", "holdings", "publish", "poll",
        ])
        self.assertEqual(len(snapshots), 4)

    def test_logs_link_order_to_refresh_and_clear_context_for_periodic_updates(self):
        contexts = []

        def snapshot(*args):
            contexts.append(command_context.get())
            return holdings_snapshot(*args)

        token = command_context.set("outer-context")
        try:
            with (patch("gigamoney.runner.holdings_snapshot", side_effect=snapshot),
                  self.assertLogs("gigamoney", level="INFO") as logs):
                self.run_command({"type": "MARKET_ORDER"}, later_polls=(130,))
            self.assertEqual(command_context.get(), "outer-context")
        finally:
            command_context.reset(token)
        self.assertEqual(contexts, [None, "test-order", None])
        commands = self.command_results(logs)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["outcome"], "success")
        self.assertTrue(commands[0]["committed"])
        messages = [record.getMessage() for record in logs.records]
        publications = [message for message in messages if message.startswith("Published holdings")]
        self.assertEqual(len(publications), 3)
        self.assertIn("trigger=startup command_id=None", publications[0])
        self.assertIn("trigger=post_order command_id=test-order", publications[1])
        self.assertIn("trigger=periodic command_id=None", publications[2])

    def test_thirty_seconds_from_completion_after_each_order_and_periodic_update(self):
        for interval in (30, None):  # Explicit setting and absent-setting default.
            for kind, operation in (("MARKET_ORDER", "market"), ("LIMIT_ORDER", "limit"),
                                    ("LIMIT_ORDER_FOK", "limit")):
                with self.subTest(interval=interval, kind=kind):
                    # Startup finishes at 6, order update at 16. Queries at 46 and 82.
                    events, snapshots, _ = self.run_command(
                        {"type": kind}, holdings_interval=interval, query_seconds=4, delivery_seconds=2,
                        later_polls=(36, 45, 46, 76, 81, 82),
                    )
                    self.assertEqual(events, [
                        "holdings", "publish", "poll", operation, "commit", "holdings", "publish",
                        "poll", "poll", "poll", "holdings", "publish",
                        "poll", "poll", "poll", "holdings", "publish", "poll",
                    ])
                    self.assertEqual(len(snapshots), 4)

    def test_thirty_second_startup_and_periodic_schedule_without_order(self):
        events, snapshots, _ = self.run_command(
            {"type": "UNSUPPORTED"}, holdings_interval=None, query_seconds=4, delivery_seconds=2,
            later_polls=(30, 35, 36, 66, 71, 72),
        )
        self.assertEqual(events, [
            "holdings", "publish", "poll", "commit",
            "poll", "poll", "poll", "holdings", "publish",
            "poll", "poll", "poll", "holdings", "publish", "poll",
        ])
        self.assertEqual(len(snapshots), 3)

    def test_failed_updates_wait_thirty_seconds_from_failure_without_order_replay(self):
        for failure, polls, failed_events in (
            ({"refresh_error": RuntimeError("Query failed")}, (40, 43, 44, 74, 77, 78), ["holdings"]),
            ({"delivery_error": "Delivery rejected"}, (40, 45, 46, 76, 81, 82), ["holdings", "publish"]),
        ):
            with self.subTest(failure=failure), self.assertLogs("gigamoney", level="ERROR"):
                events, _, _ = self.run_command(
                    {"type": "MARKET_ORDER"}, holdings_interval=30, query_seconds=4, delivery_seconds=2,
                    later_polls=polls, **failure,
                )
            self.assertEqual(events, [
                "holdings", "publish", "poll", "market", "commit", *failed_events,
                "poll", "poll", "poll", *failed_events,
                "poll", "poll", "poll", *failed_events, "poll",
            ])

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

    def test_publication_failure_does_not_change_successful_command_outcome(self):
        with self.assertLogs("gigamoney", level="INFO") as logs:
            self.run_command({"type": "MARKET_ORDER"}, delivery_error="Delivery rejected")
        commands = self.command_results(logs)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["outcome"], "success")
        failures = [record.getMessage() for record in logs.records
                    if "Holdings publication failed" in record.getMessage()]
        self.assertEqual(len(failures), 1)
        self.assertIn("stage=publish trigger=post_order command_id=test-order", failures[0])

    def test_periodic_updates_reset_interval_without_an_order(self):
        # Startup publishes at t=6. Each query and delivery takes six seconds,
        # so periodic queries should start at t=126 and then t=252.
        events, snapshots, _ = self.run_command(
            {"type": "UNSUPPORTED"}, query_seconds=4, delivery_seconds=2,
            later_polls=(120, 125, 126, 246, 251, 252),
        )
        self.assertEqual(events, [
            "holdings", "publish", "poll", "commit",
            "poll", "poll", "poll", "holdings", "publish",
            "poll", "poll", "poll", "holdings", "publish", "poll",
        ])
        self.assertEqual(len(snapshots), 3)


if __name__ == "__main__":
    unittest.main()
