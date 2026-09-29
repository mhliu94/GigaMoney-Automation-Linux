"""Offline order reporting: mapping, delivery acknowledgments and crash recovery."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from jsonschema import Draft202012Validator, FormatChecker

from gigamoney.config import Settings
from gigamoney.order_status import OrderStatusPublisher, order_snapshot
from gigamoney.runner import TradingRunner
from tests.test_api_trading import broker_order
from tests.test_runner import FakeProducer


class OrderStatusTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.settings = Settings({"gigamoney": {"accountId": "TEST", "accountNumId": 7, "backend": "api"}},
                                 root / "config.json", root)
        self.now = datetime.now(timezone.utc)
        self.row = broker_order(submitted_at=str(int(self.now.timestamp())))
        self.snapshot = order_snapshot(self.row, self.settings, observed_at=self.now)
        self.publisher = OrderStatusPublisher(self.settings)
        self.sent = []

        def deliver(snapshot):
            self.sent.append(copy.deepcopy(snapshot))
            return 1, len(self.sent)
        self.deliver = deliver

    def publish(self, snapshot=None):
        return self.publisher.publish([snapshot or self.snapshot], self.deliver)

    def test_mapping_preserves_exact_id_and_broker_creation_time(self):
        result = self.snapshot
        self.assertEqual(result["order_id"], "9007199254740993123")
        self.assertEqual(datetime.fromisoformat(result["created_at"]).timestamp(), int(self.row["submitted_at"]))
        self.assertEqual((result["side"], result["order_type"], result["status"]), ("BUY", "LIMIT", "NEW"))
        self.assertEqual((result["quantity"], result["filled_quantity"], result["average_fill_price"]), (2, 0, None))
        self.assertEqual((result["limit_price"], result["currency"], result["time_in_force"]), (2.28, "USD", "DAY"))
        self.assertEqual(result["account_num_id"], 7)
        self.assertNotIn("aaid", result)

    def test_published_snapshots_match_ktrader_schema(self):
        # Exact upstream contract at mhliu94/KTrader commit 2d6c9d3,
        # order_ui/schema/order-status.v1.schema.json (2026-09-29).
        schema = json.loads((Path(__file__).parent / "fixtures/order-status.v1.schema.json").read_text())
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        self.publish()
        for fields in ({"status": "PartialFilledStatus", "executed_qty": "1", "executed_price": "2.3"},
                       {"status": "FilledStatus", "executed_qty": "2", "executed_price": "2.3"},
                       {"status": "PartialWithdrawal", "executed_qty": "1", "executed_price": "2.3"},
                       {"status": "RejectedStatus", "msg": "broker rejection"},
                       {"order_type": "MO", "action": 2},
                       {"order_type": "MO", "quantity": "0", "submitted_amount": "25"},
                       {"order_type": "unmapped", "status": "unmapped"}):
            self.publish(order_snapshot(dict(self.row, **fields), self.settings, observed_at=self.now))
        for snapshot in self.sent:
            validator.validate(snapshot)

    def test_broker_history_supplies_latest_update_time(self):
        row = dict(self.row, submitted_at="1790097285", done_at="0", order_histories=[
            {"status": "FilledStatus", "time": "1790097286"}, {"status": "NewStatus", "time": "1790097285"}])
        result = order_snapshot(row, self.settings, observed_at=self.now)
        self.assertEqual(datetime.fromisoformat(result["updated_at"]).timestamp(), 1790097286)

    def test_status_mapping_and_partial_cancellation(self):
        for raw, expected in (("NotReported", "PENDING_NEW"), ("WaitToCancel", "PENDING_CANCEL"),
                              ("RejectedStatus", "REJECTED"), ("ExpiredStatus", "EXPIRED"),
                              ("PartialWithdrawal", "CANCELED"), ("FutureStatus", "UNKNOWN")):
            with self.subTest(raw=raw):
                result = order_snapshot(dict(self.row, status=raw, executed_qty="1", executed_price="2.31",
                                             withdrawal_reason="test reason"), self.settings, observed_at=self.now)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["filled_quantity"], 1)
                self.assertEqual(result["average_fill_price"], 2.31)
                self.assertIn(raw, result["message"])
                self.assertIn("test reason", result["message"])

    def test_rejects_invalid_timestamps_numbers_and_execution_states(self):
        for fields in ({"submitted_at": ""}, {"submitted_at": "2026-09-29T12:00:00"},
                       {"executed_qty": "NaN"}, {"executed_qty": "3"}, {"price": "Infinity"},
                       {"status": "FilledStatus"}, {"status": "PartialFilledStatus"},
                       {"quantity": "-1"}, {"action": 3}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                order_snapshot(dict(self.row, **fields), self.settings, observed_at=self.now)

    def test_market_sell_and_unknown_order_type(self):
        for kind, expected in (("MO", "MARKET"), ("unmapped", "OTHER")):
            result = order_snapshot(dict(self.row, action=2, order_type=kind, price=""),
                                    self.settings, observed_at=self.now, command_id="original-command")
            self.assertEqual((result["side"], result["order_type"]), ("SELL", expected))
            self.assertNotIn("limit_price", result)
            self.assertEqual(result["command_id"], "original-command")

    def test_unchanged_polls_do_not_publish_and_fills_prices_and_statuses_do(self):
        self.assertEqual(self.publish(), 1)
        self.assertEqual(self.publish(dict(self.snapshot, updated_at=(self.now + timedelta(seconds=30)).isoformat())), 0)
        partial = dict(self.snapshot, status="PARTIALLY_FILLED", filled_quantity=1, average_fill_price=2.3)
        self.publish(partial)
        self.publish(dict(partial, average_fill_price=2.31))
        self.publish(dict(partial, status="CANCELED"))
        self.assertEqual([row["sequence"] for row in self.sent], [0, 1, 2, 3])
        record = json.loads(self.publisher.path(self.snapshot["order_id"]).read_text())
        self.assertEqual(record["published"], self.sent[-1])
        self.assertIsNone(record["pending"])
        self.assertEqual(self.publisher.path(self.snapshot["order_id"]).stat().st_mode & 0o777, 0o600)

    def test_failed_delivery_survives_restart_and_retries_exact_payload_before_new_state(self):
        fail = Mock(side_effect=RuntimeError("Kafka timeout"))
        with self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publisher.publish([self.snapshot], fail)
        original = copy.deepcopy(fail.call_args.args[0])
        record = json.loads(self.publisher.path(self.snapshot["order_id"]).read_text())
        self.assertIsNone(record["published"])
        self.assertEqual(record["pending"], original)
        self.publisher = OrderStatusPublisher(self.settings)
        self.publish(dict(self.snapshot, status="FILLED", filled_quantity=2, average_fill_price=2.3))
        self.assertEqual(self.sent[0], original)
        self.assertEqual([row["sequence"] for row in self.sent], [0, 1])
        self.assertEqual(self.sent[-1]["status"], "FILLED")

    def test_restart_republishes_completed_order_once_with_same_sequence(self):
        filled = dict(self.snapshot, status="FILLED", filled_quantity=2, average_fill_price=2.3)
        self.publish(filled)
        self.publisher = OrderStatusPublisher(self.settings)
        self.publish(filled)
        self.publish(filled)
        self.assertEqual(len(self.sent), 2)
        self.assertEqual(self.sent[0], self.sent[1])
        # The server permits a correction to a terminal status, with a new sequence.
        self.publish(dict(filled, average_fill_price=2.31))
        self.assertEqual(self.sent[-1]["sequence"], 1)

    def test_pending_order_retried_even_when_missing_from_next_broker_snapshot(self):
        with self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publisher.publish([self.snapshot], Mock(side_effect=RuntimeError("timeout")))
        self.publisher = OrderStatusPublisher(self.settings)
        self.assertEqual(self.publisher.publish([], self.deliver), 1)
        self.assertEqual(self.sent[0]["order_id"], self.snapshot["order_id"])
        self.assertEqual(self.sent[0]["sequence"], 0)
        self.assertEqual(self.publisher.publish([], self.deliver), 0)

    def test_crash_after_kafka_ack_before_disk_save_retries_same_sequence(self):
        from gigamoney.api import save_session
        writes = 0

        def save(path, record):
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("disk error after delivery")
            save_session(path, record)
        with patch("gigamoney.order_status.save_session", side_effect=save), \
                self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publish()
        self.publisher = OrderStatusPublisher(self.settings)
        self.publish()
        self.assertEqual(self.sent[0], self.sent[1])

    def test_prior_day_and_future_orders_ignored_using_configured_day(self):
        self.settings.data["api"] = {"orderTimezone": "America/New_York"}
        for created in (self.now - timedelta(days=2), self.now + timedelta(days=2)):
            self.assertEqual(self.publish(dict(self.snapshot, created_at=created.isoformat())), 0)
        self.assertEqual(self.sent, [])

    def test_midnight_and_dst_day_boundaries(self):
        for now, current, old in (("2026-03-08T05:01:00+00:00", "2026-03-08T05:00:00+00:00", "2026-03-08T04:59:00+00:00"),
                                  ("2026-11-01T06:01:00+00:00", "2026-11-01T05:00:00+00:00", "2026-11-01T03:59:00+00:00")):
            with patch("gigamoney.order_status.datetime") as clock:
                clock.now.return_value = datetime.fromisoformat(now)
                clock.fromisoformat.side_effect = datetime.fromisoformat
                self.publisher = OrderStatusPublisher(self.settings)
                snapshot = dict(self.snapshot, order_id=now, created_at=current, updated_at=now)
                self.assertEqual(self.publish(snapshot), 1)
                self.assertEqual(self.publish(dict(snapshot, order_id=old, created_at=old)), 0)

    def test_failure_of_one_order_does_not_block_another(self):
        def deliver(snapshot):
            if snapshot["order_id"] == self.snapshot["order_id"]:
                raise RuntimeError("failure")
            return self.deliver(snapshot)
        with self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publisher.publish([self.snapshot, dict(self.snapshot, order_id="12")], deliver)
        self.assertEqual([row["order_id"] for row in self.sent], ["12"])

    def test_corrupt_journal_never_resets_sequence(self):
        self.publish()
        with patch.object(Path, "read_text", return_value="not JSON"), \
                self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publish(dict(self.snapshot, limit_price=2.29))
        self.assertEqual(len(self.sent), 1)

    def test_creation_time_immutable_and_update_time_never_decreases(self):
        self.publish()
        with self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            self.publish(dict(self.snapshot, created_at=(self.now + timedelta(seconds=1)).isoformat()))
        self.publish(dict(self.snapshot, limit_price=2.31, updated_at=self.snapshot["created_at"]))
        self.assertEqual(self.sent[-1]["updated_at"], self.sent[0]["updated_at"])

    def test_runner_uses_configured_topic_stable_key_and_requires_delivery_ack(self):
        self.settings.data["kafka"] = {"orderStatusTopic": "custom-orders"}
        runner = TradingRunner(self.settings)
        producer = FakeProducer(report=False)
        with self.assertLogs("gigamoney", level="ERROR"), self.assertRaises(RuntimeError):
            runner._publish_orders(producer, [self.snapshot])
        producer.report = True
        with patch.object(producer, "produce", wraps=producer.produce) as produce:
            self.assertEqual(runner._publish_orders(producer, [self.snapshot]), 1)
            self.assertEqual(produce.call_args.args[0], "custom-orders")
        self.assertEqual(json.loads(producer.key), ["TEST", self.snapshot["order_id"]])
        self.assertEqual(producer.value["sequence"], 0)


if __name__ == "__main__":
    unittest.main()
