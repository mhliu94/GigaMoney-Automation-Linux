"""Open-only discovery and durable retirement after the first terminal report."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from gigamoney.api_trading import ApiTrading, OrderFailed
from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
from gigamoney.order_status import OrderStatusPublisher, order_snapshot
from tests.test_api_trading import Broker, broker_order


class OrderPollingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.settings = Settings({"gigamoney": {"backend": "api", "accountId": "TEST"}},
                                 root / "config.json", root)
        self.broker = Broker()
        self.api = self.restart()

    def restart(self):
        return ApiTrading(self.settings, client=self.broker, price_books=Mock(), sleeper=lambda _: None)

    def detail_reads(self):
        return [call for call in self.broker.calls if call[1] == "order_detail"]

    def test_terminal_receipts_seed_hundreds_of_retired_orders_without_polling_them(self):
        now = datetime.now(timezone.utc)
        snapshots = [order_snapshot(broker_order(str(1000 + index), status="FilledStatus", executed_qty="2",
                                                executed_price="2.3"), self.settings, observed_at=now)
                     for index in range(300)]
        publisher = OrderStatusPublisher(self.settings)
        publisher.publish(snapshots, lambda _: (0, 1))
        for _ in range(2):
            self.api = self.restart()
            result = self.api.query()
            self.assertEqual(len(result["orders"]), 300)
            self.assertEqual(result["orderStatusErrors"], 0)
            self.assertEqual(publisher.publish(result["orders"], lambda _: self.fail("Unchanged order published")), 0)
        self.assertEqual(self.detail_reads(), [])
        reads = [call for call in self.broker.calls if call[1] == "orders"]
        self.assertEqual(len(reads), 2)
        self.assertTrue(all(call[2]["status"] == 1 and call[2]["page"] == 1 for call in reads))

    def test_missing_open_order_requires_terminal_evidence_and_retries_after_read_failure(self):
        self.broker.orders["11"] = broker_order("11")
        self.api.query()
        self.broker.orders["11"].update(status="FilledStatus", executed_qty="2", executed_price="2.3")
        self.broker.fail_detail = True
        with self.assertLogs("gigamoney", level="ERROR"):
            failed = self.api.query()
        self.assertEqual(failed["orderStatusErrors"], 1)
        self.assertEqual(failed["orders"][0]["status"], "NEW")
        self.api = self.restart()
        self.broker.fail_detail = False
        confirmed = self.api.query()
        self.assertEqual(confirmed["orders"][0]["status"], "FILLED")
        reads = len(self.detail_reads())
        self.api.query()
        self.assertEqual(len(self.detail_reads()), reads)

    def test_partial_fill_and_terminal_change_publish_once_each(self):
        self.broker.orders["11"] = broker_order("11")
        publisher = OrderStatusPublisher(self.settings)
        sent = []

        def deliver(snapshot):
            sent.append(dict(snapshot))
            return 0, len(sent)

        for status, filled in (("NewStatus", "0"), ("PartialFilledStatus", "1"), ("PartialWithdrawal", "1")):
            self.broker.orders["11"].update(status=status, executed_qty=filled, executed_price="2.3")
            publisher.publish(self.api.query()["orders"], deliver)
            publisher.publish(self.api.query()["orders"], deliver)
        self.assertEqual([row["status"] for row in sent], ["NEW", "PARTIALLY_FILLED", "CANCELED"])
        self.assertEqual([row["sequence"] for row in sent], [0, 1, 2])
        self.assertEqual(len(self.detail_reads()), 1)

    def test_prior_and_future_date_orders_are_not_tracked_and_midnight_drops_old_pending_ids(self):
        now = datetime.now(timezone.utc)
        for identifier, offset in (("11", -1), ("12", 0), ("13", 1)):
            self.broker.orders[identifier] = broker_order(identifier,
                submitted_at=str(int((now + timedelta(days=offset)).timestamp())))
        result = self.api.query()
        self.assertEqual([row["order_id"] for row in result["orders"]], ["12"])
        tomorrow = now + timedelta(days=1)
        with patch("gigamoney.order_polling.datetime") as clock:
            clock.now.return_value = tomorrow
            result = self.api.query()
        self.assertEqual([row["order_id"] for row in result["orders"]], ["13"])
        self.assertEqual(self.detail_reads(), [])
        state = json.loads((self.settings.work_dir / "api/order-polling.json").read_text())
        self.assertEqual(list(state["pending"]), ["13"])

    def test_fast_local_fill_uses_submit_confirmation_without_polling_a_terminal_order(self):
        original = self.broker.write

        def write(operation, body, **kwargs):
            response = original(operation, body, **kwargs)
            if operation == "submit":
                self.broker.orders[response["order_id_str"]].update(
                    status="FilledStatus", executed_qty=body["quantity"], executed_price=body["price"])
            return response

        self.broker.write = write
        token = command_context.set("fast-fill")
        try:
            self.api.limit_order("BIYA", "2.28", "2", "Buy")
        finally:
            command_context.reset(token)
        self.broker.calls.clear()
        self.api = self.restart()
        result = self.api.query()
        self.assertEqual(result["orders"][0]["status"], "FILLED")
        self.assertEqual(result["orders"][0]["command_id"], "fast-fill")
        self.assertEqual(self.detail_reads(), [])

    def test_confirmed_broker_rejection_is_cached_without_terminal_repoll(self):
        self.broker.initial_status = "RejectedStatus"
        token = command_context.set("broker-rejection")
        try:
            with self.assertRaises(OrderFailed):
                self.api.limit_order("BIYA", "2.28", "2", "Buy")
        finally:
            command_context.reset(token)
        self.broker.calls.clear()
        result = self.api.query()
        self.assertEqual(result["orders"][0]["status"], "REJECTED")
        self.assertEqual(self.detail_reads(), [])


if __name__ == "__main__":
    unittest.main()
