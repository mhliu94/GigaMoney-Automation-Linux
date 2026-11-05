"""Definite refusals close control-server handoffs without replaying an order."""
import copy
import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from jsonschema import Draft202012Validator, FormatChecker

from gigamoney.api import ApiError, ApiResult, AuthenticationRequired, TransportError
from gigamoney.api_trading import ApiTrading
from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
from gigamoney.runner import TradingRunner
from tests.test_api_trading import Broker, broker_order
from tests.test_runner import FakeProducer


class CommandRejectionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.settings = Settings({
            "gigamoney": {"backend": "api", "accountId": "TEST", "accountNumId": 7},
            "kafka": {"bootstrapServers": "offline", "commandTopic": "commands",
                      "accountDetailsTopic": "account-details", "consumerGroupId": "test"},
        }, root / "config.json", root)
        self.broker = Broker()
        self.command = {
            "account_id": "TEST", "command_id": "oversized-sell", "type": "LIMIT_ORDER",
            "symbol": "BIYA", "side": "SELL", "qty_shares": 200000, "limit_price": 14,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        self.arguments = {"symbol": "BIYA", "side": "Sell", "quantity": "200000", "price": "14", "kill": False}

    def api(self):
        return ApiTrading(self.settings, client=self.broker, price_books=Mock(), sleeper=lambda _: None)

    def session(self, commands=(), *, fail_rejections=False, api=None):
        api = api or self.api()
        runner = TradingRunner(self.settings)
        runner.api_executor = api
        sent, persisted_at_commit = [], []
        messages = iter(commands)
        consumer = Mock()
        self.last_consumer = consumer

        def poll(timeout):
            command = next(messages, None)
            if command is None:
                runner.running = False
                return None
            message = Mock()
            message.error.return_value = None
            message.value.return_value = json.dumps(command).encode()
            return message

        def commit(**kwargs):
            command = json.loads(kwargs["message"].value())
            persisted_at_commit.append(copy.deepcopy(api.journal.read(command["command_id"])))

        producer = FakeProducer()
        original = producer.produce

        def produce(topic, **kwargs):
            snapshot = json.loads(kwargs["value"])
            if topic == "order-status":
                if fail_rejections and snapshot["status"] == "REJECTED":
                    raise RuntimeError("Kafka unavailable")
                sent.append(copy.deepcopy(snapshot))
            original(topic, **kwargs)

        producer.produce = produce
        consumer.poll.side_effect = poll
        consumer.commit.side_effect = commit
        kafka = SimpleNamespace(Consumer=Mock(return_value=consumer), Producer=Mock(return_value=producer),
                                KafkaException=RuntimeError)
        with (patch.dict(sys.modules, {"confluent_kafka": kafka}),
              patch("gigamoney.runner.signal.signal"), self.assertLogs("gigamoney", level="INFO") as logs):
            runner.run()
        return SimpleNamespace(sent=sent, committed=persisted_at_commit, consumer=consumer, logs=logs.output)

    def test_oversized_sell_reports_rejection_immediately_and_persists_before_commit(self):
        # A separate order entered in the app has no local journal or command ID.
        self.broker.orders["123"] = broker_order("123", action=2, quantity="1", status="NewStatus")
        result = self.session([self.command])
        self.assertEqual(self.broker.writes(), [])
        result.consumer.commit.assert_called_once()
        rejected = next(row for row in result.sent if row["status"] == "REJECTED")
        self.assertEqual((rejected["command_id"], rejected["quantity"], rejected["limit_price"]),
                         ("oversized-sell", 200000, 14))
        self.assertEqual((rejected["filled_quantity"], rejected["average_fill_price"]), (0, None))
        self.assertIn("Not submitted: Sell exceeds available long shares", rejected["message"])
        self.assertEqual(rejected["created_at"], self.command["ts"])
        self.assertTrue(rejected["order_id"].startswith("local-rejection:"))
        schema = json.loads((Path(__file__).parent / "fixtures/order-status.v1.schema.json").read_text())
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(rejected)
        self.assertEqual(result.committed[0]["phase"], "rejected")
        self.assertNotIn("sent_at", result.committed[0])
        self.assertNotIn("order_id", result.committed[0])
        self.assertEqual(result.committed[0]["rejection_snapshot"], {k: v for k, v in rejected.items() if k != "sequence"})
        manual = next(row for row in result.sent if row["order_id"] == "123")
        self.assertEqual(manual["status"], "NEW")
        self.assertNotIn("command_id", manual)

    def test_delivery_failure_retries_after_restart_and_replay_never_submits(self):
        failed = self.session([self.command], fail_rejections=True)
        self.assertEqual(failed.sent, [])
        self.assertEqual(failed.committed[0]["phase"], "rejected")
        self.assertTrue(any("rejection publication deferred" in line for line in failed.logs))
        original = failed.committed[0]["rejection_snapshot"]
        # Inventory becoming sufficient must not revive a rejected command.
        self.broker.positions["stockhold"]["holdings"][0].update(quantity="300000", available_quantity="300000")
        restarted = self.session([self.command])
        self.assertEqual(self.broker.writes(), [])
        self.assertTrue(restarted.sent)
        self.assertTrue(all(row == dict(original, sequence=0) for row in restarted.sent))
        self.assertEqual(restarted.committed[0]["rejection_snapshot"], original)

    def test_broker_submit_refusal_is_reported_without_resending(self):
        self.broker.submit_error = ApiError(ApiResult("submit", 400, 602035, 0, "test", {}))
        command = dict(self.command, side="BUY", qty_shares=2)
        result = self.session([command])
        self.assertEqual(len(self.broker.writes()), 1)
        rejected = next(row for row in result.sent if row["status"] == "REJECTED")
        self.assertIn("Broker rejected submission:", rejected["message"])
        self.assertEqual(rejected["filled_quantity"], 0)

    def test_uncertain_submit_is_never_falsely_reported_as_rejected(self):
        self.broker.submit_error = TransportError("Lost acknowledgment")
        result = self.session([dict(self.command, side="BUY", qty_shares=2)])
        self.assertEqual(len(self.broker.writes()), 1)
        self.assertFalse(any(row["status"] == "REJECTED" for row in result.sent))
        self.assertEqual(result.committed[0]["phase"], "ambiguous")
        self.assertNotIn("rejection_snapshot", result.committed[0])

    def test_authentication_challenge_does_not_commit_or_report_rejection(self):
        self.broker.before_error = AuthenticationRequired("Verification needed")
        result = self.session([dict(self.command, side="BUY", qty_shares=2)])
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(result.sent, [])
        result.consumer.commit.assert_not_called()

    def test_rejection_record_cannot_overwrite_acknowledged_or_ambiguous_order(self):
        for uncertain in (False, True):
            key = "existing-" + str(uncertain)
            api = self.api()
            record = {"account": "TEST", "command_id": key, "intent": self.arguments,
                      "fingerprint": hashlib.sha256(json.dumps(self.arguments, sort_keys=True).encode()).hexdigest(),
                      "sent_at": 1, "phase": "ambiguous" if uncertain else "accepted"}
            if not uncertain:
                record["order_id"] = "123"
            api.journal.save(key, record)
            self.assertIsNone(api.record_rejection(key, self.arguments, "refused"))
            self.assertNotIn("rejection_snapshot", api.journal.read(key))

    def test_rejection_persistence_failure_leaves_command_uncommitted(self):
        api = self.api()
        with patch.object(api.journal, "save", side_effect=OSError("disk unavailable")):
            with self.assertRaisesRegex(OSError, "disk unavailable"):
                self.session([self.command], api=api)
        self.assertEqual(self.broker.writes(), [])
        self.last_consumer.commit.assert_not_called()

    def test_retry_does_not_require_a_successful_holdings_read(self):
        failed = self.session([self.command], fail_rejections=True)
        original = failed.committed[0]["rejection_snapshot"]
        api = self.api()
        with patch.object(api, "query", side_effect=TransportError("Portfolio endpoint offline")):
            retried = self.session(api=api)
        self.assertEqual(retried.sent, [dict(original, sequence=0)])
        self.assertEqual(self.broker.writes(), [])

    def test_rejection_reason_redacts_credentials_and_conflicting_intent_is_refused(self):
        self.settings.data["gigamoney"]["tradePassword"] = "secret-password"
        api = self.api()
        snapshot = api.record_rejection("test", self.arguments, "refused secret-password")
        self.assertNotIn("secret-password", snapshot["message"])
        with self.assertRaisesRegex(ValueError, "different order"):
            api.record_rejection("test", dict(self.arguments, quantity="1"), "refused")
        token = command_context.set("another-command")
        try:
            self.assertTrue(api.market_order("BIYA", "1", "Buy")["submitted"])
        finally:
            command_context.reset(token)

    def test_conflicting_redelivery_preserves_evidence_without_stopping_intake(self):
        result = self.session([self.command, dict(self.command, qty_shares=1)])
        self.assertEqual(result.consumer.commit.call_count, 2)
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(len(result.sent), 1)
        self.assertEqual(result.committed[0]["rejection_snapshot"], result.committed[1]["rejection_snapshot"])
        self.assertTrue(any("could not be correlated" in line for line in result.logs))


if __name__ == "__main__":
    unittest.main()
