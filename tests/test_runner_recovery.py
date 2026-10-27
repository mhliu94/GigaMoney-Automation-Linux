"""Run the real journal/recovery workflows with offline Kafka and broker doubles."""
import json
from pathlib import Path
import sys
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gigamoney.api import ApiError, ApiResult, AuthenticationRequired, TransportError
from gigamoney.api_trading import ApiTrading
from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
from gigamoney.runner import TradingRunner, runner_status
from tests.test_api_trading import Broker
from tests.test_runner import FakeProducer


class RuntimeRecoveryTests(unittest.TestCase):
    def run_session(self, broker, *, recovery_auth_failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings({
                "gigamoney": {"backend": "api", "accountId": "TEST"},
                "kafka": {"bootstrapServers": "offline", "commandTopic": "commands",
                          "accountDetailsTopic": "account-details", "consumerGroupId": "test",
                          "holdingsIntervalSeconds": 30},
            }, root / "config.json", root)
            now = 0

            def sleep(seconds):
                nonlocal now
                now += seconds

            api = ApiTrading(settings, client=broker, price_books=Mock(), sleeper=sleep)
            runner = TradingRunner(settings)
            runner.api_executor = api
            messages = []
            for key in ("first", "second"):
                message = Mock()
                message.error.return_value = None
                message.value.return_value = json.dumps({
                    "account_id": "TEST", "command_id": key, "type": "LIMIT_ORDER_FOK",
                    "symbol": "BIYA", "side": "Buy", "qty_shares": 2, "limit_price": "2.28",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }).encode()
                messages.append(message)
            polls = iter([(10, messages[0]), (80, None), (81, messages[1])])

            def poll(timeout):
                nonlocal now
                item = next(polls, None)
                if item is None:
                    runner.running = False
                    return None
                when, message = item
                now = max(now, when)
                return message

            consumer = Mock()
            consumer.poll.side_effect = poll
            producer = FakeProducer()
            snapshots = []
            phases_at_publication = []
            original_produce = producer.produce

            def produce(topic, **kwargs):
                if topic == "account-details":
                    snapshots.append(json.loads(kwargs["value"]))
                    first = api.journal.read("first")
                    phases_at_publication.append(first.get("phase") if first else None)
                original_produce(topic, **kwargs)

            producer.produce = produce
            original_recover = api.recover_pending
            recoveries = []

            def recover():
                recoveries.append(command_context.get())
                if recovery_auth_failure and len(recoveries) > 1:
                    raise AuthenticationRequired("Verification required")
                return original_recover()

            kafka = SimpleNamespace(Consumer=Mock(return_value=consumer),
                                    Producer=Mock(return_value=producer), KafkaException=RuntimeError)
            with (
                patch.dict(sys.modules, {"confluent_kafka": kafka}),
                patch("gigamoney.runner.signal.signal"),
                patch("gigamoney.runner.time.monotonic", side_effect=lambda: now),
                patch("gigamoney.runner.time.sleep", side_effect=sleep),
                patch.object(api, "recover_pending", side_effect=recover),
                self.assertLogs("gigamoney", level="INFO") as logs,
            ):
                runner.run()
            consumer.close.assert_called_once_with()
            records = {row["command_id"]: row for row in api.journal.records()}
            commands = [json.loads(row.getMessage().split("Command finished ", 1)[1])
                        for row in logs.records if row.getMessage().startswith("Command finished ")]
            return SimpleNamespace(records=records, commands=commands, snapshots=snapshots,
                                   phases=phases_at_publication, consumer=consumer, messages=messages,
                                   logs=logs.output, recoveries=recoveries, status=runner_status(settings))

    def test_periodic_recovery_clears_failed_confirmation_before_next_order(self):
        broker = Broker()
        broker.initial_status = "FilledStatus"
        # Exhaust both the initial confirmation and immediate post-order recovery.
        # Only the later periodic refresh can reconcile this order successfully.
        broker.fail_reads["order_detail"] = [
            ApiError(ApiResult("/v3/orders/detail", 400, 602023, 0, "test", {}))
            for _ in range(4)
        ]
        result = self.run_session(broker)
        self.assertEqual(result.phases[:3], [None, "accepted", "finished"])
        self.assertEqual([row["outcome"] for row in result.commands], ["failure", "success"])
        self.assertTrue(all(row["committed"] for row in result.commands))
        self.assertEqual(result.records["first"]["phase"], "finished")
        self.assertEqual(result.records["second"]["phase"], "finished")
        self.assertEqual([call[1] for call in broker.writes()], ["submit", "submit"])
        request_ids = [call[2]["client_request_id"] for call in broker.writes()]
        self.assertEqual(len(set(request_ids)), 2)
        self.assertEqual(result.consumer.commit.call_count, 2)
        self.assertTrue(any("Order recovery deferred" in line for line in result.logs))
        # Recovery restores the enclosing command context before holdings publish.
        self.assertEqual(result.recoveries[1:], ["first", None, "second"])

    def test_unknown_submission_remains_blocked_without_duplicate_send(self):
        broker = Broker()
        broker.submit_error = TransportError("Lost submit response")
        result = self.run_session(broker)
        self.assertEqual(len(broker.writes()), 1)
        self.assertEqual(result.records["first"]["phase"], "ambiguous")
        self.assertNotIn("second", result.records)
        self.assertEqual([row["outcome"] for row in result.commands], ["failure", "failure"])
        self.assertGreaterEqual(len(result.snapshots), 3)
        self.assertEqual(result.phases[1:], ["ambiguous"] * (len(result.phases) - 1))

    def test_runtime_recovery_authentication_failure_stops_intake(self):
        broker = Broker()
        broker.initial_status = "FilledStatus"
        result = self.run_session(broker, recovery_auth_failure=True)
        self.assertEqual(len(broker.writes()), 1)
        result.consumer.commit.assert_called_once_with(message=result.messages[0], asynchronous=False)
        self.assertEqual(result.consumer.poll.call_count, 1)
        self.assertEqual(result.status["state"], "authentication_required")
        self.assertFalse(result.status["running"])


if __name__ == "__main__":
    unittest.main()
