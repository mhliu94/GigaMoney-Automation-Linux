import json
from pathlib import Path
import sys
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gigamoney.api import AuthenticationRequired
from gigamoney.config import Settings
from gigamoney.runner import TradingRunner, runner_status
from gigamoney.monitor import GigaMoneyController
from tests.test_runner import FakeProducer


class AuthenticationIntakeTests(unittest.TestCase):
    def test_verification_stops_intake_without_committing_unsubmitted_order(self):
        for failure_stage in ("holdings", "market"):
            with self.subTest(stage=failure_stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                settings = Settings({"gigamoney": {"backend": "api", "accountId": "account-a"},
                    "kafka": {"bootstrapServers": "offline", "commandTopic": "commands", "accountDetailsTopic": "details",
                              "consumerGroupId": "account-a"}}, root / "config.json", root)
                runner = TradingRunner(settings)
                message = Mock()
                message.error.return_value = None
                message.value.return_value = json.dumps({"type": "MARKET_ORDER", "account_id": "account-a",
                    "command_id": "test", "ts": datetime.now(timezone.utc).isoformat(), "symbol": "BIYA", "side": "Buy", "qty_shares": 1}).encode()
                consumer = Mock()
                consumer.poll.return_value = message
                kafka = SimpleNamespace(Consumer=Mock(return_value=consumer), Producer=Mock(return_value=FakeProducer()), KafkaException=RuntimeError)
                def operation(kind, arguments):
                    if kind == failure_stage:
                        raise AuthenticationRequired("Phone verification required in the app.")
                    return {"positions": []}
                with patch.dict(sys.modules, {"confluent_kafka": kafka}), patch("gigamoney.runner.signal.signal"), \
                     patch.object(runner, "_operation", side_effect=operation), self.assertLogs("gigamoney", level="ERROR"):
                    runner.run()
                consumer.commit.assert_not_called()
                consumer.close.assert_called_once()
                self.assertEqual(consumer.poll.call_count, 0 if failure_stage == "holdings" else 1)
                status = runner_status(settings)
                self.assertFalse(status["running"])
                self.assertEqual(status["state"], "authentication_required")
                self.assertTrue(GigaMoneyController(settings).script_status()["authentication_required"])


if __name__ == "__main__":
    unittest.main()
