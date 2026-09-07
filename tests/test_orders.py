import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from gigamoney.automation import OrderResult
from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
from gigamoney.errors import UiError
from gigamoney.orders import OrderAutomation
from gigamoney.ui import parse_xml


TICKET_XML = """<hierarchy>
  <node resource-id="p:id/deal_quick_price">
    <node resource-id="p:id/et_input" bounds="[100,200][300,300]" />
  </node>
  <node resource-id="p:id/deal_quick_qty">
    <node resource-id="p:id/et_input" bounds="[100,400][300,500]" />
  </node>
  <node resource-id="p:id/btn_place_order" bounds="[600,1500][800,1600]" />
</hierarchy>"""


class OrderRetryTests(unittest.TestCase):
    @staticmethod
    def events(logs, event="finished"):
        prefix = f"Workflow {event} "
        return [json.loads(record.getMessage()[len(prefix):]) for record in logs.records
                if record.getMessage().startswith(prefix)]

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.settings = Settings({}, root / "config.json", root)
        sleep = patch("gigamoney.orders.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    @staticmethod
    def result(*, canceled=False):
        return OrderResult(
            "Canceled" if canceled else "Confirmed",
            "Canceled" if canceled else "Submitted",
            parse_xml('<node bounds="[400,700][600,800]" />'),
            "", "0", canceled, not canceled,
        )

    def automation(self):
        automation = OrderAutomation(self.settings, adb=Mock(), ocr=Mock())
        automation.screenshot_ocr = Mock(return_value=(
            b"", SimpleNamespace(text="Watchlist Market Feed Portfolio"),
        ))
        automation.open_symbol = Mock()
        automation.adb.dump_xml.return_value = parse_xml(TICKET_XML)
        automation.open_ticket = Mock(return_value=automation.ticket_nodes(parse_xml(TICKET_XML)))
        automation.wait_order_result = Mock(return_value=self.result())
        automation.handle_trade_password = Mock(return_value=False)
        automation.order_result = Mock(return_value=self.result(canceled=True))
        return automation

    @staticmethod
    def run_order(automation, kind, **kwargs):
        if kind == "market":
            return automation.market_order("NVDA", "2", "Buy", **kwargs)
        return automation.limit_order("NVDA", "100", "2", "Buy", kill=kind == "fok", **kwargs)

    def test_first_attempt_never_classifies_ads_including_fok_cancellation(self):
        for kind in ("market", "limit", "fok"):
            with (self.subTest(kind=kind), patch("gigamoney.automation.classify_png") as classify,
                  self.assertLogs("gigamoney", level="INFO") as logs):
                automation = self.automation()
                result = self.run_order(automation, kind)
                self.assertTrue(result["submitted"])
                classify.assert_not_called()
                automation.adb.screenshot.assert_not_called()
                automation.adb.restart.assert_not_called()
                if kind == "fok":
                    self.assertEqual(result["result"], "Canceled")
                    automation.handle_trade_password.assert_called_once_with()
                    self.assertIn(call(792, 2282), automation.adb.tap.call_args_list)
            finished = self.events(logs)
            self.assertEqual(len(finished), 1)
            self.assertEqual(finished[0]["workflow"], f"{kind}_order")
            self.assertEqual(finished[0]["outcome"], "success")
            self.assertEqual(finished[0]["attempts"], 1)
            self.assertTrue(finished[0]["submission_confirmed"])

    def test_unconfirmed_order_checks_ads_only_on_second_attempt(self):
        for kind, expected_checks in (("market", 2), ("limit", 2), ("fok", 4)):
            with self.subTest(kind=kind):
                automation = self.automation()
                automation.wait_order_result.side_effect = [None, self.result()]
                checked_attempts = []

                def classify(png):
                    checked_attempts.append(automation.adb.restart.call_count)
                    return "None"

                with patch("gigamoney.automation.classify_png", side_effect=classify):
                    with self.assertLogs("gigamoney", level="WARNING"):
                        result = self.run_order(automation, kind)
                self.assertTrue(result["submitted"])
                self.assertEqual(checked_attempts, [1] * expected_checks)
                automation.adb.restart.assert_called_once_with()
                self.assertEqual(automation.wait_order_result.call_count, 2)

    def test_ui_setup_failure_reaches_retry_before_any_submission(self):
        for kind in ("market", "limit", "fok"):
            with self.subTest(kind=kind):
                automation = self.automation()
                ticket = automation.open_ticket.return_value
                automation.open_ticket.side_effect = [UiError("Ticket blocked"), ticket]
                checked_attempts = []

                def classify(png):
                    checked_attempts.append(automation.adb.restart.call_count)
                    return "None"

                with patch("gigamoney.automation.classify_png", side_effect=classify):
                    with self.assertLogs("gigamoney", level="WARNING"):
                        result = self.run_order(automation, kind)
                self.assertTrue(result["submitted"])
                self.assertTrue(checked_attempts)
                self.assertTrue(all(attempt == 1 for attempt in checked_attempts))
                automation.adb.restart.assert_called_once_with()
                automation.wait_order_result.assert_called_once_with()
                self.assertEqual(automation.adb.tap.call_args_list.count(call(734, 1540)), 1)

    def test_retry_dismisses_ad_and_restores_ticket_before_submitting(self):
        for kind in ("market", "limit"):
            with self.subTest(kind=kind):
                automation = self.automation()
                automation.wait_order_result.side_effect = [None, self.result()]
                # No ad at retry home; a stable ad appears before retry Submit.
                with patch("gigamoney.automation.classify_png", side_effect=["None", "Ad", "Ad", "None"]):
                    with self.assertLogs("gigamoney", level="WARNING"):
                        result = self.run_order(automation, kind)
                self.assertTrue(result["submitted"])
                self.assertEqual(automation.adb.keyevent.call_args_list, [
                    call("KEYCODE_BACK"), call("KEYCODE_BACK"),
                ])
                self.assertEqual(automation.adb.tap.call_args_list, [call(734, 1540), call(700, 1550)])
                self.assertEqual(automation.adb.set_text_at.call_args_list[-1], call(200, 450, "2"))
                if kind == "limit":
                    self.assertEqual(automation.adb.set_text_at.call_args_list[-2], call(200, 250, "100"))

    def test_second_setup_failure_exits_without_submitting(self):
        automation = self.automation()
        automation.open_ticket.side_effect = UiError("Ticket blocked")
        with patch("gigamoney.automation.classify_png", return_value="None"):
            with self.assertLogs("gigamoney", level="ERROR"):
                with self.assertRaisesRegex(UiError, "Ticket blocked"):
                    self.run_order(automation, "market")
        automation.adb.restart.assert_called_once_with()
        automation.wait_order_result.assert_not_called()
        automation.adb.tap.assert_not_called()

    def test_fok_cancel_failure_does_not_resubmit_confirmed_order(self):
        automation = self.automation()
        automation.order_result.return_value = self.result()
        with (patch("gigamoney.automation.classify_png") as classify,
              self.assertLogs("gigamoney", level="ERROR") as logs):
            with self.assertRaisesRegex(UiError, "FOK cancel was not confirmed"):
                self.run_order(automation, "fok")
        classify.assert_not_called()
        automation.adb.restart.assert_not_called()
        automation.wait_order_result.assert_called_once_with()
        self.assertEqual(automation.adb.tap.call_args_list.count(call(734, 1540)), 1)
        finished = self.events(logs)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["stage"], "fok_cancel")
        self.assertEqual(finished[0]["outcome"], "failure")
        self.assertTrue(finished[0]["submission_confirmed"])

    def test_staging_does_not_check_ads_or_retry(self):
        for kind in ("market", "limit", "fok"):
            for option in ("dry_run", "no_submit"):
                with self.subTest(kind=kind, option=option):
                    automation = self.automation()
                    with (patch("gigamoney.automation.classify_png") as classify,
                          self.assertLogs("gigamoney", level="INFO") as logs):
                        result = self.run_order(automation, kind, **{option: True})
                    self.assertFalse(result["submitted"])
                    classify.assert_not_called()
                    automation.adb.restart.assert_not_called()
                    automation.wait_order_result.assert_not_called()
                    self.assertEqual(self.events(logs)[0]["outcome"],
                                     "dry_run" if option == "dry_run" else "staged")

    def test_retry_logs_one_final_outcome_with_total_time_and_command_id(self):
        automation = self.automation()
        now = 0
        results = iter([None, self.result()])

        def order_result():
            nonlocal now
            now += 3 if automation.adb.restart.call_count == 0 else 5
            return next(results)

        def restart():
            nonlocal now
            now += 2

        automation.wait_order_result.side_effect = order_result
        automation.adb.restart.side_effect = restart
        token = command_context.set("order-123")
        try:
            with (patch("gigamoney.diagnostics.time.perf_counter", side_effect=lambda: now),
                  patch("gigamoney.automation.classify_png", return_value="None"),
                  self.assertLogs("gigamoney", level="INFO") as logs):
                self.run_order(automation, "market")
        finally:
            command_context.reset(token)
        finished = self.events(logs)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["command_id"], "order-123")
        self.assertEqual(finished[0]["attempts"], 2)
        self.assertEqual(finished[0]["elapsed_s"], 10)
        self.assertEqual(finished[0]["attempt_elapsed_s"], 5)
        retries = self.events(logs, "retry")
        self.assertEqual(len(retries), 1)
        self.assertEqual(retries[0]["stage"], "confirm_result")
        self.assertEqual(retries[0]["attempt_elapsed_s"], 3)
        self.assertEqual(retries[0]["next_attempt"], 2)

    def test_validation_failure_logs_without_exposing_configured_passwords(self):
        self.settings.data["gigamoney"] = {"tradePassword": "secret-trade"}
        self.settings.data["monitor"] = {"adminPassword": "secret-admin"}
        automation = self.automation()
        automation.adb.validate.side_effect = UiError("Failed: secret-trade secret-admin")
        with self.assertLogs("gigamoney", level="DEBUG") as logs:
            with self.assertRaises(UiError):
                self.run_order(automation, "market")
        self.assertNotIn("secret-trade", "\n".join(logs.output))
        self.assertNotIn("secret-admin", "\n".join(logs.output))
        finished = self.events(logs)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["stage"], "validate")
        self.assertEqual(finished[0]["attempts"], 0)
        self.assertEqual(finished[0]["outcome"], "failure")

    def test_shared_helpers_keep_ad_checks_by_default_for_other_actions(self):
        automation = self.automation()
        with patch("gigamoney.automation.classify_png", return_value="None") as classify:
            automation.ensure_home()
            automation.confirm_cancel()
        self.assertEqual(classify.call_count, 2)


if __name__ == "__main__":
    unittest.main()
