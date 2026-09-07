import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from gigamoney.cancel_orders import CancelAutomation
from gigamoney.config import Settings
from gigamoney.errors import UiError
from gigamoney.holdings import HoldingsAutomation
from gigamoney.ui import parse_xml


class WorkflowLoggingTests(unittest.TestCase):
    @staticmethod
    def events(logs, event="finished"):
        prefix = f"Workflow {event} "
        return [json.loads(record.getMessage()[len(prefix):]) for record in logs.records
                if record.getMessage().startswith(prefix)]

    def settings(self):
        return Settings({}, Path("/unused/config.json"), Path("/unused"))

    def test_holdings_retry_has_one_final_outcome_and_identifies_failed_stage(self):
        for final_failure in (False, True):
            with self.subTest(final_failure=final_failure):
                automation = HoldingsAutomation(self.settings(), adb=Mock(), ocr=Mock())
                automation.ensure_home = Mock()
                automation._open_portfolio = Mock(return_value=(945, 462))
                automation._open_cash = Mock(side_effect=[
                    UiError("Cash page did not open"),
                    UiError("Cash page still unavailable") if final_failure else parse_xml("<hierarchy/>"),
                ])
                automation._collect_positions = Mock(return_value=[])
                with (patch("gigamoney.holdings.time.sleep"),
                      self.assertLogs("gigamoney", level="INFO") as logs):
                    if final_failure:
                        with self.assertRaises(UiError):
                            automation.query()
                    else:
                        self.assertEqual(automation.query()["positions"], [])
                automation.adb.restart.assert_called_once_with()
                finished = self.events(logs)
                self.assertEqual(len(finished), 1)
                self.assertEqual(finished[0]["workflow"], "holdings")
                self.assertEqual(finished[0]["attempts"], 2)
                self.assertEqual(finished[0]["outcome"], "failure" if final_failure else "success")
                self.assertEqual(self.events(logs, "retry")[0]["stage"], "read_cash")
                self.assertEqual(finished[0]["stage"], "read_cash" if final_failure else "read_positions")

    def test_cancel_failure_reports_partial_progress_without_claiming_success(self):
        automation = CancelAutomation(self.settings(), adb=Mock(), ocr=Mock())
        orders = parse_xml('''<hierarchy>
          <node resource-id="p:id/cl_order" bounds="[0,600][1080,900]">
            <node resource-id="p:id/wealth_item_tv_code" text="NVDA"/>
            <node resource-id="p:id/wealth_item_order_normal_tv_status" text="Submitted"/>
            <node resource-id="p:id/wealth_item_order_normal_tv_action" text="Buy"/>
          </node>
        </hierarchy>''')
        automation.ensure_home = Mock()
        automation._open_in_progress = Mock(return_value=orders)
        automation.adb.dump_xml.return_value = orders
        automation.dismiss_ad_if_present = Mock(return_value=0)
        automation.confirm_cancel = Mock()
        automation._orders_after_cancel = Mock(side_effect=UiError("Orders page did not return"))
        with (patch("gigamoney.cancel_orders.time.sleep"),
              self.assertLogs("gigamoney", level="INFO") as logs):
            with self.assertRaises(UiError):
                automation.cancel_all()
        finished = self.events(logs)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["workflow"], "cancel_all")
        self.assertEqual(finished[0]["outcome"], "failure")
        self.assertEqual(finished[0]["stage"], "verify_orders")
        self.assertEqual(finished[0]["cancel_requests"], 1)
        self.assertEqual(finished[0]["symbol"], "NVDA")

    def test_empty_cancel_workflow_completes_with_zero_requests(self):
        automation = CancelAutomation(self.settings(), adb=Mock(), ocr=Mock())
        automation.ensure_home = Mock()
        automation._open_in_progress = Mock(return_value=parse_xml("<hierarchy/>"))
        with (patch("gigamoney.cancel_orders.time.sleep"),
              self.assertLogs("gigamoney", level="INFO") as logs):
            result = automation.cancel_all()
        self.assertEqual(result["canceledCount"], 0)
        finished = self.events(logs)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["outcome"], "success")
        self.assertEqual(finished[0]["cancel_requests"], 0)
        self.assertEqual(finished[0]["remainingDetected"], 0)


if __name__ == "__main__":
    unittest.main()
