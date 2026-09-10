"""Exercise the real workflows against a stateful, offline broker double."""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gigamoney.api import ApiError, ApiResult, SessionError, TransportError, save_session
from gigamoney.api_trading import ApiTrading, OrderUncertain, TradingApiClient
from gigamoney.config import Settings
from gigamoney.diagnostics import command_context
from gigamoney.runner import TradingRunner, holdings_snapshot
from gigamoney.price_books import BookPrice
from tests.test_api import Opener, Response, state

ROUTE = {"aaid": "8", "org_id": "1", "account_channel": "9"}


def broker_order(identifier="9007199254740993123", **extra):
    return {**ROUTE, "order_id_str": identifier, "id": identifier,
            "counter_id": "ST/US/BIYA", "action": 1, "order_type": "LO", "price": "2.28",
            "quantity": "2", "executed_qty": "0", "executed_price": "0", "status": "NewStatus", **extra}


class Broker:
    def __init__(self):
        self.calls = []
        self.orders = {}
        self.fail_reads = {}
        self.submit_error = None
        self.before_error = None
        self.echo_request_id = False
        self.initial_status = "NewStatus"
        self.cancel_status = "CanceledStatus"
        self.withdraw_error = None
        self.fail_detail = False
        self.info = {"counter_id": "ST/US/BIYA", "buy_lot_size": "1", "sell_lot_size": "1",
                     "channel_infos": {**ROUTE, "support_trade": True, "restricted": 0,
                         "settlement_currency": ["USD"], "comm_card_id": "current-card",
                         "deductions_card_id": "", "platform_deductions_card_id": "",
                         "order_type_config": {"MO": {"support_sessions": [1], "support_time_in_forces": [0]},
                                               "LO": {"support_sessions": [1, 2], "support_time_in_forces": [0]}}}}
        self.quote = {"counter_id": "ST/US/BIYA", "last_done": "2.26", "market_price": "2.29",
                      "timestamp": "10", "market_timestamp": "20"}
        self.positions = {"account_channel": "9", "account": {"account_no": "synthetic-account"},
                          "stockhold": {"holdings": [{"counter_id": "ST/US/BIYA", "quantity": "10",
                              "available_quantity": "8", "average_cost": "2", "diluted_avg_cost": "2", "last_done": "2.26",
                              "stock_invest_of_today": "20", "sum_invest": "20", "today_pl": "0",
                              "achieves_pl": "2.60", "achieves_pl_percent": "13"}]}}
        self.cash = {"account_channel": "9", "account_no": "synthetic-account",
                     "cash_balance": [{"currency": "USD", "balance": "123.45"}, {"currency": "HKD", "balance": "0"}]}
        self.pages = None
        self.market_status = 202
        self.market_timestamp = None

    def account_context(self):
        return dict(ROUTE)

    def read(self, endpoint, params=None):
        self.calls.append(("GET", endpoint, params))
        errors = self.fail_reads.get(endpoint, [])
        if errors:
            raise errors.pop(0)
        if endpoint == "market_status":
            data = {"market_time": [{"market": "US", "trade_status": self.market_status,
                    "delay_trade_status": 201, "timestamp": str(self.market_timestamp or int(time.time()))}]}
        elif endpoint == "order_detail":
            if self.fail_detail:
                raise TransportError("offline timeout")
            data = {"order": self.orders[params["order_id_str"]]}
        elif endpoint == "orders":
            data = (self.pages[params["page"] - 1] if self.pages else
                    {"orders": list(self.orders.values()), "count": str(len(self.orders)), "has_more": False})
        else:
            data = {"positions": self.positions, "cash": self.cash, "order_info": self.info, "quote": self.quote}[endpoint]
        return ApiResult(endpoint, 200, 0, 1, "test-read", {"code": 0, "data": copy.deepcopy(data)})

    def write(self, operation, body, *, before_send):
        if self.before_error:
            raise self.before_error
        before_send()
        self.calls.append(("POST", operation, copy.deepcopy(body)))
        if operation == "submit":
            identifier = str(9007199254740993123 + len(self.orders))
            row = broker_order(identifier, **{k: v for k, v in body.items() if k != "client_request_id"},
                               status=self.initial_status)
            if self.echo_request_id:
                row["client_request_id"] = body["client_request_id"]
            self.orders[identifier] = row
            if self.submit_error:
                raise self.submit_error
            return {"order_id_str": identifier}
        self.orders[body["order_id_str"]]["status"] = self.cancel_status
        if self.withdraw_error:
            raise self.withdraw_error
        return {"ok": True}

    def writes(self, operation=None):
        return [call for call in self.calls if call[0] == "POST" and (operation is None or call[1] == operation)]


class TradingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.settings = Settings({"gigamoney": {"accountId": "TEST", "backend": "api"},
                                  "api": {"allowedSymbols": ["BIYA"], "maxOrderQuantity": 200, "maxOrders": 100}},
                                 root / "config.json", root)
        self.broker = Broker()
        self.price_books = Mock()
        self.price_books.price.return_value = BookPrice("2.29", time.time_ns(), "200", 1)
        self.sleeps = []
        self.api = ApiTrading(self.settings, client=self.broker, price_books=self.price_books, sleeper=self.sleeps.append)
        token = command_context.set("test-command")
        self.addCleanup(command_context.reset, token)

    def test_staging_buy_sell_market_limit_and_fok_never_writes(self):
        for side in ("Buy", "Sell"):
            self.assertFalse(self.api.market_order("BIYA", "2", side, no_submit=True)["submitted"])
            for kill in (False, True):
                self.assertFalse(self.api.limit_order("BIYA", "2.28", "2", side, kill=kill, dry_run=True)["submitted"])
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(list(self.api.journal.records()), [])

    def test_market_uses_current_quote_cards_and_exact_broker_id(self):
        result = self.api.market_order("biya", "2", "BUY")
        body = self.broker.writes("submit")[0][2]
        self.assertEqual(body["order_type"], "MO")
        self.assertEqual(body["trigger_price"], "2.29")
        self.assertEqual(body["card_ids"], ["current-card"])
        self.assertEqual(body["time_in_force"], "0")
        self.assertNotIn("force_only_rth", body)
        self.assertEqual(len(body["client_request_id"]), 13)
        self.assertEqual(result["orderId"], "9007199254740993123")
        self.assertEqual(self.api.journal.read("test-command")["phase"], "finished")

    def test_regular_market_ignores_delayed_broker_price_and_session(self):
        self.broker.quote.update(market_price="9999", timestamp="1", market_timestamp="2", latency=True)
        result = self.api.market_order("BIYA", "2", "BUY", no_submit=True)
        self.assertEqual(result["ticket"]["trigger_price"], "2.29")
        self.assertEqual(result["ticket"]["order_type"], "MO")
        self.assertEqual(result["pricing"]["tradingSession"], "regular")
        self.price_books.price.assert_called_once_with("BIYA", "Buy", "2")
        self.assertNotIn("quote", [call[1] for call in self.broker.calls])

    def test_limit_sell_preserves_ticket_settings_and_prechecks_shares(self):
        result = self.api.limit_order("BIYA", "2.28", "2", "Sell")
        body = self.broker.writes("submit")[0][2]
        self.assertEqual((body["price"], body["action"], body["force_only_rth"]), ("2.28", "2", "2"))
        self.assertTrue(result["submitted"])
        reads = [x[1] for x in self.broker.calls if x[0] == "GET"]
        self.assertIn("positions", reads)
        self.assertIn("orders", reads)

    def test_extended_market_uses_kafka_limit_and_keeps_original_intent(self):
        for status, session in ((201, "premarket"), (203, "afterhours")):
            for side, price in (("Buy", "2.37"), ("Sell", "2.36")):
                with self.subTest(session=session, side=side):
                    key = f"{status}-{side}"
                    command_context.set(key)
                    self.broker.market_status = status
                    self.broker.initial_status = "NotReported"
                    self.price_books.price.return_value = BookPrice("2.3625", time.time_ns(), "100", 2, "price-books", 0, 123)
                    with self.assertLogs("gigamoney", level="INFO") as logs:
                        result = self.api.market_order("BIYA", "2", side)
                    body = self.broker.writes("submit")[-1][2]
                    self.assertEqual((body["order_type"], body["price"], body["force_only_rth"]), ("LO", price, "2"))
                    self.assertNotIn("trigger_price", body)
                    self.assertEqual(result["filledQty"], "0")
                    self.assertEqual(result["pricing"]["requestedOrderType"], "MO")
                    self.assertEqual(result["pricing"]["tradingSession"], session)
                    self.assertEqual(result["pricing"]["priceBookOffset"], 123)
                    self.assertIsNone(self.api.journal.read(key)["intent"]["price"])
                    self.assertTrue(any('"effectiveOrderType":"LO"' in line for line in logs.output))
                    self.broker.market_status = 202
                    restarted = ApiTrading(self.settings, client=self.broker, price_books=Mock())
                    replay = restarted.market_order("BIYA", "2", side)
                    self.assertTrue(replay["deduplicated"])
                    self.assertEqual(replay["pricing"], result["pricing"])
                    restarted.price_books.price.assert_not_called()
        self.assertEqual(len(self.broker.writes("submit")), 4)

    def test_extended_market_recovery_keeps_original_price_after_session_change(self):
        self.broker.market_status = 201
        self.broker.fail_detail = True
        with self.assertRaises(OrderUncertain):
            self.api.market_order("BIYA", "2", "Buy")
        self.broker.fail_detail = False
        self.broker.market_status = 202
        self.price_books.price.side_effect = AssertionError("A sent order must not be repriced")
        self.assertEqual(self.api.recover_pending()["recovered"], 1)
        self.assertEqual(len(self.broker.writes("submit")), 1)
        self.assertEqual(self.api.journal.read("test-command")["result"]["pricing"]["effectiveOrderType"], "LO")

    def test_closed_or_stale_market_status_never_queues_market_order(self):
        for status, stamp in ((204, None), (206, None), (207, None), (999, None),
                              (201, time.time() - 60), (201, time.time() + 60)):
            with self.subTest(status=status, timestamp=stamp):
                self.broker.market_status, self.broker.market_timestamp = status, stamp
                with self.assertRaises(ValueError):
                    self.api.market_order("BIYA", "2", "Buy")
        self.assertEqual(self.broker.writes(), [])
        self.price_books.price.assert_not_called()

    def test_extended_session_capability_and_switch_are_checked_before_pricing(self):
        self.broker.market_status = 201
        self.settings.data["api"]["extendedHoursMarketOrders"] = False
        with self.assertRaisesRegex(ValueError, "disabled"):
            self.api.market_order("BIYA", "2", "Buy")
        self.settings.data["api"]["extendedHoursMarketOrders"] = True
        self.broker.info["channel_infos"]["order_type_config"]["LO"]["support_sessions"] = [1]
        with self.assertRaisesRegex(ValueError, "unavailable"):
            self.api.market_order("BIYA", "2", "Buy")
        self.assertEqual(self.broker.writes(), [])
        self.price_books.price.assert_not_called()

    def test_missing_kafka_price_never_uses_broker_price_or_submits(self):
        self.broker.market_status = 201
        self.price_books.price.side_effect = ValueError("No current Kafka price book")
        with self.assertRaisesRegex(ValueError, "Kafka"):
            self.api.market_order("BIYA", "2", "Buy")
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(list(self.api.journal.records()), [])
        self.assertNotIn("quote", [call[1] for call in self.broker.calls])

    def test_explicit_limit_price_has_no_local_price_band_or_quote_dependency(self):
        self.price_books.price.side_effect = AssertionError("Explicit limits do not use a quote")
        for side, price in (("Buy", "9999999.99"), ("Sell", "0.01")):
            command_context.set("unbounded-limit-" + side)
            result = self.api.limit_order("BIYA", price, "1", side)
            self.assertTrue(result["submitted"])
            self.assertEqual(self.broker.writes("submit")[-1][2]["price"], price)
        self.assertNotIn("market_status", [call[1] for call in self.broker.calls])

    def test_limit_and_fok_round_outgoing_price_and_reconcile_effective_ticket(self):
        for side, price in (("Buy", "2.37"), ("Sell", "2.36")):
            for kill in (False, True):
                with self.subTest(side=side, kill=kill):
                    key = f"round-{side}-{kill}"
                    command_context.set(key)
                    with self.assertLogs("gigamoney", level="INFO") as logs:
                        result = self.api.limit_order("BIYA", "2.3625", "2", side, kill=kill)
                    self.assertTrue(result["submitted"])
                    body = self.broker.writes("submit")[-1][2]
                    self.assertEqual((body["price"], body["quantity"]), (price, "2"))
                    record = self.api.journal.read(key)
                    self.assertEqual(record["intent"]["price"], "2.3625")
                    self.assertEqual(record["request"]["price"], price)
                    self.assertEqual(record["phase"], "finished")
                    self.assertTrue(any(f"command_id={key}" in line and
                                        f"requested=2.3625 effective={price}" in line for line in logs.output))
                    restarted = ApiTrading(self.settings, client=self.broker)
                    duplicate = restarted.limit_order("BIYA", "2.3625", "2", side, kill=kill)
                    self.assertTrue(duplicate["deduplicated"])
        self.assertEqual(len(self.broker.writes("submit")), 4)

    def test_market_trigger_rounds_current_quote_for_both_sides(self):
        for side, price in (("Buy", "2.37"), ("Sell", "2.36")):
            with self.subTest(side=side):
                command_context.set("market-" + side)
                self.price_books.price.return_value = BookPrice("2.3625", time.time_ns(), "200", 1)
                result = self.api.market_order("BIYA", "2", side)
                self.assertTrue(result["submitted"])
                body = self.broker.writes("submit")[-1][2]
                self.assertEqual(body["trigger_price"], price)
                self.assertEqual(body["order_type"], "MO")
                self.assertNotIn("price", body)

    def test_subcent_sell_is_rejected_before_submit(self):
        self.price_books.price.return_value = BookPrice("0.0099", time.time_ns(), "200", 1)
        for price in (None, "0.0099"):
            with self.subTest(price=price), self.assertRaisesRegex(ValueError, "rounds down to zero"):
                if price is None:
                    self.api.market_order("BIYA", "2", "Sell")
                else:
                    self.api.limit_order("BIYA", price, "2", "Sell")
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(list(self.api.journal.records()), [])

    def test_pre_rounding_journal_entries_are_not_resubmitted(self):
        for rejected in (False, True):
            with self.subTest(rejected=rejected):
                key = f"old-price-{rejected}"
                command_context.set(key)
                self.broker.submit_error = (ApiError(ApiResult("submit", 400, 602035, 1, "rejected",
                    {"code": 602035, "message": "Invalid price increment"})) if rejected else None)
                # Simulate the previous release, which sent the original price.
                with patch("gigamoney.orders.OrderAutomation.order_price", return_value="2.3625"):
                    if rejected:
                        with self.assertRaises(ApiError):
                            self.api.limit_order("BIYA", "2.3625", "2", "Buy")
                    else:
                        self.api.limit_order("BIYA", "2.3625", "2", "Buy")
                restarted = ApiTrading(self.settings, client=self.broker)
                if rejected:
                    with self.assertRaisesRegex(ValueError, "previously rejected"):
                        restarted.limit_order("BIYA", "2.3625", "2", "Buy")
                else:
                    self.assertTrue(restarted.limit_order("BIYA", "2.3625", "2", "Buy")["deduplicated"])
        self.assertEqual(len(self.broker.writes("submit")), 2)

    def test_short_selling_and_reserved_shares_are_rejected(self):
        for quantity, available, pending in ((11, 20, 0), (9, 8, 0), (3, 10, 8)):
            with self.subTest(quantity=quantity):
                self.broker.positions["stockhold"]["holdings"][0]["available_quantity"] = str(available)
                self.broker.orders = {"11": broker_order("11", action=2, quantity=str(pending))} if pending else {}
                with self.assertRaisesRegex(ValueError, "short selling"):
                    self.api.market_order("BIYA", str(quantity), "Sell")
        self.broker.positions["stockhold"]["holdings"] = []
        with self.assertRaisesRegex(ValueError, "long holding"):
            self.api.market_order("BIYA", "1", "Sell")
        self.assertEqual(self.broker.writes(), [])

    def test_limits_unknown_security_and_route_mismatch_fail_before_write(self):
        for symbol, quantity in (("NVDA", "1"), ("BIYA", "201"), ("BIYA", "0"), ("BIYA", "-1")):
            with self.assertRaises(ValueError):
                self.api.market_order(symbol, quantity, "Buy")
        self.broker.info["channel_infos"]["aaid"] = "another-account"
        with self.assertRaisesRegex(ValueError, "routing"):
            self.api.market_order("BIYA", "1", "Buy")
        self.assertEqual(self.broker.writes(), [])

    def test_duplicate_survives_client_restart_and_conflicting_id_is_rejected(self):
        first = self.api.market_order("BIYA", "2", "Buy")
        restarted = ApiTrading(self.settings, client=self.broker)
        second = restarted.market_order("BIYA", "2", "Buy")
        self.assertTrue(second["deduplicated"])
        self.assertEqual(first["orderId"], second["orderId"])
        with self.assertRaisesRegex(ValueError, "different order"):
            restarted.market_order("BIYA", "3", "Buy")
        self.assertEqual(len(self.broker.writes()), 1)

    def test_total_limit_is_persistent_across_commands(self):
        self.settings.data["api"]["maxOrders"] = 1
        self.api.market_order("BIYA", "2", "Buy")
        command_context.set("second-command")
        with self.assertRaisesRegex(ValueError, "maxOrders"):
            self.api.market_order("BIYA", "2", "Buy")
        self.assertEqual(len(self.broker.writes()), 1)

    def test_lost_submit_reply_reconciles_by_exact_client_id_without_resending(self):
        self.broker.echo_request_id = True
        self.broker.submit_error = TransportError("lost reply")
        result = self.api.market_order("BIYA", "2", "Buy")
        self.assertTrue(result["ok"])
        self.assertEqual(len(self.broker.writes()), 1)

    def test_lost_submit_without_correlation_blocks_replay_and_new_orders(self):
        self.broker.submit_error = TransportError("lost reply")
        for _ in range(2):
            with self.assertRaisesRegex(OrderUncertain, "unknown"):
                self.api.market_order("BIYA", "2", "Buy")
        command_context.set("another-command")
        with self.assertRaisesRegex(OrderUncertain, "unresolved"):
            self.api.market_order("BIYA", "2", "Buy")
        with self.assertRaises(OrderUncertain):
            self.api.recover_pending()
        self.assertEqual(len(self.broker.writes()), 1)

    def test_pre_send_auth_failure_is_retryable_without_a_send_marker(self):
        self.broker.before_error = SessionError("Sign-in required")
        with self.assertRaises(SessionError):
            self.api.market_order("BIYA", "2", "Buy")
        record = self.api.journal.read("test-command")
        self.assertEqual(record["phase"], "prepared")
        self.assertNotIn("sent_at", record)
        self.broker.before_error = None
        self.api.market_order("BIYA", "2", "Buy")
        self.assertEqual(len(self.broker.writes()), 1)

    def test_acknowledged_order_confirmation_timeout_is_recoverable(self):
        self.broker.fail_detail = True
        with self.assertRaises(OrderUncertain) as error:
            self.api.market_order("BIYA", "2", "Buy")
        self.assertTrue(error.exception.submitted)
        self.assertEqual(self.sleeps, [0.25])
        self.broker.fail_detail = False
        result = self.api.recover_pending()
        self.assertEqual(result["recovered"], 1)
        self.assertEqual(len(self.broker.writes()), 1)

    def test_fok_waits_then_confirms_cancellation_and_full_fill_skips_cancel(self):
        self.broker.cancel_status = "PartialWithdrawal"
        result = self.api.limit_order("BIYA", "2.28", "2", "Buy", kill=True)
        self.assertEqual(result["status"], "PartialWithdrawal")
        self.assertEqual(self.sleeps, [2])
        self.assertEqual([x[1] for x in self.broker.writes()], ["submit", "withdraw"])
        command_context.set("filled-command")
        self.broker.initial_status = "FilledStatus"
        result = self.api.limit_order("BIYA", "2.28", "2", "Buy", kill=True)
        self.assertEqual(result["status"], "FilledStatus")
        self.assertEqual(len(self.broker.writes("withdraw")), 1)

    def test_fok_pending_cancel_is_failure_and_redelivery_does_not_cancel_twice(self):
        self.broker.cancel_status = "PendingCancelStatus"
        for _ in range(2):
            with self.assertRaises(OrderUncertain) as error:
                self.api.limit_order("BIYA", "2.28", "2", "Buy", kill=True)
            self.assertTrue(error.exception.submitted)
        self.assertEqual(len(self.broker.writes("submit")), 1)
        self.assertEqual(len(self.broker.writes("withdraw")), 1)
        for order in self.broker.orders.values():
            order["status"] = "CanceledStatus"
        self.assertEqual(self.api.recover_pending()["recovered"], 1)

    def test_cancel_lost_reply_and_fill_race_are_reconciled(self):
        self.broker.orders = {"11": broker_order("11")}
        self.broker.cancel_status = "FilledStatus"
        self.broker.withdraw_error = TransportError("lost cancel reply")
        result = self.api.cancel_all()
        self.assertEqual(result["remainingDetected"], 0)
        self.assertEqual(result["canceledCount"], 0)
        self.assertEqual(len(self.broker.writes()), 1)

    def test_cancel_all_counts_confirmed_results_and_dry_run_never_writes(self):
        self.broker.orders = {"11": broker_order("11"), "12": broker_order("12", status="FilledStatus")}
        self.assertEqual(self.api.cancel_all(dry_run=True)["remainingDetected"], 1)
        self.assertEqual(self.broker.writes(), [])
        self.assertEqual(self.api.cancel_all()["canceledCount"], 1)
        self.assertEqual(len(self.broker.writes()), 1)

    def test_cancel_limit_and_unknown_order_status_do_not_claim_success(self):
        self.settings.data["automation"] = {"maxCancelCount": 1}
        self.broker.orders = {"11": broker_order("11"), "12": broker_order("12")}
        with self.assertRaisesRegex(OrderUncertain, "remaining"):
            self.api.cancel_all()
        self.broker.orders["12"]["status"] = "NewUnknownStatus"
        with self.assertRaisesRegex(ValueError, "unknown"):
            self.api.cancel_all()
        self.assertEqual(len(self.broker.writes()), 1)

    def test_orders_paginate_and_reject_repeats_or_incomplete_lists(self):
        self.broker.pages = [{"orders": [broker_order("11")], "has_more": True, "count": "2"},
                             {"orders": [broker_order("12")], "has_more": False, "count": "2"}]
        self.assertEqual(self.api.cancel_all(dry_run=True)["remainingDetected"], 2)
        self.broker.pages[1]["orders"] = [broker_order("11")]
        with self.assertRaisesRegex(ValueError, "repeated"):
            self.api.cancel_all(dry_run=True)
        self.broker.pages = [{"orders": [broker_order("11")], "has_more": False, "count": "2"}]
        with self.assertRaisesRegex(ValueError, "fewer"):
            self.api.cancel_all(dry_run=True)
        self.assertEqual(self.broker.writes(), [])

    def test_holdings_preserve_kafka_contract_and_refuse_incomplete_accounts(self):
        snapshot = holdings_snapshot(self.api.query(), self.settings, True)
        self.assertEqual(snapshot["cash"], 123.45)
        self.assertEqual(snapshot["positions"][0]["qty"], 10)
        self.assertEqual(snapshot["positions"][0]["avg_price"], 2)
        self.assertEqual(snapshot["positions"][0]["marketValue"], 22.6)
        self.broker.cash["account_no"] = "different-account"
        with self.assertRaisesRegex(ValueError, "different accounts"):
            self.api.query()
        self.broker.cash["account_no"] = "synthetic-account"
        self.broker.positions["stockhold"] = {}
        with self.assertRaisesRegex(ValueError, "missing"):
            self.api.query()

    def test_read_timeout_retries_once_but_auth_failure_is_not_looped(self):
        self.broker.fail_reads["order_info"] = [TransportError("read timeout")]
        self.api.market_order("BIYA", "2", "Buy", no_submit=True)
        self.assertEqual(self.sleeps, [0.25])
        self.broker.fail_reads["order_info"] = [SessionError("verification required")]
        with self.assertRaises(SessionError):
            self.api.market_order("BIYA", "2", "Buy", no_submit=True)
        self.assertEqual(self.sleeps, [0.25])

    def test_position_display_uses_diluted_cost_extended_price_and_truncation(self):
        row = self.broker.positions["stockhold"]["holdings"][0]
        row.update(quantity="2", average_cost="10", diluted_avg_cost="9.9", last_done="11.31",
                   market_price="11.302", stock_invest_of_today="23.20", sum_invest="23.20", today_pl="0")
        snapshot = holdings_snapshot(self.api.query(), self.settings, True)["positions"][0]
        self.assertEqual(snapshot, {"symbol": "BIYA", "qty": 2, "avg_price": 9.9,
                                   "marketPrice": 11.302, "marketValue": 22.6, "dailyPL": -0.59, "dailyPLPercent": -2.54})

    def test_preview_cannot_resume_cancellation_of_an_existing_fok(self):
        self.broker.cancel_status = "PendingCancelStatus"
        with self.assertRaises(OrderUncertain):
            self.api.limit_order("BIYA", "2.28", "2", "Buy", kill=True)
        writes = len(self.broker.writes())
        with self.assertRaises(OrderUncertain):
            self.api.limit_order("BIYA", "2.28", "2", "Buy", kill=True, no_submit=True)
        self.assertEqual(len(self.broker.writes()), writes)

    def test_journal_failure_after_acknowledgment_preserves_submitted_state(self):
        original = self.api.journal.save
        def save(key, record):
            if record.get("order_id"):
                raise OSError("simulated disk failure")
            original(key, record)
        with patch.object(self.api.journal, "save", side_effect=save), self.assertRaises(OrderUncertain) as error:
            self.api.market_order("BIYA", "2", "Buy")
        self.assertTrue(error.exception.submitted)
        self.assertEqual(self.api.journal.read("test-command")["phase"], "dispatched")
        self.assertEqual(len(self.broker.writes()), 1)

    def test_real_transport_persists_marker_before_one_signed_submit(self):
        path = self.settings.work_dir / "api" / "test-session.json"
        save_session(path, state())
        opener = Opener(Response({"code": 0, "data": {"order_id_str": "11"}}))
        client = TradingApiClient(path, opener=opener, clock=lambda: 1000)
        observed = []
        body = {"counter_id": "ST/US/BIYA", "quantity": "2", "order_type": "MO"}
        client.write("submit", body, before_send=lambda: observed.append(len(opener.requests)))
        self.assertEqual(observed, [0])
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(json.loads(opener.requests[0].data), body)
        opener = Opener(TransportError("lost reply"))
        client.opener = opener
        with self.assertRaises(TransportError):
            client.write("submit", body, before_send=lambda: None)
        self.assertEqual(len(opener.requests), 1)

    def test_kafka_redelivery_uses_api_once_and_publishes_after_each_command(self):
        self.settings.data["kafka"] = {"bootstrapServers": "offline", "commandTopic": "commands",
                                       "accountDetailsTopic": "account-details", "consumerGroupId": "test"}
        runner = TradingRunner(self.settings)
        runner.api_executor = self.api
        message = Mock()
        message.error.return_value = None
        message.value.return_value = json.dumps({"account_id": "TEST", "command_id": "kafka-duplicate",
            "ts": datetime.now(timezone.utc).isoformat(), "type": "MARKET_ORDER", "symbol": "BIYA",
            "side": "BUY", "qty_shares": 2}).encode()
        consumer, producer = Mock(), Mock()
        messages = iter([message, message])
        events = []
        def poll(timeout):
            item = next(messages, None)
            events.append("poll")
            if item is None:
                runner.running = False
            return item
        def publish(producer, topic, snapshot, **kwargs):
            self.assertEqual(topic, "account-details")
            self.assertEqual(snapshot["account_id"], "TEST")
            events.append("positions")
            return 0, 1
        consumer.poll.side_effect = poll
        consumer.commit.side_effect = lambda **kw: events.append("commit")
        kafka = SimpleNamespace(Consumer=Mock(return_value=consumer), Producer=Mock(return_value=producer), KafkaException=RuntimeError)
        with patch.dict(sys.modules, {"confluent_kafka": kafka}), patch("gigamoney.runner.signal.signal"), \
             patch.object(runner, "_publish_snapshot", side_effect=publish), patch("gigamoney.adb.Adb.__init__", side_effect=AssertionError("ADB must not be used")) as android:
            runner.run()
        self.assertEqual(len(self.broker.writes("submit")), 1)
        self.assertEqual(consumer.commit.call_count, 2)
        self.assertEqual(events, ["positions", "poll", "commit", "positions", "poll", "commit", "positions", "poll"])
        android.assert_not_called()


if __name__ == "__main__":
    unittest.main()
