"""Direct order workflows. Financial requests are never retried blindly."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
import logging
from pathlib import Path
import time
import uuid

from .api import ApiClient, ApiError, SessionError, TransportError, save_session
from .diagnostics import command_context, logged_workflow, workflow_attempt, workflow_retry, workflow_step
from .lock import EmulatorLock
from .orders import OrderAutomation

LOG = logging.getLogger("gigamoney")
TERMINAL = {"FilledStatus", "CanceledStatus", "PartialWithdrawal", "RejectedStatus", "ExpiredStatus"}
ACTIVE = {"NotReported", "ReplacedNotReported", "ProtectedNotReported", "VarietiesNotReported",
          "WaitToNew", "NewStatus", "WaitToReplace", "PendingReplaceStatus", "ReplacedStatus",
          "PartialFilledStatus", "WaitToCancel", "PendingCancelStatus"}


class OrderUncertain(RuntimeError):
    def __init__(self, message, *, submitted=False):
        self.submitted = submitted
        super().__init__(message)


def decimal(value, name="number") -> Decimal:
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise InvalidOperation
        return number
    except (ValueError, InvalidOperation):
        raise ValueError(f"API returned an invalid {name}.") from None


def order_id(order: dict) -> str:
    value = order.get("order_id_str") or order.get("id")
    if not isinstance(value, str) or not value or not value.isdecimal():
        raise ValueError("API response has no exact order ID.")
    return value


class TradingApiClient(ApiClient):
    """Explicit write routes; the diagnostic ApiClient remains read-only."""
    WRITE_PATHS = {"submit": "/v3/orders/submit", "withdraw": "/v2/orders/withdraw"}

    def _permitted(self, method, path, body):
        return super()._permitted(method, path, body) or (
            method == "POST" and path in self.WRITE_PATHS.values() and isinstance(body, dict))

    def write(self, operation: str, body: dict, *, before_send):
        if operation not in self.WRITE_PATHS:
            raise ValueError("Unsupported order operation.")
        with self._session() as state:
            self.ensure_session(state)
            # Authentication failure happens before the durable send marker.
            before_send()
            result = self._send(state, "POST", self.WRITE_PATHS[operation], body)
            if not result.ok:
                raise ApiError(result)
            return result.payload["data"]


class OrderJournal:
    """Durable per-command intent, acknowledgment and outcome, under a shared lock."""
    def __init__(self, directory: Path, account: str):
        self.directory = directory
        self.account = account
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        digest = hashlib.sha256((self.account + "\0" + key).encode()).hexdigest()
        return self.directory / (digest + ".json")

    def read(self, key: str):
        path = self.path(key)
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text())
            if not isinstance(record, dict) or record.get("account") != self.account or record.get("command_id") != key:
                raise ValueError
            return record
        except (ValueError, OSError):
            raise OrderUncertain("Order journal is unreadable; inspect it before trading.") from None

    def save(self, key: str, record: dict):
        record["updated_at"] = time.time()
        save_session(self.path(key), record)

    def records(self):
        for path in sorted(self.directory.glob("*.json")):
            try:
                record = json.loads(path.read_text())
                if not isinstance(record, dict):
                    raise ValueError
            except (ValueError, OSError):
                raise OrderUncertain("Order journal is unreadable; inspect it before trading.") from None
            if record.get("account") == self.account:
                yield record


class ApiTrading:
    def __init__(self, settings, *, client=None, price_books=None, sleeper=time.sleep):
        from .price_books import KafkaPriceBooks
        self.settings = settings
        self.client = client or TradingApiClient.from_settings(settings)
        self.price_books = price_books or KafkaPriceBooks(settings)
        self.sleeper = sleeper
        self.directory = settings.work_dir / "api"
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.journal = OrderJournal(self.directory / "orders", str(settings.require("gigamoney.accountId")))

    def _lock(self):
        return EmulatorLock(self.directory / "operation.lock", float(self.settings.get("automation.lockTimeoutSeconds", 300)))

    def _read(self, endpoint, params=None):
        attempts = int(self.settings.get("api.readAttempts", 2))
        for attempt in range(1, attempts + 1):
            try:
                return self.client.read(endpoint, params).payload["data"]
            except (TransportError, ApiError) as error:
                if attempt == attempts or (isinstance(error, ApiError) and error.result.http_status < 500):
                    raise
                workflow_retry(error)
                workflow_attempt(attempt + 1)
                workflow_step("read_" + endpoint)
                self.sleeper(float(self.settings.get("api.readRetryDelaySeconds", 0.25)))
        raise AssertionError("unreachable")

    def _orders(self) -> list[dict]:
        context = self.client.account_context()
        output, seen = [], set()
        maximum = int(self.settings.get("api.maxOrderPages", 100))
        for page in range(1, maximum + 1):
            data = self._read("orders", {"account_channel": context["account_channel"], "page": page, "limit": 100})
            rows = data.get("orders")
            if not isinstance(rows, list) or type(data.get("has_more")) is not bool:
                raise ValueError("Order list is incomplete or malformed.")
            for row in rows:
                self._check_account(row, context)
                key = order_id(row)
                if key in seen:
                    raise ValueError("Order pagination repeated an order; refusing an incomplete list.")
                seen.add(key)
                output.append(row)
            if not data["has_more"]:
                if data.get("count") is not None and int(data["count"]) > len(output):
                    raise ValueError("Order list contains fewer orders than the reported count.")
                return output
            if not rows:
                break
        raise ValueError("Order pagination did not finish; refusing an incomplete list.")

    @staticmethod
    def _check_account(row, context):
        if any(str(row.get(k, "")) != value for k, value in context.items()):
            raise ValueError("Order/account routing does not match the selected API account.")

    def _detail(self, identifier: str):
        context = self.client.account_context()
        data = self._read("order_detail", {**context, "order_id": identifier,
                                         "order_id_str": identifier, "is_attached": "false"})
        row = data.get("order")
        if not isinstance(row, dict) or order_id(row) != identifier:
            raise ValueError("Order detail does not match the requested order ID.")
        self._check_account(row, context)
        if row.get("status") not in ACTIVE | TERMINAL:
            raise ValueError("Order detail returned an unknown status.")
        return row

    @staticmethod
    def _positions(data):
        for group in ("us_option", "crypto_hold", "multi_leg", "mmf"):
            if data.get(group, {}).get("holdings"):
                raise ValueError("Non-stock positions require a mapping before publishing this account.")
        rows = data.get("stockhold", {}).get("holdings")
        if not isinstance(rows, list):
            raise ValueError("Portfolio response is missing its stock holdings.")
        return rows

    @logged_workflow
    def query(self) -> dict:
        with self._lock():
            workflow_attempt(1)
            workflow_step("read_positions")
            context = self.client.account_context()
            data = self._read("positions", {"account_channel": context["account_channel"]})
            positions = []
            cost_mode = self.settings.get("api.positionCost", "diluted")
            if cost_mode not in {"average", "diluted"}:
                raise ValueError("api.positionCost must be average or diluted.")
            for row in self._positions(data):
                parts = str(row.get("counter_id", "")).split("/")
                if len(parts) != 3 or parts[0] != "ST":
                    raise ValueError("Unsupported position identifier; refusing a partial snapshot.")
                quantity = decimal(row.get("quantity"), "position quantity")
                price = decimal(row.get("market_price") or row.get("last_done"), "position price")
                if price <= 0:
                    price = decimal(row.get("last_done"), "position price")
                cost = decimal(row.get("diluted_avg_cost" if cost_mode == "diluted" else "average_cost"), "position cost")
                value = price * quantity
                # These columns are today's P/L, not unrealized P/L. Include
                # realized intraday P/L and the broker's daily investment basis.
                invested = decimal(row.get("stock_invest_of_today"), "daily investment")
                pl = (value - invested + decimal(row.get("today_pl"), "daily realized P/L")).quantize(Decimal(".01"), rounding=ROUND_DOWN)
                denominator = decimal(row.get("sum_invest"), "daily P/L denominator")
                percent = ((pl / abs(denominator) * 100).quantize(Decimal(".01"), rounding=ROUND_DOWN)
                           if denominator else None)
                positions.append({"market": parts[1], "symbol": parts[2], "name": row.get("name", ""),
                                  "quantity": str(quantity), "cost": str(cost),
                                  "marketPrice": str(price), "marketValue": str(value.quantize(Decimal(".01"), rounding=ROUND_DOWN)),
                                  "dailyPL": str(pl), "dailyPLPercent": str(percent) if percent is not None else ""})
            workflow_step("read_cash")
            cash = self._read("cash", {"account_channel": context["account_channel"]})
            rows = cash.get("cash_balance")
            if not isinstance(rows, list) or not rows:
                raise ValueError("Cash response is incomplete.")
            balances = [{"currency": row["currency"], "balance": str(decimal(row["balance"], "cash balance"))}
                        for row in rows]
            if (not cash.get("account_no") or data.get("account", {}).get("account_no") != cash["account_no"]
                    or any(str(part.get("account_channel", "")) != context["account_channel"] for part in (data, cash))
                    or len({row["currency"] for row in balances}) != len(balances)):
                raise ValueError("Cash and positions belong to different accounts.")
            return {"queriedAt": datetime.now(timezone.utc).isoformat(),
                    "cash": {"balances": balances}, "positions": positions}

    def _guard_sell(self, counter, quantity):
        context = self.client.account_context()
        data = self._read("positions", {"account_channel": context["account_channel"]})
        if str(data.get("account_channel", "")) != context["account_channel"]:
            raise ValueError("Sell availability belongs to a different account.")
        rows = self._positions(data)
        matches = [row for row in rows if row.get("counter_id") == counter]
        if len(matches) != 1:
            raise ValueError("No unambiguous long holding exists for this sell.")
        row = matches[0]
        owned = decimal(row["quantity"], "owned quantity")
        available = decimal(row["available_quantity"], "available quantity")
        pending = Decimal(0)
        for order in self._orders():
            if order.get("status") not in ACTIVE | TERMINAL:
                raise ValueError("Unknown order status prevents a reliable sell availability check.")
            if order.get("counter_id") == counter and int(order["action"]) == 2 and order["status"] in ACTIVE:
                pending += max(Decimal(0), decimal(order["quantity"]) - decimal(order["executed_qty"]))
        if decimal(quantity) > max(Decimal(0), min(available, owned - pending)):
            raise ValueError("Sell exceeds available long shares after open sells; short selling is disabled.")

    def _guard_submission(self, symbol):
        allowed = self.settings.get("api.allowedSymbols", ["NVDA", "BIYA", "AEHL", "TANH"])
        if symbol not in allowed:
            raise ValueError("Symbol is outside api.allowedSymbols.")
        if any(row.get("sent_at") and row.get("phase") not in {"finished", "rejected"}
               for row in self.journal.records()):
            raise OrderUncertain("A previous submission is unresolved; reconcile the order journal before placing another order.")

    def _market_session(self):
        # Use the app's non-delayed market status, not delayed quote status or
        # a hardcoded weekday schedule that would miss exchange holidays.
        data = self._read("market_status", {})
        matches = [row for row in data.get("market_time", []) if row.get("market") == "US"]
        if len(matches) != 1:
            raise ValueError("Broker did not return one current US trading session.")
        row = matches[0]
        stamp = decimal(row.get("timestamp"), "market-status timestamp")
        age = decimal(str(time.time())) - stamp
        if not 0 <= age <= decimal(str(self.settings.get("api.marketStatusMaxAgeSeconds", 30))):
            raise ValueError("Broker trading-session status is stale or future-dated.")
        session = {201: "premarket", 202: "regular", 203: "afterhours"}.get(row.get("trade_status"))
        if session is None:
            raise ValueError("The US market is closed or in an unsupported session; market order was not queued.")
        return session

    def _prepare(self, symbol, quantity, side, price):
        self._guard_submission(symbol)
        counter = "ST/US/" + symbol
        info = self._read("order_info", {"counter_id": counter})
        if info.get("counter_id") != counter:
            raise ValueError("Order information returned a different security.")
        channel = info["channel_infos"]
        self._check_account(channel, self.client.account_context())
        if not channel.get("support_trade") or channel.get("restricted"):
            raise ValueError("The account cannot trade this security.")
        lot = decimal(info["buy_lot_size" if side == "Buy" else "sell_lot_size"], "lot size")
        if lot <= 0 or decimal(quantity) % lot:
            raise ValueError("Quantity is not a multiple of the supported lot size.")
        if side == "Sell":
            self._guard_sell(counter, quantity)
        pricing = {"requestedOrderType": "LO" if price is not None else "MO"}
        extended_market = False
        if price is None:
            session_name = self._market_session()
            extended_market = session_name != "regular"
            if extended_market and not self.settings.get("api.extendedHoursMarketOrders", True):
                raise ValueError("Premarket/after-hours market-to-limit conversion is disabled.")
            pricing["tradingSession"] = session_name
        kind = "LO" if price is not None or extended_market else "MO"
        config = channel.get("order_type_config", {}).get(kind, {})
        session = 2 if kind == "LO" else 1
        if session not in config.get("support_sessions", []) or 0 not in config.get("support_time_in_forces", []):
            raise ValueError("The original day-order/session settings are unavailable for this security.")
        if "USD" not in channel.get("settlement_currency", []):
            raise ValueError("USD settlement is unavailable for this security.")
        # Match the quick ticket's enabled fee-rebate preferences, with current IDs.
        cards = list(dict.fromkeys(str(channel[k]) for k in
                     ("comm_card_id", "deductions_card_id", "platform_deductions_card_id") if channel.get(k)))
        body = {"action": "1" if side == "Buy" else "2", "counter_id": counter,
                "order_type": kind, "quantity": quantity, "time_in_force": "0",
                "settlement_currency": "USD", "statistics_entry": "classic_quick_mode",
                "commission_free": True, "amount_deducted": True, "platform_amount_deducted": True,
                "card_ids": cards}
        if price is not None:
            # Preserve the original intent/fingerprint; reconcile against this
            # effective request price so old command IDs remain deduplicated.
            body.update(price=OrderAutomation.order_price(price, side), force_only_rth="2")
            pricing.update(priceSource="command", referencePrice=price)
        elif extended_market:
            selected = self.price_books.price(symbol, side, quantity)
            pricing.update(selected.details())
            pricing["priceBookAgeSeconds"] = round(time.time() - selected.timestamp_ns / 1e9, 3)
            effective = OrderAutomation.order_price(selected.price, side, "Kafka book price")
            body.update(price=effective, force_only_rth="2")
        else:
            # A true market order has no client-selected price. Only the
            # extended-hours limit conversion needs a quote/book lookup.
            pricing["priceSource"] = "broker_market_execution"
        pricing["effectiveOrderType"] = kind
        if "price" in body:
            pricing["submittedPrice"] = body["price"]
        workflow_step("prepare_ticket", **pricing)
        LOG.info("api_order_pricing command_id=%s %s", command_context.get(), json.dumps(pricing, separators=(",", ":")))
        return body, pricing

    def _reconcile(self, record, key):
        workflow_step("reconcile_submission")
        if not record.get("order_id"):
            matches = [row for row in self._orders()
                       if row.get("client_request_id") == record["request"]["client_request_id"]]
            if len(matches) == 1:
                record["order_id"] = order_id(matches[0])
                record["phase"] = "accepted"
                self.journal.save(key, record)
            else:
                raise OrderUncertain("Submission outcome is unknown; it was not resubmitted. Inspect the order journal and broker orders.")
        row = self._detail(record["order_id"])
        expected = record["request"]
        if (row.get("counter_id") != expected["counter_id"] or str(row.get("action")) != expected["action"]
                or decimal(row.get("quantity")) != decimal(expected["quantity"])
                or row.get("order_type") != expected["order_type"]
                or (expected["order_type"] == "LO" and decimal(row.get("price")) != decimal(expected["price"]))):
            raise OrderUncertain("Acknowledged order does not match the intended ticket.", submitted=True)
        return row

    def _cancel_one(self, identifier, record, key):
        workflow_step("confirm_cancel", orderId=identifier)
        row = self._detail(identifier)
        if row["status"] in TERMINAL:
            return row
        if not record.get("cancel_sent_at"):
            body = {**self.client.account_context(), "order_id": identifier, "order_id_str": identifier}
            def sent():
                record["cancel_sent_at"] = time.time()
                self.journal.save(key, record)
            try:
                self.client.write("withdraw", body, before_send=sent)
            except (ApiError, SessionError):
                workflow_retry("Cancel response was not confirmed; checking its order status")
        # A cancel acknowledgment is not a terminal status. Never treat absence of a
        # Cancel button or a timeout as successful cancellation.
        for delay in self.settings.get("api.cancelPollDelaysSeconds", [0, 1, 1, 2]):
            if delay:
                self.sleeper(delay)
            row = self._detail(identifier)
            if row["status"] in TERMINAL:
                return row
        raise OrderUncertain("Cancellation is still unconfirmed; no additional cancel was sent.", submitted=bool(record.get("request")))

    def _order(self, symbol, quantity, side, price=None, *, kill=False, dry_run=False, no_submit=False):
        symbol = OrderAutomation.normalize_symbol(symbol)
        side = OrderAutomation.normalize_side(side)
        quantity = OrderAutomation.positive_number(quantity, "quantity")
        price = OrderAutomation.positive_number(price, "price") if price is not None else None
        intent = {"symbol": symbol, "side": side, "quantity": quantity, "price": price, "kill": kill}
        fingerprint = hashlib.sha256(json.dumps(intent, sort_keys=True).encode()).hexdigest()
        key = command_context.get() or "cli:" + uuid.uuid4().hex
        with self._lock():
            workflow_attempt(1)
            if dry_run or no_submit:
                workflow_step("prepare_ticket")
                body, pricing = self._prepare(symbol, quantity, side, price)
                return {"ok": True, "dryRun": dry_run, "submitted": False, "kill": kill,
                        "ticket": {k: v for k, v in body.items() if k != "card_ids"}, "pricing": pricing}
            record = self.journal.read(key)
            if record:
                if record.get("fingerprint") != fingerprint:
                    raise ValueError("Command ID was already used for a different order.")
                if record.get("result"):
                    return {**record["result"], "deduplicated": True}
                if record["phase"] == "rejected":
                    raise ValueError("This command was previously rejected; use a new command ID for a new intent.")
            if not record or record["phase"] == "prepared":
                workflow_step("prepare_ticket")
                body, pricing = self._prepare(symbol, quantity, side, price)
                # Match the app's millisecond request-ID format, monotonically across
                # this account's journal, including multiple processes in the same ms.
                previous = [int(row["request"]["client_request_id"]) for row in self.journal.records() if row.get("request")]
                request_id = str(max([time.time_ns() // 1_000_000, *[x + 1 for x in previous]]))
                body["client_request_id"] = request_id
                record = {"account": self.journal.account, "command_id": key, "fingerprint": fingerprint,
                          "intent": intent, "request": body, "pricing": pricing, "phase": "prepared"}
                self.journal.save(key, record)
                def sent():
                    record.update(phase="dispatched", sent_at=time.time())
                    self.journal.save(key, record)
                workflow_step("submit")
                try:
                    response = self.client.write("submit", body, before_send=sent)
                    record.update(order_id=order_id(response), phase="accepted")
                    self.journal.save(key, record)
                except ApiError as error:
                    # A server-side failure can be ambiguous even with an HTTP reply.
                    record["phase"] = "rejected" if 400 <= error.result.http_status < 500 else "ambiguous"
                    self.journal.save(key, record)
                    if record["phase"] == "rejected":
                        raise
                except (SessionError, ValueError, KeyError, OSError):
                    if record["phase"] == "prepared":
                        raise
                    record["phase"] = "ambiguous"
                    try:
                        self.journal.save(key, record)
                    except (SessionError, OSError) as error:
                        raise OrderUncertain("Submission journal could not be saved; inspect broker orders before continuing.",
                                             submitted=bool(record.get("order_id"))) from error
            if record["phase"] == "ambiguous" or record["phase"] == "dispatched":
                workflow_retry("Submission response is ambiguous; reconciling without resubmission")
                workflow_attempt(2)
            workflow_step("confirm_result")
            try:
                row = self._reconcile(record, key)
                if row["status"] in {"RejectedStatus", "ExpiredStatus"}:
                    raise OrderUncertain("Broker rejected or expired the submitted order.", submitted=True)
                if kill and row["status"] not in TERMINAL:
                    workflow_step("fok_cancel")
                    self.sleeper(float(self.settings.get("api.fokWaitSeconds", 2)))
                    row = self._cancel_one(record["order_id"], record, key)
                result = {"ok": True, "submitted": True, "kill": kill, "result": "api_order",
                          "status": row["status"], "filledPrice": row.get("executed_price", ""),
                          "filledQty": row.get("executed_qty", "0"), "orderId": record["order_id"]}
                if record.get("pricing"):
                    result["pricing"] = record["pricing"]
                record.update(phase="finished", result=result)
                self.journal.save(key, record)
                return result
            except (SessionError, ApiError, ValueError, OSError, KeyError) as error:
                raise OrderUncertain("Order was acknowledged but confirmation failed; inspect its recorded order ID.",
                                     submitted=bool(record.get("order_id"))) from error

    def recover_pending(self):
        """Reconcile interrupted commands before the runner accepts new work.

        Existing requests are only looked up. An acknowledged synthetic FOK may
        still need its one cancellation request after an interrupted process.
        """
        with self._lock():
            records = list(self.journal.records())
        recovered = 0
        for record in records:
            if not record.get("sent_at") or record.get("phase") in {"finished", "rejected"}:
                continue
            intent = record["intent"]
            token = command_context.set(record["command_id"])
            try:
                self._order(intent["symbol"], intent["quantity"], intent["side"], intent["price"], kill=intent["kill"])
                recovered += 1
            finally:
                command_context.reset(token)
        LOG.info("api_recovery_finished recovered_commands=%d", recovered)
        return {"ok": True, "recovered": recovered}

    @logged_workflow
    def market_order(self, symbol, quantity, side, *, dry_run=False, no_submit=False):
        return self._order(symbol, quantity, side, dry_run=dry_run, no_submit=no_submit)

    @logged_workflow
    def limit_order(self, symbol, price, quantity, side, *, kill=False, dry_run=False, no_submit=False):
        return self._order(symbol, quantity, side, price, kill=kill, dry_run=dry_run, no_submit=no_submit)

    @logged_workflow
    def cancel_all(self, *, dry_run=False):
        with self._lock():
            workflow_attempt(1)
            rows = self._orders()
            if any(row.get("status") not in ACTIVE | TERMINAL for row in rows):
                raise ValueError("An unknown order status prevents cancel-all.")
            pending = [row for row in rows if row["status"] in ACTIVE]
            if dry_run:
                return {"dryRun": True, "canceledCount": 0, "remainingDetected": len(pending), "canceledOrders": []}
            maximum = int(self.settings.get("automation.maxCancelCount", 50))
            canceled = []
            for row in pending[:maximum]:
                identifier = order_id(row)
                key = "cancel:" + identifier
                record = self.journal.read(key) or {"account": self.journal.account, "command_id": key,
                                                   "phase": "cancel", "order_id": identifier}
                final = self._cancel_one(identifier, record, key)
                record.update(phase="finished", status=final["status"])
                self.journal.save(key, record)
                if final["status"] in {"CanceledStatus", "PartialWithdrawal"}:
                    canceled.append({"orderId": identifier, "status": final["status"]})
            remaining = [row for row in self._orders() if row["status"] not in TERMINAL]
            if remaining:
                raise OrderUncertain("Cancel-all finished with open orders remaining.")
            return {"completedAt": datetime.now(timezone.utc).isoformat(), "dryRun": False,
                    "canceledCount": len(canceled), "remainingDetected": 0, "canceledOrders": canceled}
