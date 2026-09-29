"""KTrader v1 order snapshots and a durable, acknowledged Kafka outbox."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import math
import re
from zoneinfo import ZoneInfo

from .api import save_session
from .lock import EmulatorLock

LOG = logging.getLogger("gigamoney")
MAX_SEQUENCE = 2**53 - 1
STATUS = {
    **dict.fromkeys(("NotReported", "ReplacedNotReported", "ProtectedNotReported",
                     "VarietiesNotReported", "WaitToNew"), "PENDING_NEW"),
    **dict.fromkeys(("NewStatus", "WaitToReplace", "PendingReplaceStatus", "ReplacedStatus"), "NEW"),
    "PartialFilledStatus": "PARTIALLY_FILLED",
    "WaitToCancel": "PENDING_CANCEL", "PendingCancelStatus": "PENDING_CANCEL",
    "FilledStatus": "FILLED", "CanceledStatus": "CANCELED", "PartialWithdrawal": "CANCELED",
    "RejectedStatus": "REJECTED", "ExpiredStatus": "EXPIRED",
}


def timestamp(value):
    """Broker timestamps are epoch seconds; also accept offset-bearing ISO times."""
    try:
        if isinstance(value, bool) or value in (None, "", "0", 0):
            raise ValueError
        if isinstance(value, str) and "T" in value:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if result.utcoffset() is None:
                raise ValueError
        else:
            result = datetime.fromtimestamp(float(value), timezone.utc)
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        raise ValueError("Order timestamp is missing or invalid.") from None


def number(value, *, positive=False):
    try:
        if isinstance(value, bool):
            raise ValueError
        exact = Decimal(str(value))
        result = float(exact)
        if not math.isfinite(result) or exact < 0 or (positive and exact <= 0):
            raise ValueError
        return int(exact) if exact == exact.to_integral_value() else result
    except (InvalidOperation, ValueError, OverflowError):
        raise ValueError("Order quantity or price is missing or invalid.") from None


def order_snapshot(row, settings, *, observed_at, command_id=None):
    """Map a broker order without inventing its creation time or an execution."""
    from .api_trading import order_id

    created = timestamp(row.get("submitted_at"))
    parts = str(row.get("counter_id", "")).split("/")
    side = {"1": "BUY", "2": "SELL"}.get(str(row.get("action")))
    if len(parts) != 3 or parts[0] != "ST" or not re.fullmatch(r"\S{1,64}", parts[2]) or side is None:
        raise ValueError("Order security or side is unsupported.")
    # The today list does not always expose history/update times. In that case
    # updated_at is when this source observed the state, never the creation time.
    updates = [row.get("updated_at"), row.get("done_at")]
    updates.extend(item.get("time") for item in row.get("order_histories", []) if isinstance(item, dict))
    times = [timestamp(value) for value in updates if value not in (None, "", "0", 0)]
    updated = max([created, *times]) if times else max(created, observed_at)
    raw_status = str(row.get("status", ""))
    status = STATUS.get(raw_status, "UNKNOWN")
    quantity = number(row["quantity"], positive=True) if row.get("quantity") not in (None, "", "0", 0) else None
    filled = number(row.get("executed_qty"))
    if quantity is not None and filled > quantity:
        raise ValueError("Order fill exceeds its quantity.")
    if status == "FILLED" and (filled <= 0 or (quantity is not None and filled != quantity)):
        raise ValueError("Filled order has an incomplete execution quantity.")
    if status == "PARTIALLY_FILLED" and (filled <= 0 or (quantity is not None and filled >= quantity)):
        raise ValueError("Partial fill has an invalid execution quantity.")
    result = {
        "schema_version": 1, "account_id": str(settings.require("gigamoney.accountId")),
        "order_id": order_id(row), "created_at": created.isoformat(), "updated_at": updated.isoformat(),
        "symbol": parts[2], "side": side,
        "order_type": {"MO": "MARKET", "LO": "LIMIT"}.get(row.get("order_type"), "OTHER"),
        "status": status, "quantity": quantity, "filled_quantity": filled,
        "average_fill_price": (number(row["executed_price"])
                               if filled and row.get("executed_price") not in (None, "") else None),
        "message": ("Broker status: " + raw_status + "".join(
            "; " + str(row[key]) for key in ("msg", "withdrawal_reason", "remark") if row.get(key)))[:2000],
    }
    if quantity is None:
        result["notional"] = number(row.get("submitted_amount"), positive=True)
    if result["order_type"] == "LIMIT":
        result["limit_price"] = number(row.get("price"))
    currency = row.get("currency") or row.get("settlement_currency")
    if currency:
        if not re.fullmatch(r"[A-Z]{3}", str(currency)):
            raise ValueError("Order currency is invalid.")
        result["currency"] = currency
    if row.get("time_in_force") is not None:
        value = str(row["time_in_force"])
        if value:
            result["time_in_force"] = "DAY" if value == "0" else value
    numeric_id = settings.get("gigamoney.accountNumId")
    if numeric_id is not None:
        if type(numeric_id) is not int or numeric_id <= 0:
            raise ValueError("Order account_num_id must be a positive integer.")
        result["account_num_id"] = numeric_id
    for key, value in (("command_id", command_id), ("client_order_id", row.get("client_request_id"))):
        if value is not None and str(value):
            result[key] = str(value)
    for key, maximum in (("account_id", 128), ("order_id", 256), ("time_in_force", 32),
                         ("command_id", 256), ("client_order_id", 256)):
        if key in result and (not result[key] or result[key].strip() != result[key] or len(result[key]) > maximum):
            raise ValueError(f"Order {key} is invalid.")
    return result


def state_fields(snapshot):
    # Poll observation times alone do not constitute a changed order.
    return {key: value for key, value in snapshot.items() if key not in {"sequence", "updated_at"}}


class OrderStatusPublisher:
    """Persist pending snapshots before send, and published snapshots after ACK.

    Each process republishes an unchanged order once on first observation. A
    retry sends the exact persisted payload/sequence before any newer snapshot.
    """
    def __init__(self, settings):
        self.settings = settings
        self.account = str(settings.require("gigamoney.accountId"))
        self.directory = settings.work_dir / "api" / "order-status"
        self.directory.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.zone = ZoneInfo(settings.get("api.orderTimezone", "America/New_York"))
        self._seen = set()

    def path(self, identifier):
        digest = hashlib.sha256((self.account + "\0" + identifier).encode()).hexdigest()
        return self.directory / (digest + ".json")

    def _read(self, identifier):
        path = self.path(identifier)
        if not path.exists():
            return {"version": 1, "account_id": self.account, "order_id": identifier,
                    "published": None, "pending": None}
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if (record["version"] != 1 or record["account_id"] != self.account
                    or record["order_id"] != identifier):
                raise ValueError
            for name in ("published", "pending"):
                item = record[name]
                if item is not None and (item["account_id"] != self.account or item["order_id"] != identifier
                        or type(item["sequence"]) is not int or not 0 <= item["sequence"] <= MAX_SEQUENCE):
                    raise ValueError
                if item is not None:
                    if (timestamp(item["updated_at"]) < timestamp(item["created_at"])
                            or item["schema_version"] != 1):
                        raise ValueError
                    # A truncated state file must not turn into a new order.
                    for field in ("symbol", "side", "order_type", "status", "quantity",
                                  "filled_quantity", "average_fill_price"):
                        if field not in item:
                            raise ValueError
            if record["published"] is None and record["pending"] is None:
                raise ValueError
            if record["published"] is not None and record["pending"] is not None:
                published, pending = record["published"], record["pending"]
                if (pending["sequence"] < published["sequence"]
                        or pending["created_at"] != published["created_at"]
                        or timestamp(pending["updated_at"]) < timestamp(published["updated_at"])
                        or (pending["sequence"] == published["sequence"] and pending != published)):
                    raise ValueError
            return record
        except (OSError, ValueError, TypeError, KeyError):
            raise ValueError("Order publication record is unreadable; preserve it for sequence recovery.") from None

    def _deliver(self, record, deliver):
        snapshot = record["pending"]
        partition, offset = deliver(snapshot)
        record.update(published=snapshot, pending=None,
                      published_at=datetime.now(timezone.utc).isoformat(), partition=partition, offset=offset)
        save_session(self.path(record["order_id"]), record)
        self._seen.add(record["order_id"])

    def publish(self, snapshots, deliver):
        """Deliver independent orders even if another order's delivery fails."""
        sent, failed = 0, 0
        with EmulatorLock(self.directory.parent / "order-status.lock",
                          float(self.settings.get("automation.lockTimeoutSeconds", 300))):
            today = datetime.now(timezone.utc).astimezone(self.zone).date()
            observed_ids = {snapshot["order_id"] for snapshot in snapshots}
            for snapshot in snapshots:
                try:
                    if snapshot["account_id"] != self.account:
                        raise ValueError("Order publication account mismatch.")
                    if timestamp(snapshot["created_at"]).astimezone(self.zone).date() != today:
                        continue
                    identifier = snapshot["order_id"]
                    record = self._read(identifier)
                    if record["pending"] is not None:
                        self._deliver(record, deliver)
                        sent += 1
                    previous = record["published"]
                    if previous is not None and previous["created_at"] != snapshot["created_at"]:
                        raise ValueError("Order creation time changed for an existing order ID.")
                    changed = previous is None or state_fields(previous) != state_fields(snapshot)
                    if not changed and identifier in self._seen:
                        continue
                    if changed:
                        sequence = previous["sequence"] + 1 if previous else 0
                        if sequence > MAX_SEQUENCE:
                            raise ValueError("Order sequence exceeds the KTrader contract.")
                        update = dict(snapshot, sequence=sequence)
                        if previous:
                            update["updated_at"] = max(timestamp(previous["updated_at"]),
                                                       timestamp(update["updated_at"])).isoformat()
                    else:
                        update = previous
                    record["pending"] = update
                    save_session(self.path(identifier), record)
                    self._deliver(record, deliver)
                    sent += 1
                except Exception as error:
                    failed += 1
                    LOG.error("Order publication failed error_type=%s; will retry on next holdings refresh.",
                              type(error).__name__)
            # A failed send remains deliverable even if a later broker list
            # omits the order or the orders endpoint is temporarily unavailable.
            for path in sorted(self.directory.glob("*.json")):
                try:
                    saved = json.loads(path.read_text(encoding="utf-8"))
                    if saved["account_id"] != self.account or saved["order_id"] in observed_ids:
                        continue
                    if path != self.path(saved["order_id"]):
                        raise ValueError("Order publication record filename mismatch.")
                    record = self._read(saved["order_id"])
                    pending = record["pending"]
                    if pending is not None and timestamp(pending["created_at"]).astimezone(self.zone).date() == today:
                        self._deliver(record, deliver)
                        sent += 1
                except Exception as error:
                    failed += 1
                    LOG.error("Pending order publication failed error_type=%s; record retained.", type(error).__name__)
        LOG.info("Order status publication completed delivered=%d failed=%d observed=%d.", sent, failed, len(snapshots))
        if failed:
            raise RuntimeError(f"Order status publication failed for {failed} order(s); pending snapshots retained.")
        return sent
