"""Today's discovered orders; terminal orders retain evidence but stop polling."""
from datetime import datetime, timezone
import json
from zoneinfo import ZoneInfo

from .api import save_session
from .order_status import state_fields, timestamp

TERMINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


class OrderPollingState:
    def __init__(self, settings):
        self.account = str(settings.require("gigamoney.accountId"))
        self.zone = ZoneInfo(settings.get("api.orderTimezone", "America/New_York"))
        self.today = datetime.now(timezone.utc).astimezone(self.zone).date()
        self.path = settings.work_dir / "api/order-polling.json"
        self.data = {"version": 1, "account_id": self.account, "date": self.today.isoformat(),
                     "snapshots": {}, "pending": {}}
        if self.path.exists():
            saved = json.loads(self.path.read_text())
            if not isinstance(saved, dict) or saved.get("version") != 1 or saved.get("account_id") != self.account:
                raise ValueError("Order polling state belongs to another account or version.")
            if saved.get("date") == self.data["date"]:
                if not isinstance(saved.get("snapshots"), dict) or not isinstance(saved.get("pending"), dict):
                    raise ValueError("Order polling state is incomplete.")
                self.data = saved
                for identifier, snapshot in saved["snapshots"].items():
                    if (not identifier.isdecimal() or not isinstance(snapshot, dict)
                            or snapshot.get("order_id") != identifier or snapshot.get("account_id") != self.account
                            or not self.is_today(snapshot.get("created_at")) or not snapshot.get("status")):
                        raise ValueError("Order polling snapshot is invalid; preserve it for recovery.")
                for identifier, created_at in saved["pending"].items():
                    if not identifier.isdecimal() or not self.is_today(created_at) or self.terminal(identifier):
                        raise ValueError("Pending order polling identity or date is invalid.")
        self._saved = json.dumps(self.data, sort_keys=True)
        # Seed a new day's/process's tracking from delivery receipts. This avoids
        # querying already-terminal orders during an upgrade or restart.
        if not self.data["snapshots"] and not self.data["pending"]:
            for path in sorted((settings.work_dir / "api/order-status").glob("*.json")):
                record = json.loads(path.read_text())
                snapshot = record.get("pending") or record.get("published")
                if snapshot and str(snapshot.get("order_id", "")).isdecimal():
                    self.observe({key: value for key, value in snapshot.items() if key != "sequence"})

    def is_today(self, created_at):
        return timestamp(created_at).astimezone(self.zone).date() == self.today

    def terminal(self, identifier):
        return self.data["snapshots"].get(identifier, {}).get("status") in TERMINAL_STATUSES

    def track(self, identifier, created_at):
        if not isinstance(identifier, str) or not identifier.isdecimal():
            raise ValueError("Order polling requires an exact broker order ID.")
        if self.is_today(created_at) and not self.terminal(identifier):
            self.data["pending"][identifier] = timestamp(created_at).isoformat()

    def observe(self, snapshot):
        identifier = snapshot["order_id"]
        if snapshot["account_id"] != self.account:
            raise ValueError("Order polling account mismatch.")
        if not self.is_today(snapshot["created_at"]):
            return
        previous = self.data["snapshots"].get(identifier)
        if previous and previous["created_at"] != snapshot["created_at"]:
            raise ValueError("Order creation time changed during polling.")
        if previous is None or state_fields(previous) != state_fields(snapshot):
            self.data["snapshots"][identifier] = snapshot
        if self.terminal(identifier):
            self.data["pending"].pop(identifier, None)
        else:
            self.track(identifier, snapshot["created_at"])

    def save(self):
        serialized = json.dumps(self.data, sort_keys=True)
        if serialized != self._saved:
            save_session(self.path, self.data)
            self._saved = serialized
