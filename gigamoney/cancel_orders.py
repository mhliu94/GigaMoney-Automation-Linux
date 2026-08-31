from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from .automation import Automation
from .errors import UiError
from .ui import bounds, child_text, find_all_by_resource, find_by_resource

LOG = logging.getLogger("gigamoney")


_ORDER_FIELDS = {
    "name": "wealth_item_order_normal_tv_name",
    "market": "wealth_item_tv_market",
    "symbol": "wealth_item_tv_code",
    "side": "wealth_item_order_normal_tv_action",
    "status": "wealth_item_order_normal_tv_status",
    "price": "wealth_item_order_normal_tv_price",
    "latest": "tv_price",
    "quantity": "wealth_item_order_normal_tv_quantity",
    "filledQty": "wealth_item_order_normal_tv_executed_quantity",
}


def in_progress_rows(xml) -> list:
    output = []
    for row in find_all_by_resource(xml, "cl_order"):
        rect = bounds(row)
        if not 500 <= rect.top <= 2100:
            continue
        if not (child_text(row, "wealth_item_order_normal_tv_name") or child_text(row, "wealth_item_tv_code")):
            continue
        status = child_text(row, "wealth_item_order_normal_tv_status")
        if status in {"Canceled", "Cancelled", "Rejected", "Filled"}:
            continue
        output.append(row)
    return sorted(output, key=lambda row: bounds(row).top)


def order_summary(row) -> dict:
    return {name: child_text(row, resource) for name, resource in _ORDER_FIELDS.items()}


class CancelAutomation(Automation):
    @staticmethod
    def _orders_page(xml) -> bool:
        return any(
            find_by_resource(xml, name) is not None
            for name in ("wealth_rv_order", "view_order_filter", "wealth_refresh_layout_order")
        )

    def _open_orders_page(self) -> None:
        for _ in range(8):
            self.adb.swipe(540, 1212, 540, 420, 120)
            time.sleep(0.09)
        self.adb.tap(368, 268)
        time.sleep(0.18)

    def _open_in_progress(self):
        self.adb.tap(756, 2332)
        time.sleep(0.5)
        self._open_orders_page()
        self.adb.tap(433, 371)
        time.sleep(0.25)
        xml = self.adb.dump_xml()
        if self.handle_trade_password(xml):
            time.sleep(3)
            self._open_orders_page()
            self.adb.tap(433, 371)
            time.sleep(0.25)
            xml = self.adb.dump_xml()
        if self._orders_page(xml):
            return xml
        result = self.wait_for(self._orders_page, "In progress orders", timeout=3)
        if result is None:
            raise UiError("In progress orders did not open.")
        return result[0]

    def _orders_after_cancel(self):
        xml = self.adb.dump_xml()
        if self.handle_trade_password(xml):
            time.sleep(3)
            xml = self.adb.dump_xml()
        if self._orders_page(xml):
            return xml
        self.adb.keyevent("KEYCODE_BACK")
        time.sleep(0.45)
        xml = self.adb.dump_xml()
        if self._orders_page(xml):
            return xml
        result = self.wait_for(self._orders_page, "Orders page after cancel", timeout=3)
        if result is None:
            raise UiError("Orders page did not remain visible after cancel.")
        return result[0]

    def cancel_all(self, *, dry_run: bool = False) -> dict:
        self.adb.validate(strict_layout=True)
        self.ensure_home()
        xml = self._open_in_progress()
        canceled: list[dict] = []
        remaining = 0
        maximum = int(self.settings.get("automation.maxCancelCount", 50))
        for _ in range(maximum):
            rows = in_progress_rows(xml)
            remaining = len(rows)
            if not rows or dry_run:
                break
            self.dismiss_ad_if_present()
            xml = self.adb.dump_xml()
            rows = in_progress_rows(xml)
            if not rows:
                remaining = 0
                break
            summary = order_summary(rows[0])
            LOG.info("Opening in-progress order %s %s.", summary.get("side"), summary.get("symbol"))
            self.adb.tap(540, 605)
            time.sleep(0.3)
            self.adb.tap(519, 767)
            self.confirm_cancel()
            time.sleep(2)
            canceled.append(summary)
            xml = self._orders_after_cancel()
        remaining = len(in_progress_rows(xml))
        if not dry_run and remaining and len(canceled) >= maximum:
            raise UiError(f"Stopped after maxCancelCount={maximum} with orders still in progress.")
        if canceled:
            time.sleep(2.5)
        self.adb.tap(124, 268)
        time.sleep(0.5)
        return {
            "completedAt": datetime.now(timezone.utc).isoformat(),
            "dryRun": dry_run,
            "canceledCount": len(canceled),
            "remainingDetected": remaining,
            "canceledOrders": canceled,
        }
