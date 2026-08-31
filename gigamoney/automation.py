from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable

from .adb import Adb
from .config import Settings
from .errors import ConfigurationError, SafetyError, UiError
from .ui import bounds, find_by_resource, find_by_text, node_text, nodes, resource_name
from .vision import OcrEngine, OcrResult, classify_png, contains_all, normalized_text

LOG = logging.getLogger("gigamoney")


@dataclass
class TicketNodes:
    price: ET.Element | None
    quantity: ET.Element | None
    submit: ET.Element | None


@dataclass
class OrderResult:
    kind: str | None
    status: str
    cancel_node: ET.Element | None
    filled_price: str
    filled_quantity: str
    canceled: bool
    submission_success: bool


class Automation:
    SIDE_POINTS = {"Buy": (724, 2282), "Sell": (940, 2282)}

    def __init__(self, settings: Settings, adb: Adb | None = None, ocr: OcrEngine | None = None):
        self.settings = settings
        self.adb = adb or Adb(settings)
        self.ocr = ocr or OcrEngine(settings)

    def log(self, message: str) -> None:
        LOG.info(message)

    def tap_node(self, node: ET.Element) -> None:
        self.adb.tap(*bounds(node).center)

    def wait_for(
        self,
        finder: Callable[[ET.Element], ET.Element | bool | None],
        description: str,
        *,
        timeout: float = 6,
        password: bool = False,
    ) -> tuple[ET.Element, ET.Element | bool] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            xml = self.adb.dump_xml()
            if password and self.handle_trade_password(xml):
                time.sleep(0.5)
                continue
            match = finder(xml)
            if match is not None and match is not False:
                return xml, match
            time.sleep(0.25)
        self.log(f"Timed out waiting for {description}.")
        return None

    def screenshot_ocr(self) -> tuple[bytes, OcrResult]:
        screenshot = self.adb.screenshot()
        return screenshot, self.ocr.png(screenshot)

    def dismiss_ad_if_present(self, *, maximum: int = 2) -> int:
        dismissed = 0
        for _ in range(maximum + 1):
            first = classify_png(self.adb.screenshot())
            if first in {"Ad", "Ambiguous"}:
                time.sleep(0.2)
                second = classify_png(self.adb.screenshot())
                if first == "Ad" and second == "Ad":
                    stable = "Ad"
                elif second in {"None", "ConfirmationLike"}:
                    stable = second
                else:
                    stable = "Ambiguous"
            else:
                stable = first
            if stable in {"None", "ConfirmationLike"}:
                return dismissed
            if stable == "Ambiguous":
                raise SafetyError("Ad guard saw an ambiguous dark screen; refusing to press Back.")
            if dismissed >= maximum:
                raise UiError("Advertisement remained after two Back presses.")
            self.log("Stable full-screen advertisement detected; pressing Back.")
            self.adb.keyevent("KEYCODE_BACK")
            dismissed += 1
            time.sleep(0.7)
        return dismissed

    def ensure_home(self) -> OcrResult:
        _, ocr = self.screenshot_ocr()
        restarted = False
        if "isn't responding" in normalized_text(ocr.text):
            self.adb.restart()
            restarted = True
        self.adb.ensure_foreground()
        _, ocr = self.screenshot_ocr()
        if "isn't responding" in normalized_text(ocr.text):
            if restarted:
                raise UiError("GigaMoney still isn't responding after restart.")
            self.adb.restart()
            _, ocr = self.screenshot_ocr()
            if "isn't responding" in normalized_text(ocr.text):
                raise UiError("GigaMoney still isn't responding after restart.")
        if self.dismiss_ad_if_present():
            _, ocr = self.screenshot_ocr()
        maximum = int(self.settings.get("automation.maxHomeBacks", 3))
        for attempt in range(maximum + 1):
            if contains_all(ocr.text, ("Watchlist", "Market", "Feed", "Portfolio")):
                self.log("Home screen detected by OCR.")
                return ocr
            # UI hierarchy is a useful fallback when Tesseract misses a tab label.
            xml = self.adb.dump_xml()
            labels = {node_text(node).casefold() for node in nodes(xml)}
            if all(value in labels for value in ("watchlist", "market", "feed", "portfolio")):
                self.log("Home screen detected by UI hierarchy.")
                return ocr
            if attempt == maximum:
                raise UiError(
                    f"Home screen was not detected after {maximum} Back presses. OCR: {ocr.text[:500]}"
                )
            self.log(f"Home screen not detected; pressing Back ({attempt + 1}/{maximum}).")
            self.adb.keyevent("KEYCODE_BACK")
            time.sleep(0.85)
            _, ocr = self.screenshot_ocr()
        raise AssertionError("unreachable")

    def handle_trade_password(self, xml: ET.Element | None = None) -> bool:
        xml = xml or self.adb.dump_xml()
        marker = find_by_resource(xml, "et_pwd")
        if marker is None:
            marker = find_by_text(xml, "Enter Trade Password", exact=False)
        if marker is None:
            return False
        password = str(self.settings.get("gigamoney.tradePassword", ""))
        if not password:
            raise ConfigurationError(
                "The app requested a trade password, but gigamoney.tradePassword is empty."
            )
        field = find_by_resource(xml, "et_pwd")
        if field is None:
            field = marker
        self.log("Entering configured trade password.")
        self.adb.set_text_at(*bounds(field).center, password)
        refreshed = self.adb.dump_xml()
        confirm = find_by_resource(refreshed, "common_rb_right")
        if confirm is None:
            for label in ("Confirm", "OK", "Yes"):
                confirm = find_by_text(refreshed, label, exact=True, prefer_bottom=True)
                if confirm is not None:
                    break
        if confirm is not None:
            self.tap_node(confirm)
        # The current numeric-password sheet has no confirmation button and
        # closes automatically after the configured number of digits.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            time.sleep(0.25)
            candidate = self.adb.dump_xml()
            if find_by_resource(candidate, "et_pwd") is None:
                return True
        raise UiError("Trade-password prompt remained visible after password entry.")

    def select_watchlist_all(self) -> None:
        xml = self.adb.dump_xml()
        watchlist = find_by_text(xml, "Watchlist", exact=True, prefer_bottom=True)
        if watchlist is not None:
            self.tap_node(watchlist)
        else:
            self.adb.tap(108, 2332)
        time.sleep(0.45)
        xml = self.adb.dump_xml()
        all_tab = find_by_text(
            xml,
            "ALL",
            exact=True,
            predicate=lambda node: resource_name(node) == "tv_stock_list_name",
        )
        if all_tab is not None:
            self.tap_node(all_tab)
            time.sleep(0.35)

    def _ocr_symbol(self, symbol: str, ocr: OcrResult) -> bool:
        candidates = [word for word in ocr.words if word.text.upper() == symbol]
        if not candidates:
            return False
        self.adb.tap(*min(candidates, key=lambda word: word.box.top).center)
        return True

    def open_symbol(self, symbol: str, home_ocr: OcrResult | None = None) -> None:
        target = symbol.upper()
        if home_ocr and self._ocr_symbol(target, home_ocr):
            self.log(f"Matched {target} by OCR.")
            time.sleep(0.8)
            return
        self.select_watchlist_all()
        maximum = int(self.settings.get("automation.maxWatchlistScrolls", 8))
        for scroll in range(maximum + 1):
            _, ocr = self.screenshot_ocr()
            if self._ocr_symbol(target, ocr):
                self.log(f"Matched {target} by OCR.")
                time.sleep(0.8)
                return
            xml = self.adb.dump_xml()
            candidates = [
                node for node in nodes(xml)
                if node_text(node).upper() == target
                and resource_name(node) == "market_tv_market_item_name"
            ]
            node = min(candidates, key=lambda candidate: bounds(candidate).top) if candidates else find_by_text(xml, target)
            if node is not None:
                self.tap_node(node)
                time.sleep(0.8)
                return
            if scroll == maximum:
                raise UiError(f"{target} was not visible after {maximum} watchlist scrolls.")
            self.adb.swipe(540, 1820, 540, 650, 500)
            time.sleep(0.45)

    @staticmethod
    def ticket_nodes(xml: ET.Element) -> TicketNodes:
        price = quantity = None
        for container in nodes(xml):
            name = resource_name(container)
            if name not in {"deal_quick_price", "deal_quick_qty"}:
                continue
            field = next((child for child in container.iter("node") if resource_name(child) == "et_input"), None)
            if name == "deal_quick_price":
                price = field
            else:
                quantity = field
        return TicketNodes(price, quantity, find_by_resource(xml, "btn_place_order"))

    def open_ticket(self, side: str, *, market: bool = False) -> TicketNodes:
        point = self.SIDE_POINTS.get(side)
        if point is None:
            raise ValueError(f"Unsupported side: {side}")
        for attempt in range(2):
            self.log(f"Opening {side} ticket (attempt {attempt + 1}/2).")
            started = time.monotonic()
            self.adb.tap(*point)
            time.sleep(0.3)
            handled = self.handle_trade_password()
            if not handled:
                time.sleep(max(0, 0.9 - (time.monotonic() - started)))
            ticket = self.ticket_nodes(self.adb.dump_xml())
            if ticket.price is not None and ticket.quantity is not None and ticket.submit is not None:
                if market:
                    return self.set_market_order_type()
                return ticket
            time.sleep(0.5)
        raise UiError(f"Could not open the {side} ticket with all required controls.")

    def set_market_order_type(self) -> TicketNodes:
        self.log("Changing order type to Market.")
        self.adb.tap(545, 1736)
        time.sleep(0.5)
        xml = self.adb.dump_xml()
        option = find_by_text(xml, "Market", exact=True, prefer_bottom=True)
        if option is None:
            raise UiError("The Market option was not visible in the order-type menu.")
        self.tap_node(option)
        time.sleep(0.7)
        xml = self.adb.dump_xml()
        type_node = find_by_resource(xml, "tv_quick_type_content")
        ticket = self.ticket_nodes(xml)
        if (
            type_node is None
            or node_text(type_node) != "Market"
            or ticket.quantity is None
            or ticket.submit is None
        ):
            raise UiError("Market order type did not expose Qty and Submit controls.")
        return ticket

    def order_result(self) -> OrderResult:
        xml = self.adb.dump_xml()
        status_node = find_by_resource(xml, "tv_status")
        status = node_text(status_node) if status_node is not None else ""
        cancel = find_by_resource(xml, "tvCancel")
        execution = find_by_resource(xml, "tv_execution_price")
        done = find_by_resource(xml, "tv_done_num")
        confirmed = all(find_by_resource(xml, name) is not None for name in (
            "tvModify", "tvCancel", "tvDuplicate", "tvDetail"
        ))
        reported = execution is not None and done is not None
        canceled = status in {"Canceled", "Cancelled"}
        kind = "Canceled" if canceled else "Confirmed" if confirmed else "ExecutionReported" if reported else None
        return OrderResult(
            kind,
            status,
            cancel,
            node_text(execution) if execution is not None else "",
            node_text(done) if done is not None else "",
            canceled,
            not canceled and (confirmed or reported),
        )

    def wait_order_result(self) -> OrderResult | None:
        for delay in (2.0, 1.0, 1.0):
            time.sleep(delay)
            result = self.order_result()
            if result.submission_success:
                return result
        return None

    @staticmethod
    def positive_number(value: str, name: str) -> str:
        try:
            parsed = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{name} must be a number: {value}") from exc
        if not parsed.is_finite() or parsed <= 0:
            raise ValueError(f"{name} must be positive: {value}")
        return format(parsed, "f")

    @staticmethod
    def normalize_symbol(value: str) -> str:
        import re
        symbol = value.strip().upper()
        if not re.fullmatch(r"[A-Z0-9.\-]+", symbol):
            raise ValueError(f"Invalid symbol: {value}")
        return symbol

    @staticmethod
    def normalize_side(value: str) -> str:
        side = value.strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"Invalid side: {value}")
        return side.title()

    def cancel_confirm_button(self, xml: ET.Element) -> ET.Element | None:
        button = find_by_resource(xml, "common_rb_right")
        if button is not None:
            return button
        for text in ("Confirm", "OK", "Yes"):
            button = find_by_text(
                xml, text, exact=True,
                predicate=lambda candidate: bounds(candidate).top > 1500,
            )
            if button is not None:
                return button
        return None

    def confirm_cancel(self) -> None:
        time.sleep(0.7)
        dismissed = self.dismiss_ad_if_present()
        if dismissed:
            time.sleep(0.3)
            button = self.cancel_confirm_button(self.adb.dump_xml())
            if button is None:
                raise UiError("Cancel confirmation disappeared after ad dismissal.")
            self.tap_node(button)
        else:
            self.adb.tap(792, 2282)
