from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone

from .automation import Automation
from .errors import UiError
from .ui import bounds, child_text, find_all_by_resource, find_by_resource, node_text, nodes
from .vision import OcrResult

LOG = logging.getLogger("gigamoney")

def _market_for_position_row(xml, row) -> str:
    row_top = bounds(row).top
    headers = [
        header for header in find_all_by_resource(xml, "wealth_stock_hold_header")
        if bounds(header).top <= row_top
    ]
    if not headers:
        return ""
    header = max(headers, key=lambda candidate: bounds(candidate).top)
    return child_text(header, "tv_market_title")


def positions_from_xml(xml) -> list[dict]:
    """Read visible position rows from one zero-idle UIAutomator hierarchy."""
    output = []
    for row in find_all_by_resource(xml, "wealth_stock_hold_item_view"):
        symbol = child_text(row, "tv_stock_code").strip().upper()
        name = child_text(row, "tv_stock_name")
        if not (symbol or name):
            continue
        output.append({
            "market": _market_for_position_row(xml, row),
            "symbol": symbol,
            "name": name,
            "marketValue": child_text(row, "tv_column_00"),
            "quantity": child_text(row, "tv_column_01"),
            "marketPrice": child_text(row, "tv_column_10"),
            "cost": child_text(row, "tv_column_11"),
            "dailyPL": child_text(row, "tv_column_20"),
            "dailyPLPercent": child_text(row, "tv_column_21"),
            "_rowTop": bounds(row).top,
        })
    return output


def portfolio_bottom_from_xml(xml) -> bool:
    bottom_markers = ("wealth_ut_item_view", "tv_collapse_mmf", "wealth_tv_ut_title")
    if any(find_by_resource(xml, marker) is not None for marker in bottom_markers):
        return True
    hierarchy_text = " ".join(node_text(node) for node in nodes(xml))
    return bool(re.search(r"(?i)\bFund\b|Opt\s+in\s+Cash\s+Plus|Cash\s+Plus", hierarchy_text))


def cash_data(xml) -> dict:
    balances = []
    for row in find_all_by_resource(xml, "rootContainer"):
        currency = child_text(row, "tv_currency")
        balance = child_text(row, "tv_balance")
        if currency or balance:
            balances.append({"currency": currency, "balance": balance})

    def text(name: str) -> str:
        node = find_by_resource(xml, name)
        return node_text(node) if node is not None else ""

    return {
        "totalUsd": text("tv_summary_value"),
        "withdrawableCash": text("tv_withdraw_cash_value"),
        "incomingFunds": text("tv_cash_in_arrive_value"),
        "blocked": text("frozenTag"),
        "outstanding": text("settleTag"),
        "balances": balances,
    }


class HoldingsAutomation(Automation):
    def _find_cash(self, ocr: OcrResult) -> tuple[int, int] | None:
        candidates = [
            word for word in ocr.words
            if re.sub(r"[^A-Za-z]", "", word.text).upper() == "CASH"
            and word.center[0] >= 650
            and 250 <= word.center[1] <= 900
        ]
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda word: (word.center[0] - 945) ** 2 + (word.center[1] - 462) ** 2,
        ).center

    def _portfolio_top(self) -> tuple[int, int]:
        maximum = int(self.settings.get("automation.maxPositionScrolls", 8))
        for attempt in range(maximum + 1):
            _, ocr = self.screenshot_ocr()
            if "enter trade password" in " ".join(ocr.text.split()).casefold():
                self.handle_trade_password()
                time.sleep(3)
                continue
            cash = self._find_cash(ocr)
            if cash:
                return cash
            if attempt == maximum:
                raise UiError("Portfolio top Cash control did not become visible.")
            self.adb.swipe(540, 1120, 540, 1560, 280)
            time.sleep(0.35)
        raise AssertionError("unreachable")

    def _open_portfolio(self) -> tuple[int, int]:
        self.dismiss_ad_if_present()
        self.adb.tap(756, 2332)
        time.sleep(0.7)
        return self._portfolio_top()

    def _open_cash(self, point: tuple[int, int]):
        self.adb.tap(*point)

        def marker(xml):
            title = find_by_resource(xml, "title_bar_title")
            if title is not None and node_text(title) == "Cash":
                return title
            return find_by_resource(xml, "group_cash")

        result = self.wait_for(marker, "Cash page", timeout=8, password=True)
        if result is None:
            raise UiError("Cash page did not open.")
        return result[0]

    @staticmethod
    def _merge_position(destination: dict[str, dict], position: dict) -> None:
        key = f"{position.get('market')}:{position.get('symbol')}" if position.get("market") else position.get("symbol", "")
        if not key:
            return
        if key not in destination:
            destination[key] = position
            return
        for name in (
            "market", "name", "marketValue", "quantity", "marketPrice", "cost",
            "dailyPL", "dailyPLPercent",
        ):
            if not destination[key].get(name) and position.get(name):
                destination[key][name] = position[name]

    def _collect_positions(self) -> list[dict]:
        positions: dict[str, dict] = {}
        last_signature = ""
        reached_bottom = False
        maximum = int(self.settings.get("automation.maxPositionScrolls", 8))
        for scroll in range(maximum + 1):
            xml = self.adb.dump_xml(wait_for_idle_timeout_ms=0)
            visible = positions_from_xml(xml)
            for position in visible:
                self._merge_position(positions, position)
            if portfolio_bottom_from_xml(xml):
                reached_bottom = True
                break
            signature = "|".join(
                f"{position['symbol']}:{round(float(position['_rowTop']) / 20)}"
                for position in visible
            )
            if scroll > 0 and signature and signature == last_signature:
                reached_bottom = True
                break
            last_signature = signature
            if scroll == maximum:
                break
            self.adb.swipe(540, 2070, 540, 760, 180)
            time.sleep(0.35)
        if not reached_bottom:
            raise UiError(
                f"Portfolio bottom was not reached after {maximum} scrolls; refusing partial holdings."
            )
        for position in positions.values():
            position.pop("_rowTop", None)
        return list(positions.values())

    def _query_once(self) -> dict:
        self.ensure_home()
        cash_point = self._open_portfolio()
        cash_xml = self._open_cash(cash_point)
        cash = cash_data(cash_xml)
        self.adb.keyevent("KEYCODE_BACK")
        time.sleep(0.45)
        positions = self._collect_positions()
        return {
            "queriedAt": datetime.now(timezone.utc).isoformat(),
            "cash": cash,
            "positions": positions,
        }

    def query(self) -> dict:
        self.adb.validate(strict_layout=True)
        for attempt in range(2):
            try:
                return self._query_once()
            except Exception:
                if attempt:
                    raise
                LOG.exception("Holdings query failed on the first attempt; restarting and retrying.")
                self.adb.restart()
        raise AssertionError("unreachable")
