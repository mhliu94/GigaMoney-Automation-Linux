from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation

from .automation import Automation, OrderResult, TicketNodes
from .errors import UiError
from .ui import bounds

LOG = logging.getLogger("gigamoney")


class OrderAutomation(Automation):
    def _restore_limit(self, price: str, quantity: str) -> TicketNodes:
        ticket = self.ticket_nodes(self.adb.dump_xml())
        if ticket.price is None or ticket.quantity is None or ticket.submit is None:
            raise UiError("Limit ticket was unavailable after dismissing an ad.")
        self.adb.set_text_at(*bounds(ticket.price).center, price)
        ticket = self.ticket_nodes(self.adb.dump_xml())
        if ticket.quantity is None:
            raise UiError("Qty field disappeared while restoring the limit ticket.")
        self.adb.set_text_at(*bounds(ticket.quantity).center, quantity)
        ticket = self.ticket_nodes(self.adb.dump_xml())
        if ticket.submit is None:
            raise UiError("Submit disappeared while restoring the limit ticket.")
        return ticket

    def _restore_market(self, quantity: str) -> TicketNodes:
        ticket = self.ticket_nodes(self.adb.dump_xml())
        if ticket.quantity is None or ticket.submit is None:
            raise UiError("Market ticket was unavailable after dismissing an ad.")
        self.adb.set_text_at(*bounds(ticket.quantity).center, quantity)
        ticket = self.ticket_nodes(self.adb.dump_xml())
        if ticket.submit is None:
            raise UiError("Submit disappeared while restoring the market ticket.")
        return ticket

    def _submit_limit(self, price: str, quantity: str) -> None:
        dismissed = self.dismiss_ad_if_present()
        if dismissed:
            time.sleep(0.3)
            ticket = self._restore_limit(price, quantity)
            self.tap_node(ticket.submit)  # type: ignore[arg-type]
        else:
            self.adb.tap(734, 1540)

    def _submit_market(self, quantity: str) -> None:
        dismissed = self.dismiss_ad_if_present()
        if dismissed:
            time.sleep(0.3)
            ticket = self._restore_market(quantity)
            self.tap_node(ticket.submit)  # type: ignore[arg-type]
        else:
            self.adb.tap(734, 1540)

    @staticmethod
    def _decimal(value: str) -> Decimal | None:
        value = value.strip().replace(",", "")
        if not value or value == "--":
            return None
        try:
            return Decimal(value)
        except InvalidOperation:
            return None

    def _kill_remainder(self, result: OrderResult, requested: str) -> OrderResult:
        dismissed = self.dismiss_ad_if_present()
        if dismissed:
            time.sleep(0.3)
            result = self.order_result()
            if not result.submission_success:
                raise UiError("Order result disappeared after ad dismissal before FOK cancel.")
        filled = self._decimal(result.filled_quantity)
        expected = self._decimal(requested)
        if filled is not None and expected is not None and filled >= expected:
            LOG.info("Fill-or-kill order was fully filled; no cancel is needed.")
            return result
        if result.cancel_node is None:
            LOG.info("No Cancel action is available; treating the result as terminal.")
            return result
        self.tap_node(result.cancel_node)
        time.sleep(1)
        self.confirm_cancel()
        if not self.handle_trade_password():
            time.sleep(3)
        killed = self.order_result()
        if not killed.canceled:
            raise UiError(
                f"FOK cancel was not confirmed; current status is {killed.status or '<none>'}."
            )
        LOG.info(
            "Fill-or-kill cancel confirmed; filled price=%s filled qty=%s.",
            killed.filled_price or "--",
            killed.filled_quantity or "0",
        )
        return killed

    def market_order(
        self,
        symbol: str,
        quantity: str,
        side: str,
        *,
        dry_run: bool = False,
        no_submit: bool = False,
    ) -> dict:
        symbol = self.normalize_symbol(symbol)
        quantity = self.positive_number(quantity, "quantity")
        side = self.normalize_side(side)
        self.adb.validate(strict_layout=True)
        LOG.info("Starting market order flow: %s %s %s.", side, quantity, symbol)
        attempts = 1 if dry_run or no_submit else 2
        for attempt in range(1, attempts + 1):
            home = self.ensure_home()
            self.open_symbol(symbol, home)
            ticket = self.open_ticket(side, market=True)
            if dry_run:
                if ticket.quantity is None or ticket.submit is None:
                    raise UiError("Dry run did not reach a complete market ticket.")
                return {"ok": True, "dryRun": True, "submitted": False}
            self.adb.set_text_at(598, 2015, quantity)
            if no_submit:
                return {"ok": True, "dryRun": False, "submitted": False}
            self._submit_market(quantity)
            result = self.wait_order_result()
            if result is not None:
                self.adb.keyevent("KEYCODE_BACK")
                time.sleep(0.7)
                return {
                    "ok": True,
                    "submitted": True,
                    "result": result.kind,
                    "status": result.status,
                    "filledPrice": result.filled_price,
                    "filledQty": result.filled_quantity,
                }
            LOG.warning("No confirmed market-order result detected on attempt %d.", attempt)
            self.adb.restart()
        raise UiError("Market order submission was not confirmed after two attempts.")

    def limit_order(
        self,
        symbol: str,
        price: str,
        quantity: str,
        side: str,
        *,
        dry_run: bool = False,
        no_submit: bool = False,
        kill: bool = False,
    ) -> dict:
        symbol = self.normalize_symbol(symbol)
        price = self.positive_number(price, "price")
        quantity = self.positive_number(quantity, "quantity")
        side = self.normalize_side(side)
        self.adb.validate(strict_layout=True)
        label = "fill-or-kill limit" if kill else "limit"
        LOG.info("Starting %s order flow: %s %s %s @ %s.", label, side, quantity, symbol, price)
        attempts = 1 if dry_run or no_submit else 2
        for attempt in range(1, attempts + 1):
            home = self.ensure_home()
            self.open_symbol(symbol, home)
            ticket = self.open_ticket(side)
            if dry_run:
                if ticket.price is None or ticket.quantity is None or ticket.submit is None:
                    raise UiError("Dry run did not reach a complete limit ticket.")
                return {"ok": True, "dryRun": True, "submitted": False, "kill": kill}
            self.adb.set_text_at(598, 1882, price)
            self.adb.set_text_at(598, 1294, quantity)
            if no_submit:
                return {"ok": True, "dryRun": False, "submitted": False, "kill": kill}
            self._submit_limit(price, quantity)
            result = self.wait_order_result()
            if result is not None:
                if kill:
                    result = self._kill_remainder(result, quantity)
                self.adb.keyevent("KEYCODE_BACK")
                time.sleep(0.7)
                return {
                    "ok": True,
                    "submitted": True,
                    "kill": kill,
                    "result": result.kind,
                    "status": result.status,
                    "filledPrice": result.filled_price,
                    "filledQty": result.filled_quantity,
                }
            LOG.warning("No confirmed limit-order result detected on attempt %d.", attempt)
            self.adb.restart()
        raise UiError("Limit order submission was not confirmed after two attempts.")
