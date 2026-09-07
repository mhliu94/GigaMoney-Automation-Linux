from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation

from .automation import Automation, OrderResult, TicketNodes
from .diagnostics import logged_workflow, workflow_attempt, workflow_retry, workflow_step
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

    def _submit_limit(self, price: str, quantity: str, *, check_ads: bool = True) -> None:
        dismissed = self.dismiss_ad_if_present() if check_ads else 0
        if dismissed:
            time.sleep(0.3)
            ticket = self._restore_limit(price, quantity)
            self.tap_node(ticket.submit)  # type: ignore[arg-type]
        else:
            self.adb.tap(734, 1540)

    def _submit_market(self, quantity: str, *, check_ads: bool = True) -> None:
        dismissed = self.dismiss_ad_if_present() if check_ads else 0
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

    def _kill_remainder(
        self, result: OrderResult, requested: str, *, check_ads: bool = True,
    ) -> OrderResult:
        dismissed = self.dismiss_ad_if_present() if check_ads else 0
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
        self.confirm_cancel(check_ads=check_ads)
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

    @logged_workflow
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
        attempts = 1 if dry_run or no_submit else 2
        for attempt in range(1, attempts + 1):
            check_ads = attempt > 1
            workflow_attempt(attempt, check_ads=check_ads)
            try:
                workflow_step("home")
                home = self.ensure_home(check_ads=check_ads)
                workflow_step("find_symbol")
                self.open_symbol(symbol, home)
                workflow_step("open_ticket")
                ticket = self.open_ticket(side, market=True)
            except UiError as error:
                if attempt == attempts:
                    raise
                workflow_retry(error)
                workflow_step("restart")
                self.adb.restart()
                continue
            if dry_run:
                if ticket.quantity is None or ticket.submit is None:
                    raise UiError("Dry run did not reach a complete market ticket.")
                return {"ok": True, "dryRun": True, "submitted": False}
            workflow_step("enter_quantity")
            self.adb.set_text_at(598, 2015, quantity)
            if no_submit:
                return {"ok": True, "dryRun": False, "submitted": False}
            workflow_step("submit")
            self._submit_market(quantity, check_ads=check_ads)
            workflow_step("confirm_result", submit_tapped=True)
            result = self.wait_order_result()
            if result is not None:
                workflow_step("leave_result", submission_confirmed=True, status=result.status,
                              filledQty=result.filled_quantity)
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
            if attempt < attempts:
                workflow_retry("Order submission was not confirmed")
            workflow_step("restart")
            self.adb.restart()
        workflow_step("confirm_result")
        raise UiError("Market order submission was not confirmed after two attempts.")

    @logged_workflow
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
        attempts = 1 if dry_run or no_submit else 2
        for attempt in range(1, attempts + 1):
            check_ads = attempt > 1
            workflow_attempt(attempt, check_ads=check_ads)
            try:
                workflow_step("home")
                home = self.ensure_home(check_ads=check_ads)
                workflow_step("find_symbol")
                self.open_symbol(symbol, home)
                workflow_step("open_ticket")
                ticket = self.open_ticket(side)
            except UiError as error:
                if attempt == attempts:
                    raise
                workflow_retry(error)
                workflow_step("restart")
                self.adb.restart()
                continue
            if dry_run:
                if ticket.price is None or ticket.quantity is None or ticket.submit is None:
                    raise UiError("Dry run did not reach a complete limit ticket.")
                return {"ok": True, "dryRun": True, "submitted": False, "kill": kill}
            workflow_step("enter_price")
            self.adb.set_text_at(598, 1882, price)
            workflow_step("enter_quantity")
            self.adb.set_text_at(598, 1294, quantity)
            if no_submit:
                return {"ok": True, "dryRun": False, "submitted": False, "kill": kill}
            workflow_step("submit")
            self._submit_limit(price, quantity, check_ads=check_ads)
            workflow_step("confirm_result", submit_tapped=True)
            result = self.wait_order_result()
            if result is not None:
                workflow_step("leave_result", submission_confirmed=True, status=result.status,
                              filledQty=result.filled_quantity)
                if kill:
                    workflow_step("fok_cancel")
                    result = self._kill_remainder(result, quantity, check_ads=check_ads)
                workflow_step("leave_result", status=result.status, filledQty=result.filled_quantity)
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
            if attempt < attempts:
                workflow_retry("Order submission was not confirmed")
            workflow_step("restart")
            self.adb.restart()
        workflow_step("confirm_result")
        raise UiError("Limit order submission was not confirmed after two attempts.")
