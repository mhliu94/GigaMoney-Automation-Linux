"""Compact workflow outcomes and timings, shared by CLI and Kafka operations."""

from __future__ import annotations

import inspect
import json
import logging
import time
import traceback
from contextvars import ContextVar
from functools import wraps

LOG = logging.getLogger("gigamoney")
command_context: ContextVar[str | None] = ContextVar("command_id", default=None)
_workflow: ContextVar[Workflow | None] = ContextVar("workflow", default=None)


def error_detail(error: Exception | str, settings) -> str:
    detail = str(error)
    secrets = [settings.get(key, "") for key in ("gigamoney.tradePassword", "monitor.adminPassword")]
    secrets.extend(value for key, value in settings.get("kafka.clientConfig", {}).items() if "password" in key.lower())
    for value in secrets:
        secret = str(value or "")
        if secret:
            detail = detail.replace(secret, "<redacted>")
    return detail


class Workflow:
    def __init__(self, name, settings, details):
        self.settings = settings
        self.details = {"workflow": name, "command_id": command_context.get(), **details}
        self.started = time.perf_counter()
        self.attempt_started = self.started
        self.attempts = 0
        self.stage = "validate"

    def emit(self, event, level=logging.INFO, **fields):
        LOG.log(level, "Workflow %s %s", event, json.dumps({
            **self.details, "attempts": self.attempts, "stage": self.stage,
            "elapsed_s": round(time.perf_counter() - self.started, 3),
            "attempt_elapsed_s": round(time.perf_counter() - self.attempt_started, 3), **fields,
        }, separators=(",", ":"), default=str))

    def attempt(self, number, *, check_ads=None):
        self.attempts = number
        self.attempt_started = time.perf_counter()
        self.stage = "start_attempt"
        if check_ads is not None:
            self.details["ad_checks"] = check_ads
        self.emit("attempt")

    def retry(self, reason):
        self.emit(
            "retry", logging.WARNING,
            reason=error_detail(reason, self.settings) if isinstance(reason, Exception) else reason,
            attempt_elapsed_s=round(time.perf_counter() - self.attempt_started, 3),
            next_attempt=self.attempts + 1,
        )


def workflow_step(stage, **details):
    current = _workflow.get()
    if current is not None:
        current.stage = stage
        current.details.update(details)


def workflow_attempt(number, *, check_ads=None):
    current = _workflow.get()
    if current is not None:
        current.attempt(number, check_ads=check_ads)


def workflow_retry(reason):
    current = _workflow.get()
    if current is not None:
        current.retry(reason)


def logged_workflow(function):
    signature = inspect.signature(function)

    @wraps(function)
    def wrapped(self, *args, **kwargs):
        arguments = signature.bind(self, *args, **kwargs).arguments
        name = function.__name__
        if name == "query":
            name = "holdings"
        if name == "limit_order" and arguments.get("kill"):
            name = "fok_order"
        mode = "dry_run" if arguments.get("dry_run") else "staged" if arguments.get("no_submit") else "live"
        details = {key: arguments[key] for key in ("symbol", "side", "quantity", "price") if key in arguments}
        current = Workflow(name, self.settings, {"mode": mode, **details})
        token = _workflow.set(current)
        current.emit("started")
        try:
            result = function(self, *args, **kwargs)
        except Exception as error:
            current.emit("finished", logging.ERROR, outcome="failure",
                         error_type=type(error).__name__, error=error_detail(error, self.settings))
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("Workflow failure traceback: %s", error_detail(
                    "".join(traceback.format_exception(type(error), error, error.__traceback__)), self.settings,
                ))
            raise
        else:
            summary = {key: result[key] for key in (
                "submitted", "result", "status", "filledPrice", "filledQty", "remainingDetected", "orderId", "deduplicated",
            ) if key in result}
            if "positions" in result:
                summary["positions"] = len(result["positions"])
            if "canceledCount" in result:
                summary["cancel_requests"] = result["canceledCount"]
            current.emit("finished", outcome="success" if mode == "live" else mode, **summary)
            return result
        finally:
            _workflow.reset(token)

    return wrapped
