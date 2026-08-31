from __future__ import annotations

import json
import logging
import os
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .cancel_orders import CancelAutomation
from .config import Settings
from .holdings import HoldingsAutomation
from .lock import EmulatorLock
from .orders import OrderAutomation

LOG = logging.getLogger("gigamoney")


@dataclass(frozen=True)
class CommandPlan:
    kind: str
    name: str
    arguments: dict[str, Any]


def _positive(value: Any, name: str) -> str:
    return OrderAutomation.positive_number(str(value), name)


def command_plan(command: dict[str, Any]) -> CommandPlan:
    command_type = str(command.get("type", "")).strip().upper()
    if command_type == "MARKET_ORDER":
        if command.get("notional_usd") is not None and command.get("qty_shares") is not None:
            raise ValueError("Market order cannot contain both qty_shares and notional_usd.")
        if command.get("notional_usd") is not None:
            raise ValueError("GigaMoney market orders require qty_shares, not notional_usd.")
        if command.get("qty_shares") is None:
            raise ValueError("Market order requires qty_shares.")
        return CommandPlan("market", "market order", {
            "symbol": OrderAutomation.normalize_symbol(str(command.get("symbol", ""))),
            "quantity": _positive(command["qty_shares"], "qty_shares"),
            "side": OrderAutomation.normalize_side(str(command.get("side", ""))),
        })
    if command_type in {"LIMIT_ORDER", "LIMIT_ORDER_FOK"}:
        kill = (
            command_type == "LIMIT_ORDER_FOK"
            or str(command.get("time_in_force", "")).strip().upper() == "FOK"
            or command.get("cancel_unfilled") is True
        )
        return CommandPlan("limit", "fill-or-kill limit order" if kill else "limit order", {
            "symbol": OrderAutomation.normalize_symbol(str(command.get("symbol", ""))),
            "price": _positive(command.get("limit_price"), "limit_price"),
            "quantity": _positive(command.get("qty_shares"), "qty_shares"),
            "side": OrderAutomation.normalize_side(str(command.get("side", ""))),
            "kill": kill,
        })
    if command_type == "CANCEL_OPEN_ORDERS":
        return CommandPlan("cancel", "cancel all orders", {})
    if command_type == "SET_TRADING_ENABLED":
        value = command.get("trading_enabled")
        if isinstance(value, bool):
            enabled = value
        else:
            normalized = str(value).strip().casefold()
            if normalized in {"1", "true", "yes", "on", "enabled"}:
                enabled = True
            elif normalized in {"0", "false", "no", "off", "disabled"}:
                enabled = False
            else:
                raise ValueError(f"trading_enabled must be boolean: {value}")
        return CommandPlan("state", "trading status change", {"enabled": enabled})
    return CommandPlan("ignore", command_type, {})


def nullable_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--", "N/A"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    clean = re.sub(r"[^0-9.\-]", "", text)
    try:
        number = float(clean)
    except ValueError:
        return None
    return -abs(number) if negative else number


def holdings_snapshot(holdings: dict, settings: Settings, trading_enabled: bool) -> dict:
    cash_by_currency: dict[str, float] = {}
    for balance in holdings.get("cash", {}).get("balances", []) or []:
        currency = str(balance.get("currency", "")).strip().upper()
        amount = nullable_number(balance.get("balance"))
        if re.fullmatch(r"[A-Z]{3}", currency) and amount is not None:
            cash_by_currency[currency] = amount
    cash = cash_by_currency.get("USD")
    if cash is None:
        cash = nullable_number(holdings.get("cash", {}).get("totalUsd"))
    if cash is None:
        cash = nullable_number(holdings.get("portfolio", {}).get("overview", {}).get("cash"))
    raw_positions = holdings.get("positions")
    if raw_positions is None:
        raw_positions = holdings.get("portfolio", {}).get("positions", [])
    positions = []
    for position in raw_positions or []:
        symbol = str(position.get("symbol", "")).strip().upper()
        quantity = nullable_number(position.get("quantity"))
        if not symbol or quantity in {None, 0}:
            continue
        mapped: dict[str, Any] = {
            "symbol": symbol,
            "qty": quantity,
            "avg_price": nullable_number(position.get("cost")),
        }
        for name in ("marketPrice", "marketValue", "dailyPL", "dailyPLPercent"):
            value = nullable_number(position.get(name))
            if value is not None:
                mapped[name] = value
        positions.append(mapped)
    timestamp = datetime.now(timezone.utc).isoformat()
    queried = str(holdings.get("queriedAt", "")).strip()
    if queried:
        try:
            timestamp = datetime.fromisoformat(queried.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return {
        "account_id": settings.require("gigamoney.accountId"),
        "account_num_id": settings.get("gigamoney.accountNumId"),
        "cash": cash or 0.0,
        "cash_by_currency": cash_by_currency,
        "positions": positions,
        "ts": timestamp,
        "trading_enabled": trading_enabled,
    }


def command_is_stale(command: dict, maximum_age: float, *, now: datetime | None = None) -> bool:
    if maximum_age <= 0:
        return False
    value = str(command.get("ts", "")).strip()
    if not value:
        return True
    try:
        issued = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if issued.tzinfo is None:
            issued = issued.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return ((now or datetime.now(timezone.utc)) - issued.astimezone(timezone.utc)).total_seconds() > maximum_age


class TradingRunner:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.state_path = settings.work_dir / "trading-state.json"
        self.stop_path = settings.work_dir / "runner.stop"
        self.pid_path = settings.work_dir / "runner.pid.json"
        self.lock_path = settings.work_dir / "emulator.lock"
        self.lock_timeout = float(settings.get("automation.lockTimeoutSeconds", 300))
        self.running = True

    def _signal(self, signum, frame) -> None:
        LOG.info("Signal %s received; stopping after the current operation.", signum)
        self.running = False

    def _trading_enabled(self) -> bool:
        try:
            return bool(json.loads(self.state_path.read_text(encoding="utf-8"))["tradingEnabled"])
        except (OSError, ValueError, KeyError, TypeError):
            return bool(self.settings.get("gigamoney.tradingEnabledDefault", True))

    def _set_trading_enabled(self, enabled: bool) -> None:
        self.state_path.write_text(json.dumps({
            "tradingEnabled": enabled,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }) + "\n", encoding="utf-8")

    def _operation(self, kind: str, arguments: dict[str, Any]) -> dict:
        with EmulatorLock(self.lock_path, self.lock_timeout):
            if kind == "market":
                return OrderAutomation(self.settings).market_order(**arguments)
            if kind == "limit":
                return OrderAutomation(self.settings).limit_order(**arguments)
            if kind == "cancel":
                return CancelAutomation(self.settings).cancel_all()
            if kind == "holdings":
                return HoldingsAutomation(self.settings).query()
        raise ValueError(f"Unknown operation: {kind}")

    def _write_pid(self) -> None:
        record = {
            "pid": os.getpid(),
            "startedAt": datetime.now(timezone.utc).isoformat(),
            "config": str(self.settings.path),
        }
        run_log = os.environ.get("GIGAMONEY_RUN_LOG", "").strip()
        if run_log:
            record["log"] = run_log
        self.pid_path.write_text(json.dumps(record) + "\n", encoding="utf-8")

    @staticmethod
    def _publish_snapshot(producer, topic: str, snapshot: dict) -> tuple[int, int]:
        delivery: dict[str, Any] = {"reported": False, "error": None, "partition": -1, "offset": -1}

        def delivered(error, message) -> None:
            delivery["reported"] = True
            if error is not None:
                delivery["error"] = str(error)
                return
            delivery["partition"] = int(message.partition())
            delivery["offset"] = int(message.offset())

        producer.produce(
            topic,
            # Existing account-details publishers serialize their nominally
            # null string key as zero-length bytes. A true Kafka null key is
            # rejected by this topic with INVALID_RECORD.
            key="",
            value=json.dumps(snapshot, separators=(",", ":")),
            on_delivery=delivered,
        )
        remaining = int(producer.flush(10))
        if delivery["error"] is not None:
            raise RuntimeError(f"Kafka rejected the holdings snapshot: {delivery['error']}")
        if remaining:
            raise RuntimeError(
                f"Kafka did not deliver the holdings snapshot within 10 seconds; "
                f"{remaining} message(s) remain queued."
            )
        if not delivery["reported"]:
            raise RuntimeError("Kafka returned no delivery report for the holdings snapshot.")
        return int(delivery["partition"]), int(delivery["offset"])

    def run(self) -> None:
        try:
            from confluent_kafka import Consumer, KafkaException, Producer
        except ImportError as exc:
            raise RuntimeError(
                "Kafka support is not installed. Run: .venv/bin/pip install '.[kafka]'"
            ) from exc
        account_id = str(self.settings.require("gigamoney.accountId"))
        bootstrap = str(self.settings.require("kafka.bootstrapServers"))
        command_topic = str(self.settings.require("kafka.commandTopic"))
        details_topic = str(self.settings.require("kafka.accountDetailsTopic"))
        group = str(self.settings.require("kafka.consumerGroupId"))
        interval = float(self.settings.get("kafka.holdingsIntervalSeconds", 120))
        poll_seconds = float(self.settings.get("kafka.pollTimeoutSeconds", 1))
        maximum_age = float(self.settings.get("kafka.maxCommandAgeSeconds", 300))
        commit_failed = bool(self.settings.get("kafka.commitFailedCommands", True))
        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": str(self.settings.get("kafka.autoOffsetReset", "latest")),
            "max.poll.interval.ms": int(self.settings.get("kafka.maxPollIntervalMilliseconds", 900000)),
            "session.timeout.ms": 10000,
        })
        producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        })
        if self.stop_path.exists():
            self.stop_path.unlink()
        self._write_pid()
        signal.signal(signal.SIGTERM, self._signal)
        signal.signal(signal.SIGINT, self._signal)
        consumer.subscribe([command_topic])
        trading_enabled = self._trading_enabled()
        next_holdings = time.monotonic()
        LOG.info("Runner started for account_id=%s topic=%s.", account_id, command_topic)
        try:
            while self.running and not self.stop_path.exists():
                now = time.monotonic()
                if now >= next_holdings:
                    holdings_started = time.perf_counter()
                    try:
                        holdings = self._operation("holdings", {})
                        LOG.info(
                            "Holdings query completed in %.3f seconds.",
                            time.perf_counter() - holdings_started,
                        )
                        snapshot = holdings_snapshot(holdings, self.settings, trading_enabled)
                        partition, offset = self._publish_snapshot(producer, details_topic, snapshot)
                        LOG.info(
                            "Published holdings with %d positions to partition=%d offset=%d.",
                            len(snapshot["positions"]), partition, offset,
                        )
                    except Exception:
                        LOG.exception(
                            "Holdings publication failed after %.3f seconds.",
                            time.perf_counter() - holdings_started,
                        )
                    next_holdings += interval
                    while next_holdings <= time.monotonic():
                        next_holdings += interval
                message = consumer.poll(poll_seconds)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                command_id = "<unparsed>"
                try:
                    command = json.loads(message.value().decode("utf-8"))
                    command_id = str(command.get("command_id") or "<missing>")
                    if str(command.get("account_id", "")).strip() != account_id:
                        consumer.commit(message=message, asynchronous=False)
                        continue
                    if command_is_stale(command, maximum_age):
                        LOG.warning("Ignoring stale command command_id=%s.", command_id)
                        consumer.commit(message=message, asynchronous=False)
                        continue
                    plan = command_plan(command)
                    if plan.kind == "ignore":
                        LOG.info("Ignoring unsupported command type=%s.", command.get("type"))
                    elif plan.kind == "state":
                        trading_enabled = bool(plan.arguments["enabled"])
                        self._set_trading_enabled(trading_enabled)
                    elif plan.kind != "cancel" and not trading_enabled:
                        LOG.warning("Ignoring command_id=%s because trading is disabled.", command_id)
                    else:
                        LOG.info("Executing %s command_id=%s.", plan.name, command_id)
                        self._operation(plan.kind, plan.arguments)
                    consumer.commit(message=message, asynchronous=False)
                except Exception:
                    LOG.exception("Command processing failed command_id=%s.", command_id)
                    if not commit_failed:
                        raise
                    consumer.commit(message=message, asynchronous=False)
                    LOG.warning("Committed failed command to prevent unsafe replay.")
        finally:
            consumer.close()
            producer.flush(5)
            try:
                record = json.loads(self.pid_path.read_text(encoding="utf-8"))
                if int(record.get("pid", -1)) == os.getpid():
                    self.pid_path.unlink(missing_ok=True)
                    self.stop_path.unlink(missing_ok=True)
            except (OSError, ValueError, TypeError):
                pass
            LOG.info("Runner stopped.")


def runner_status(settings: Settings) -> dict:
    path = settings.work_dir / "runner.pid.json"
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
        pid = int(record["pid"])
    except (OSError, ValueError, KeyError, TypeError):
        return {"running": False}
    try:
        os.kill(pid, 0)
    except OSError:
        return {"running": False, "stalePid": pid}
    command = ""
    try:
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
    except OSError:
        pass
    if "gigamoney" not in command.casefold():
        return {"running": False, "stalePid": pid}
    return {"running": True, **record}


def request_runner_stop(settings: Settings, *, force: bool = False) -> dict:
    status = runner_status(settings)
    if not status.get("running"):
        return {"requested": False, "reason": "runner is not running"}
    stop = settings.work_dir / "runner.stop"
    stop.write_text(datetime.now(timezone.utc).isoformat() + "\n", encoding="utf-8")
    if force:
        os.kill(int(status["pid"]), signal.SIGTERM)
    return {"requested": True, "pid": status["pid"], "forced": force}
