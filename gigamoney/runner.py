from __future__ import annotations

from gigamoney import latency
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

from .holdings_timer import HoldingsTimer, PositionProjection
from .cancel_orders import CancelAutomation
from .config import Settings, execution_backend
from .diagnostics import command_context, error_detail
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
    if command_type in {"LIMIT_ORDER", "LIMIT_ORDER_IOC", "LIMIT_ORDER_FOK"}:
        kill = (
            command_type in {"LIMIT_ORDER_IOC", "LIMIT_ORDER_FOK"}
            or str(command.get("time_in_force", "")).strip().upper() in {"IOC", "FOK"}
            or command.get("cancel_unfilled") is True
        )
        return CommandPlan("limit", "immediate-or-cancel limit order" if kill else "limit order", {
            "symbol": OrderAutomation.normalize_symbol(str(command.get("symbol", ""))),
            "price": _positive(command.get("limit_price"), "limit_price"),
            "quantity": _positive(command.get("qty_shares"), "qty_shares"),
            "side": OrderAutomation.normalize_side(str(command.get("side", ""))),
            "kill": kill,
        })
    if command_type == "CANCEL_ORDER":
        identifier = command.get("order_id")
        if (not isinstance(identifier, str) or not identifier or identifier != identifier.strip()
                or len(identifier) > 256 or any(ord(c) < 32 for c in identifier)):
            raise ValueError("CANCEL_ORDER requires an exact broker order_id.")
        return CommandPlan("cancel", "cancel one order", {"identifier": identifier})
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
        "available_cash_by_currency": {"USD": cash_by_currency["USD"]} if "USD" in cash_by_currency else {},
        "cash_basis": "GigaMoney native USD balance after frozen buys",
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
        self.backend = execution_backend(settings)
        self.api_executor = None
        self.order_publisher = None
        self.order_monitor = None
        self.state_path = settings.work_dir / "trading-state.json"
        self.stop_path = settings.work_dir / "runner.stop"
        self.pid_path = settings.work_dir / "runner.pid.json"
        self.lock_path = settings.work_dir / "emulator.lock"
        self.lock_timeout = float(settings.get("automation.lockTimeoutSeconds", 300))
        self.running = True
        from .account_delta import AccountDeltaPublisher
        self._account_deltas = AccountDeltaPublisher(clock=lambda: time.monotonic())
        self.holdings = None
        self.holdings_timer = None
        self._first_checkpoint = True
        self._needs_recovery = False

    def _make_order_monitor(self, producer):
        from .delta_order_monitor import DeltaOrderMonitor
        return DeltaOrderMonitor(self, producer)

    def _observe_positions(self, snapshots):
        if self.holdings is not None:
            try:
                self.holdings.observe(snapshots)
            except Exception as error:
                LOG.error("Position delta delivery deferred error_type=%s", type(error).__name__)

    def _publish_account_update(self, producer, topic, snapshot, full=False):
        return self._account_deltas.publish(snapshot, lambda event:
            self._publish_snapshot(producer, topic, event, key=snapshot['account_id'],
                timeout=float(self.settings.get('kafka.deliveryTimeoutSeconds', 10))),
            force=full, automatic_checkpoint=False) or (-1, -1)

    def _holdings_tick(self, producer):
        from .api import AuthenticationRequired
        token = self.holdings.begin()
        try:
            result = self._operation("holdings", {})
            if self.backend == "api" and self.order_monitor is None:
                try:
                    self._publish_orders(producer, result.get("orders", []))
                except Exception as error:
                    LOG.error("Order status refresh failed error_type=%s", type(error).__name__)
            self.holdings.checkpoint(token, holdings_snapshot(result, self.settings, self._trading_enabled()))
            if self.backend == "api" and self.running:
                (self.settings.work_dir / "api/authentication.json").unlink(missing_ok=True)
            LOG.info("Published timer holdings checkpoint.")
        except AuthenticationRequired as error:
            self._authentication_required(error)
            return
        except Exception as error:
            LOG.error("Holdings publication failed error_type=%s", type(error).__name__)
            if self.backend == "api" and self.order_monitor is None:
                try:
                    self._publish_orders(producer, [])
                except Exception as delivery_error:
                    LOG.error("Order status retry deferred error_type=%s", type(delivery_error).__name__)
            raise
        finally:
            # Periodic recovery keeps ambiguous orders protected independently
            # of fill and holdings publication. Never replay a broker write.
            scheduled_recovery = not self._first_checkpoint
            self._first_checkpoint = False
            if scheduled_recovery and self.backend == "api" and self.running:
                try:
                    self._operation("recover", {})
                except AuthenticationRequired as error:
                    self._authentication_required(error)
                except Exception as error:
                    LOG.warning("Periodic order recovery deferred error_type=%s", type(error).__name__)

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
        if self.backend == "api":
            from .api_trading import ApiTrading
            if self.api_executor is None:
                self.api_executor = ApiTrading(self.settings)
            method = {"market": "market_order", "limit": "limit_order", "cancel": "cancel_all",
                      "holdings": "query", "recover": "recover_pending", "reject": "record_rejection"}.get(kind)
            if kind == "cancel" and "identifier" in arguments:
                method = "cancel_order"
            if method is None:
                raise ValueError(f"Unknown operation: {kind}")
            if kind == "holdings" and self.order_monitor is not None:
                arguments = dict(arguments, include_orders=False)
            return getattr(self.api_executor, method)(**arguments)
        lock_started = time.perf_counter()
        with EmulatorLock(self.lock_path, self.lock_timeout):
            lock_wait = time.perf_counter() - lock_started
            if lock_wait >= 1:
                LOG.info("Emulator lock acquired operation=%s wait_s=%.3f command_id=%s.",
                         kind, lock_wait, command_context.get())
            if kind == "market":
                return OrderAutomation(self.settings).market_order(**arguments)
            if kind == "limit":
                return OrderAutomation(self.settings).limit_order(**arguments)
            if kind == "cancel":
                if "identifier" in arguments:
                    raise ValueError("Exact cancellation requires the API backend; bulk fallback is forbidden.")
                return CancelAutomation(self.settings).cancel_all()
            if kind == "holdings":
                return HoldingsAutomation(self.settings).query()
        raise ValueError(f"Unknown operation: {kind}")

    def _write_pid(self) -> None:
        record = {
            "pid": os.getpid(),
            "startedAt": datetime.now(timezone.utc).isoformat(),
            "config": str(self.settings.path),
            "backend": self.backend,
        }
        run_log = os.environ.get("GIGAMONEY_RUN_LOG", "").strip()
        if run_log:
            record["log"] = run_log
        self.pid_path.write_text(json.dumps(record) + "\n", encoding="utf-8")

    @staticmethod
    def _publish_snapshot(producer, topic: str, snapshot: dict, *, timeout: float = 10,
                          key: str = "", description: str = "holdings snapshot") -> tuple[int, int]:
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
            key=key,
            value=json.dumps(snapshot, separators=(",", ":"), allow_nan=False),
            on_delivery=delivered,
        )
        remaining = int(producer.flush(timeout))
        if delivery["error"] is not None:
            raise RuntimeError(f"Kafka rejected the {description}: {delivery['error']}")
        if remaining:
            raise RuntimeError(
                f"Kafka did not deliver the {description} within {timeout:g} seconds; "
                f"{remaining} message(s) remain queued."
            )
        if not delivery["reported"]:
            raise RuntimeError(f"Kafka returned no delivery report for the {description}.")
        return int(delivery["partition"]), int(delivery["offset"])

    def _publish_orders(self, producer, snapshots):
        self._observe_positions(snapshots)
        from .order_status import OrderStatusPublisher
        if self.api_executor is not None:
            observed = {snapshot["order_id"] for snapshot in snapshots}
            snapshots = list(snapshots) + [snapshot for snapshot in self.api_executor.rejection_snapshots()
                                           if snapshot["order_id"] not in observed]
        if self.order_publisher is None:
            self.order_publisher = OrderStatusPublisher(self.settings)
        topic = str(self.settings.get("kafka.orderStatusTopic", "order-status")).strip()
        if not topic:
            raise ValueError("kafka.orderStatusTopic must not be empty.")

        def deliver(snapshot):
            key = json.dumps([snapshot["account_id"], snapshot["order_id"]], separators=(",", ":"))
            if len(json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode()) > 65536:
                raise ValueError("Order snapshot exceeds KTrader's 64 KiB limit.")
            return self._publish_snapshot(producer, topic, snapshot, key=key, description="order snapshot",
                                          timeout=float(self.settings.get("kafka.deliveryTimeoutSeconds", 10)))

        return self.order_publisher.publish(snapshots, deliver)

    def _refuse_without_submission(self, producer, command, command_id, plan, reason):
        """Close a valid order handoff durably even when intake refuses to execute it."""
        if self.backend != "api" or plan.kind not in {"market", "limit"}:
            return
        try:
            rejection = self._operation("reject", {
                "command_id": command_id, "arguments": plan.arguments,
                "reason": reason, "created_at": command.get("ts"),
            })
        except ValueError:
            # A reused ID must not overwrite evidence for a different/possibly accepted order.
            LOG.error("Command refusal could not be correlated command_id=%s; existing evidence preserved.", command_id)
            return
        if rejection is not None:
            try:
                self._publish_orders(producer, [rejection])
            except Exception as error:
                LOG.error("Command rejection publication deferred command_id=%s error_type=%s; durable rejection retained.",
                          command_id, type(error).__name__)

    def run(self) -> None:
        # Prevent an Android and API runner (or two API runners) from consuming
        # simultaneously and overwriting each other's PID/command state.
        with EmulatorLock(self.settings.work_dir / "runner.lock", 0):
            from .api import AuthenticationRequired
            try:
                if self.backend == "api":
                    self._operation("recover", {})
                    if self.api_executor is not None:
                        self.api_executor.price_books.start()
                self._run_kafka()
            except AuthenticationRequired as error:
                self._authentication_required(error)
            finally:
                if self.api_executor is not None:
                    self.api_executor.price_books.close()

    def _authentication_required(self, error) -> None:
        from .api import save_session
        directory = self.settings.work_dir / "api"
        save_session(directory / "authentication.json", {
            "state": "authentication_required", "account_id": self.settings.get("gigamoney.accountId"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "message": str(error),
        })
        self.running = False
        LOG.error("Runner stopped intake: authentication_required. %s", error)

    def _run_kafka(self) -> None:
        from .api import AuthenticationRequired
        from .api_trading import OrderUncertain
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
        interval = float(self.settings.get("kafka.holdingsIntervalSeconds", 30))
        poll_seconds = float(self.settings.get("kafka.pollTimeoutSeconds", 1))
        maximum_age = float(self.settings.get("kafka.maxCommandAgeSeconds", 300))
        commit_failed = bool(self.settings.get("kafka.commitFailedCommands", True))
        consumer = Consumer({
            **self.settings.kafka_client_config,
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": str(self.settings.get("kafka.autoOffsetReset", "latest")),
            "max.poll.interval.ms": int(self.settings.get("kafka.maxPollIntervalMilliseconds", 900000)),
            "session.timeout.ms": int(self.settings.get("kafka.sessionTimeoutMilliseconds", 10000)),
        })
        producer = Producer({
            **self.settings.kafka_client_config,
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": int(self.settings.get("kafka.producerLingerMilliseconds", 5)),
        })
        if self.stop_path.exists():
            self.stop_path.unlink()
        self._write_pid()
        signal.signal(signal.SIGTERM, self._signal)
        signal.signal(signal.SIGINT, self._signal)
        consumer.subscribe([command_topic])
        trading_enabled = self._trading_enabled()
        self.holdings = PositionProjection(lambda state, full:
            self._publish_account_update(producer, details_topic, state, full))
        self.holdings_timer = HoldingsTimer(lambda: self._holdings_tick(producer), interval)
        LOG.info("Runner started for account_id=%s topic=%s backend=%s.", account_id, command_topic, self.backend)
        try:
            if self.backend == 'api':
                from .delta_order_monitor import DeltaOrderMonitor
                order_producer = Producer({**self.settings.kafka_client_config,
                    'bootstrap.servers': bootstrap, 'acks': 'all', 'enable.idempotence': True,
                    'delivery.timeout.ms': 10000, 'request.timeout.ms': 5000})
                self.order_monitor = self._make_order_monitor(order_producer)
                if self.order_monitor is not None:
                    self.order_monitor.start()
            self.holdings_timer.start(wait_initial=True)
            while self.running and not self.stop_path.exists():
                if self._needs_recovery:
                    try:
                        self._operation("recover", {})
                        self._needs_recovery = False
                    except AuthenticationRequired as error:
                        self._authentication_required(error)
                        break
                    except Exception as error:
                        LOG.warning("Order recovery deferred error_type=%s", type(error).__name__)
                message = consumer.poll(poll_seconds)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                command_id = "<unparsed>"
                command_type = "<unparsed>"
                received_ms = latency.now_ms()
                command_started = time.perf_counter()
                command_stage = "decode"
                command_outcome = "success"
                command_reason = None
                committed = False
                log_command = True
                command_token = None
                plan = None
                try:
                    command = json.loads(message.value().decode("utf-8"))
                    command_id = str(command.get("command_id") or "<missing>")
                    if self.backend == "api" and command_id == "<missing>":
                        command_id = f"kafka:{command_topic}:{message.partition()}:{message.offset()}"
                    command_type = str(command.get("type", ""))
                    command_token = command_context.set(command_id)
                    command_stage = "validate"
                    if str(command.get("account_id", "")).strip() != account_id:
                        log_command = False
                        consumer.commit(message=message, asynchronous=False)
                        continue
                    latency.received(command, message, received_ms)
                    if command_is_stale(command, maximum_age):
                        command_outcome, command_reason = "ignored", "stale"
                        plan = command_plan(command)
                        command_stage = "persist_refusal"
                        self._refuse_without_submission(producer, command, command_id, plan,
                                                        "Command expired before execution")
                        command_stage = "commit"
                        consumer.commit(message=message, asynchronous=False)
                        committed = True
                        continue
                    plan = command_plan(command)
                    if plan.kind == "ignore":
                        command_outcome, command_reason = "ignored", "unsupported_type"
                    elif plan.kind == "state":
                        command_stage = "persist_trading_state"
                        trading_enabled = bool(plan.arguments["enabled"])
                        self._set_trading_enabled(trading_enabled)
                        LOG.info("Trading state changed command_id=%s trading_enabled=%s.",
                                 command_id, trading_enabled)
                    elif plan.kind != "cancel" and not trading_enabled:
                        command_outcome, command_reason = "ignored", "trading_disabled"
                        command_stage = "persist_refusal"
                        self._refuse_without_submission(producer, command, command_id, plan,
                                                        "Trading was disabled before execution")
                    else:
                        command_stage = "execute"
                        LOG.info("Executing %s command_id=%s.", plan.name, command_id)
                        with latency.span("command_execution", plan.kind, command):
                            self._operation(plan.kind, plan.arguments)
                    command_stage = "commit"
                    consumer.commit(message=message, asynchronous=False)
                    committed = True
                except Exception as error:
                    if command_stage == "persist_refusal":
                        # No financial request was attempted. Preserve Kafka redelivery if the
                        # refusal itself could not be journaled; never commit a silent discard.
                        raise
                    if self.backend == "api" and (isinstance(error, OrderUncertain) or getattr(error, "submitted", False)):
                        self._needs_recovery = True
                    log_command = True
                    command_outcome = "failure"
                    command_reason = type(error).__name__
                    LOG.error("Command processing failed command_id=%s stage=%s error_type=%s error=%r.",
                              command_id, command_stage, type(error).__name__, error_detail(error, self.settings))
                    if (self.backend == "api" and command_stage == "execute" and plan is not None
                            and plan.kind in {"market", "limit"}
                            and not isinstance(error, AuthenticationRequired)
                            and not getattr(error, "submitted", False)):
                        # Persist before committing Kafka. A reporting outage must
                        # not replay a financial command; holdings refreshes and
                        # restarts retry this exact terminal evidence from disk.
                        try:
                            rejection = self._operation("reject", {
                                "command_id": command_id, "arguments": plan.arguments,
                                "reason": error_detail(error, self.settings), "created_at": command.get("ts"),
                            })
                        except ValueError:
                            # Reusing an existing ID with a conflicting intent
                            # must not overwrite its evidence or restart intake.
                            rejection = None
                            LOG.error("Command rejection could not be correlated command_id=%s; "
                                      "existing order evidence preserved.", command_id)
                        if rejection is not None:
                            try:
                                self._publish_orders(producer, [rejection])
                            except Exception as reporting_error:
                                LOG.error("Command rejection publication deferred command_id=%s error_type=%s; "
                                          "durable rejection will retry on the next refresh.",
                                          command_id, type(reporting_error).__name__)
                    if isinstance(error, AuthenticationRequired):
                        # Authentication failed before submission. Close without
                        # committing this message; a human can restore the session
                        # and restart. Existing age checks still apply on redelivery.
                        self._authentication_required(error)
                    elif not commit_failed:
                        raise
                    else:
                        consumer.commit(message=message, asynchronous=False)
                        committed = True
                        LOG.warning("Committed failed command to prevent unsafe replay. command_id=%s", command_id)
                finally:
                    if log_command:
                        LOG.info("Command finished %s", json.dumps({
                            "command_id": command_id, "type": command_type, "outcome": command_outcome,
                            "reason": command_reason, "stage": command_stage, "committed": committed,
                            "elapsed_s": round(time.perf_counter() - command_started, 3),
                        }, separators=(",", ":")))
                    if command_token is not None:
                        command_context.reset(command_token)
        finally:
            self.holdings_timer.close()
            if self.order_monitor is not None:
                self.order_monitor.close()
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
        authentication = settings.work_dir / "api/authentication.json"
        try:
            state = json.loads(authentication.read_text())
            if state.get("account_id") == settings.get("gigamoney.accountId"):
                return {"running": False, **state}
        except (OSError, ValueError):
            pass
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
