"""Local Linux command center for the GigaMoney runner and Android emulator."""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlparse

from .config import Settings, execution_backend, find_adb, load_settings
from .lock import EmulatorLock
from .runner import request_runner_stop, runner_status


LOG = logging.getLogger("gigamoney.monitor")
WEB_ROOT = Path(__file__).resolve().parent / "monitor_web"
HOLDINGS_SAMPLE_SIZE = 10
HOLDINGS_QUERY_PATTERN = re.compile(
    r"(?m)^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\s+"
    r".*?Holdings query completed in (?P<seconds>\d+(?:\.\d+)?) seconds\."
)
HOLDINGS_PUBLISHED_PATTERN = re.compile(
    r"(?m)^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\s+"
    r".*?Published holdings with \d+ positions to partition="
)
RESTART_CHECK_INTERVAL_SECONDS = 30
RESTART_MIN_INTERVAL_SECONDS = 60 * 60
RESTART_WINDOW_START_MINUTE = 20 * 60 + 1
RESTART_WINDOW_END_MINUTE = 21 * 60
RUNNER_STOP_TIMEOUT_SECONDS = 6 * 60
RUNNER_STATE_POLL_SECONDS = 0.5
EMULATOR_STOP_SETTLE_SECONDS = 60
EMULATOR_BOOT_TIMEOUT_SECONDS = 5 * 60
EMULATOR_BOOT_POLL_SECONDS = 5
GIGAMONEY_FOREGROUND_MAX_TRIES = 5
GIGAMONEY_FOREGROUND_RETRY_SECONDS = 5
ADMIN_USERNAME = "admin"
SESSION_COOKIE_NAME = "__Host-GigaMoneySession"
SESSION_LIFETIME_SECONDS = 8 * 60 * 60
LOGIN_WINDOW_SECONDS = 5 * 60
LOGIN_LOCKOUT_SECONDS = 15 * 60
MAX_LOGIN_FAILURES = 5
LEVEL_RANK = {"healthy": 0, "under_load": 1, "restart_recommended": 2}
LEVEL_LABELS = {
    "healthy": "Healthy",
    "under_load": "Under load",
    "restart_recommended": "Restart recommended",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _month_weekday(year: int, month: int, weekday: int, occurrence: int) -> int:
    first_weekday = datetime(year, month, 1).weekday()
    return 1 + (weekday - first_weekday) % 7 + (occurrence - 1) * 7


def eastern_time(value: datetime) -> datetime:
    """Convert an aware datetime to US Eastern time without external tzdata."""
    utc_value = value.astimezone(timezone.utc)
    year = utc_value.year
    daylight_start = datetime(
        year, 3, _month_weekday(year, 3, 6, 2), 7, tzinfo=timezone.utc,
    )
    daylight_end = datetime(
        year, 11, _month_weekday(year, 11, 6, 1), 6, tzinfo=timezone.utc,
    )
    is_daylight = daylight_start <= utc_value < daylight_end
    offset = timedelta(hours=-4 if is_daylight else -5)
    return utc_value.astimezone(timezone(offset, "EDT" if is_daylight else "EST"))


def in_scheduled_restart_window(value: datetime, settings: Settings | None = None) -> bool:
    if settings is not None:
        from zoneinfo import ZoneInfo
        if not settings.get("monitor.restart.enabled", True):
            return False
        local = value.astimezone(ZoneInfo(settings.get("monitor.restart.timezone", "America/New_York")))
        start, end = [sum(int(part) * factor for part, factor in zip(
            settings.get("monitor.restart." + key, default).split(":"), (60, 1)))
            for key, default in (("start", "20:01"), ("end", "21:00"))]
        minute = local.hour * 60 + local.minute
        return start <= minute <= end if start <= end else minute >= start or minute <= end
    local = eastern_time(value)
    local_minute = local.hour * 60 + local.minute
    return RESTART_WINDOW_START_MINUTE <= local_minute <= RESTART_WINDOW_END_MINUTE


def verify_admin_credentials(username: Any, password: Any, configured_password: Any,
                             configured_username: str = ADMIN_USERNAME) -> bool:
    supplied_username = username if isinstance(username, str) else ""
    supplied_password = password if isinstance(password, str) else ""
    expected_password = configured_password if isinstance(configured_password, str) else ""
    if (
        len(supplied_username) > 128
        or len(supplied_password) > 256
        or len(expected_password) > 256
    ):
        supplied_password = ""
        expected_password = ""
    return bool(expected_password) and hmac.compare_digest(
        supplied_username.encode("utf-8", errors="replace"), configured_username.encode("utf-8")
    ) and hmac.compare_digest(
        supplied_password.encode("utf-8", errors="replace"),
        expected_password.encode("utf-8", errors="replace"),
    )


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def process_probe(pid: int) -> dict[str, Any]:
    """Check a Linux PID and report its command line and process start time."""
    if pid <= 0:
        return {"running": False, "started_epoch": None, "command": ""}
    try:
        os.kill(pid, 0)
        stat_fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(
            "utf-8", errors="replace"
        )
        boot_match = re.search(
            r"(?m)^btime\s+(\d+)$", Path("/proc/stat").read_text(encoding="ascii")
        )
        started_epoch = None
        if len(stat_fields) > 21 and boot_match:
            started_epoch = int(boot_match.group(1)) + int(stat_fields[21]) / os.sysconf("SC_CLK_TCK")
        return {"running": True, "started_epoch": started_epoch, "command": command.strip()}
    except (OSError, ValueError, IndexError):
        return {"running": False, "started_epoch": None, "command": ""}


def _read_cpu_totals() -> tuple[int, int] | None:
    try:
        fields = Path("/proc/stat").read_text(encoding="ascii").splitlines()[0].split()
        values = [int(value) for value in fields[1:]]
    except (OSError, ValueError, IndexError):
        return None
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    return idle, sum(values)


def sample_machine_resources(sample_seconds: float = 0.12) -> dict[str, float | int | None]:
    """Sample Linux CPU and memory directly from procfs without another dependency."""
    before = _read_cpu_totals()
    time.sleep(max(0.02, sample_seconds))
    after = _read_cpu_totals()
    cpu_percent = None
    if before and after:
        idle_delta = after[0] - before[0]
        total_delta = after[1] - before[1]
        if total_delta > 0:
            cpu_percent = round(max(0.0, min(100.0, 100 * (total_delta - idle_delta) / total_delta)), 1)

    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            name, raw = line.split(":", 1)
            match = re.search(r"\d+", raw)
            if match:
                values[name] = int(match.group()) * 1024
    except (OSError, ValueError):
        pass
    total = values.get("MemTotal")
    available = values.get("MemAvailable")
    percent = round(100 * (total - available) / total, 1) if total and available is not None else None
    return {
        "cpu_percent": cpu_percent,
        "memory_percent": percent,
        "memory_total_bytes": total,
        "memory_available_bytes": available,
    }


def format_bytes(value: int | float | None) -> str:
    if not isinstance(value, (int, float)):
        return "Unavailable"
    amount = max(0.0, float(value))
    return f"{amount / 1024**3:.1f} GB" if amount >= 1024**3 else f"{amount / 1024**2:.0f} MB"


def parse_android_memory_summary(output: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    patterns = {
        "total_bytes": r"(?m)^\s*Total RAM:\s*([\d,]+)K",
        "free_reclaimable_bytes": r"(?m)^\s*Free RAM:\s*([\d,]+)K",
        "used_bytes": r"(?m)^\s*Used RAM:\s*([\d,]+)K",
        "lost_bytes": r"(?m)^\s*Lost RAM:\s*([\d,]+)K",
    }
    for name, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            values[name] = int(match.group(1).replace(",", "")) * 1024
    status = re.search(r"(?m)^\s*Total RAM:.*?\(status\s+([^)]+)\)", output, re.IGNORECASE)
    values["status"] = status.group(1).strip().lower() if status else None
    zram = re.search(
        r"(?m)^\s*ZRAM:\s*([\d,]+)K physical used for\s*([\d,]+)K in swap\s*"
        r"\(([\d,]+)K total swap\)",
        output,
        re.IGNORECASE,
    )
    if zram:
        values["zram_physical_bytes"] = int(zram.group(1).replace(",", "")) * 1024
        values["swap_used_bytes"] = int(zram.group(2).replace(",", "")) * 1024
        values["swap_total_bytes"] = int(zram.group(3).replace(",", "")) * 1024
    return values


def parse_android_app_meminfo(output: str) -> dict[str, int]:
    match = re.search(
        r"TOTAL PSS:\s*([\d,]+)\s+TOTAL RSS:\s*([\d,]+)\s+TOTAL SWAP \(KB\):\s*([\d,]+)",
        output,
        re.IGNORECASE,
    )
    if not match:
        return {}
    return {
        "app_pss_bytes": int(match.group(1).replace(",", "")) * 1024,
        "app_rss_bytes": int(match.group(2).replace(",", "")) * 1024,
        "app_swap_bytes": int(match.group(3).replace(",", "")) * 1024,
    }


def run_command(arguments: Sequence[str], *, cwd: Path, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(arguments), cwd=str(cwd), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout, check=False,
    )


@dataclass(frozen=True)
class ActionResult:
    ok: bool
    message: str


class GigaMoneyController:
    """Status collection and the four command-center control actions."""

    def __init__(
        self,
        settings: Settings,
        *,
        probe: Callable[[int], dict[str, Any]] = process_probe,
        runner: Callable[..., subprocess.CompletedProcess[str]] = run_command,
        machine_sampler: Callable[[], dict[str, float | int | None]] = sample_machine_resources,
        popen: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
    ) -> None:
        self.settings = settings
        self.root = settings.root.resolve()
        self.probe = probe
        self.runner = runner
        self.machine_sampler = machine_sampler
        self.popen = popen
        self.emulator_run_path = settings.work_dir / "emulator.pid.json"
        self._action_lock = threading.Lock()
        self._health_lock = threading.Lock()
        self._cpu_history: deque[float] = deque(maxlen=5)
        self._app_pss_history: deque[int] = deque(maxlen=5)
        self._last_history_at = 0.0

    def _last_emulator_restart(self) -> datetime | None:
        return parse_datetime(read_json(self.emulator_run_path).get("lastRestartAt"))

    def _record_restart_started(self, source: str, started_at: datetime) -> None:
        metadata = read_json(self.emulator_run_path)
        metadata.update({
            "lastRestartAt": iso_utc(started_at),
            "lastRestartSource": source,
            "lastRestartState": "in_progress",
            "lastRestartCompletedAt": None,
            "lastRestartMessage": None,
        })
        write_json(self.emulator_run_path, metadata)

    def _record_restart_finished(self, result: ActionResult) -> ActionResult:
        metadata = read_json(self.emulator_run_path)
        metadata.update({
            "lastRestartState": "succeeded" if result.ok else "failed",
            "lastRestartCompletedAt": iso_utc(utc_now()),
            "lastRestartMessage": result.message,
        })
        write_json(self.emulator_run_path, metadata)
        return result

    def public_config(self) -> dict[str, str]:
        return {
            "backend": execution_backend(self.settings),
            "server_id": str(self.settings.get("monitor.serverId") or socket.gethostname()),
            "account_id": str(self.settings.get("gigamoney.accountId") or "Account not configured"),
            "device": str(self.settings.get("adb.serial") or "emulator-5554"),
            "avd": self._avd_name(),
        }

    @staticmethod
    def _tracked_process(
        pid: int, metadata: Mapping[str, Any], probe: Callable[[int], dict[str, Any]], marker: str,
    ) -> tuple[bool, datetime | None]:
        details = probe(pid)
        if not details.get("running") or marker.casefold() not in str(details.get("command", "")).casefold():
            return False, None
        recorded = parse_datetime(metadata.get("startedAt"))
        actual_epoch = details.get("started_epoch")
        actual = (
            datetime.fromtimestamp(float(actual_epoch), tz=timezone.utc)
            if isinstance(actual_epoch, (float, int)) else None
        )
        if actual and recorded and abs((actual - recorded).total_seconds()) > 30:
            return False, actual
        return True, recorded or actual

    def script_status(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or utc_now()
        status = runner_status(self.settings)
        if not status.get("running"):
            if status.get("state") == "authentication_required":
                return {"state": "attention", "running": False, "pid": None, "started_at": None,
                        "duration_seconds": None, "authentication_required": True,
                        "message": "Authentication required. Complete verification, import the session, then start the script."}
            stale = status.get("stalePid")
            return {
                "state": "stopped", "running": False, "pid": None, "started_at": None,
                "duration_seconds": None,
                "message": f"The runner is not running; PID {stale} is stale." if stale else "The trading runner is not running.",
            }
        started = parse_datetime(status.get("startedAt"))
        return {
            "state": "running", "running": True, "pid": int(status["pid"]),
            "started_at": iso_utc(started) if started else None,
            "duration_seconds": max(0, int((now - started).total_seconds())) if started else None,
            "message": "The GigaMoney trading runner is running.",
        }

    def _runner_log_path(self) -> Path | None:
        metadata = read_json(self.settings.work_dir / "runner.pid.json")
        raw = metadata.get("log") or self.settings.get("monitor.runnerLog") or str(self.settings.log_dir / "runner.log")
        try:
            path = Path(str(raw))
            resolved = (path if path.is_absolute() else self.root / path).resolve()
            configured = self.settings.get("monitor.runnerLog")
            if not configured or resolved != self.settings.resolve_path(configured):
                resolved.relative_to(self.settings.log_dir)
            return resolved
        except (OSError, ValueError):
            return None

    def holdings_query_performance(self, script: Mapping[str, Any]) -> dict[str, Any]:
        empty = {
            "available": False, "session_started_at": script.get("started_at"),
            "attempts_in_session": 0, "sample_count": 0, "sample_size": HOLDINGS_SAMPLE_SIZE,
            "average_seconds": None, "fastest_seconds": None, "slowest_seconds": None,
        }
        if not script.get("running"):
            return {**empty, "message": "No active runner session."}
        path = self._runner_log_path()
        if not path:
            return {**empty, "message": "The active runner log path is not safe or configured."}
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return {**empty, "message": "The active runner log is not available yet."}
        session_started = parse_datetime(script.get("started_at"))
        durations: list[float] = []
        for match in HOLDINGS_QUERY_PATTERN.finditer(content):
            logged = parse_datetime(match.group("timestamp"))
            if session_started and logged and logged < session_started:
                continue
            durations.append(float(match.group("seconds")))
        estimated = False
        if not durations and session_started:
            interval = float(self.settings.get("kafka.holdingsIntervalSeconds", 30) or 30)
            if interval > 0:
                for match in HOLDINGS_PUBLISHED_PATTERN.finditer(content):
                    logged = parse_datetime(match.group("timestamp"))
                    if not logged or logged < session_started:
                        continue
                    elapsed = (logged - session_started).total_seconds()
                    duration = elapsed % interval
                    # A publication is normally seconds after its scheduled query.
                    # Discard ambiguous samples that crossed most of an interval.
                    if 0 <= duration < interval * 0.75:
                        durations.append(duration)
                estimated = bool(durations)
        if not durations:
            return {**empty, "message": "No completed holdings queries in this runner session yet."}
        samples = durations[-HOLDINGS_SAMPLE_SIZE:]
        count = len(samples)
        return {
            "available": True, "session_started_at": script.get("started_at"),
            "attempts_in_session": len(durations), "sample_count": count,
            "sample_size": HOLDINGS_SAMPLE_SIZE,
            "average_seconds": round(sum(samples) / count, 3),
            "fastest_seconds": round(min(samples), 3), "slowest_seconds": round(max(samples), 3),
            "estimated": estimated,
            "message": ((
                f"Using the latest {count} of {len(durations)} completed queries from this runner session."
                if len(durations) > HOLDINGS_SAMPLE_SIZE else
                f"Using all {count} completed quer{'y' if count == 1 else 'ies'} in this runner session."
            ) + (" Times are estimated from publication timestamps for this pre-upgrade runner." if estimated else "")),
        }

    def _adb_path(self) -> Path | None:
        try:
            return Path(find_adb(str(self.settings.get("adb.path") or ""), root=self.settings.root))
        except Exception:
            return None

    def _emulator_path(self) -> Path | None:
        candidates: list[Path] = []
        configured = str(self.settings.get("monitor.emulatorPath") or "").strip()
        if configured:
            candidates.append(Path(configured).expanduser())
        adb = self._adb_path()
        if adb:
            candidates.append(adb.parent.parent / "emulator" / "emulator")
        for variable in ("ANDROID_SDK_ROOT", "ANDROID_HOME"):
            if os.environ.get(variable):
                candidates.append(Path(os.environ[variable]) / "emulator" / "emulator")
        candidates.append(Path.home() / "Android" / "Sdk" / "emulator" / "emulator")
        return next((path.resolve() for path in candidates if path.is_file() and os.access(path, os.X_OK)), None)

    def _avd_name(self) -> str:
        configured = str(self.settings.get("monitor.avd") or "").strip()
        if configured:
            return configured
        avds = sorted(Path.home().glob(".android/avd/*.ini"))
        return avds[0].stem if len(avds) == 1 else ""

    @staticmethod
    def _parse_adb_devices(output: str) -> dict[str, str]:
        devices: dict[str, str] = {}
        for line in output.splitlines():
            fields = line.strip().split()
            if len(fields) >= 2 and not line.startswith("List of devices") and not line.startswith("*"):
                devices[fields[0]] = fields[1]
        return devices

    def _emulator_tracking(self) -> tuple[bool, int | None, datetime | None]:
        metadata = read_json(self.emulator_run_path)
        try:
            pid = int(metadata["pid"])
        except (KeyError, TypeError, ValueError):
            return False, None, None
        running, started = self._tracked_process(pid, metadata, self.probe, "emulator")
        return running, pid if running else None, started

    def emulator_status(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or utc_now()
        public = self.public_config()
        tracked, pid, started = self._emulator_tracking()
        last_restart = self._last_emulator_restart()
        base = {
            "device": public["device"], "avd": public["avd"], "pid": pid,
            "started_at": iso_utc(started) if started else None,
            "duration_seconds": max(0, int((now - started).total_seconds())) if started else None,
            "last_restart_at": iso_utc(last_restart) if last_restart else None,
            "adb_state": "unavailable", "adb_latency_ms": None, "shell_latency_ms": None,
        }
        adb = self._adb_path()
        if not adb:
            return {**base, "state": "unavailable", "running": tracked, "message": "ADB was not found."}
        adb_started = time.perf_counter()
        try:
            result = self.runner([str(adb), "devices"], cwd=self.root, timeout=8)
            adb_latency = round((time.perf_counter() - adb_started) * 1000)
        except (OSError, subprocess.SubprocessError) as exc:
            return {
                **base, "state": "starting" if tracked else "unavailable", "running": tracked,
                "adb_state": "error", "adb_latency_ms": round((time.perf_counter() - adb_started) * 1000),
                "message": f"ADB status check failed: {exc}",
            }
        if result.returncode != 0:
            return {
                **base, "state": "starting" if tracked else "unavailable", "running": tracked,
                "adb_state": "error", "adb_latency_ms": adb_latency,
                "message": "The emulator is starting." if tracked else "ADB could not check the emulator.",
            }
        device_state = self._parse_adb_devices(result.stdout).get(public["device"])
        if device_state == "device":
            shell_started = time.perf_counter()
            try:
                boot = self.runner(
                    [str(adb), "-s", public["device"], "shell", "getprop", "sys.boot_completed"],
                    cwd=self.root, timeout=5,
                )
                latency = round((time.perf_counter() - shell_started) * 1000)
                ready = boot.returncode == 0 and boot.stdout.strip() == "1"
            except (OSError, subprocess.SubprocessError):
                latency, ready = round((time.perf_counter() - shell_started) * 1000), False
            return {
                **base, "state": "running" if ready else "booting", "running": True,
                "adb_state": "device", "adb_latency_ms": adb_latency, "shell_latency_ms": latency,
                "message": "The Android emulator is ready." if ready else "Android is finishing its boot sequence.",
            }
        if device_state in {"offline", "unauthorized"}:
            return {
                **base, "state": "booting" if device_state == "offline" else "attention",
                "running": True, "adb_state": device_state, "adb_latency_ms": adb_latency,
                "message": f"ADB reports the emulator as {device_state}.",
            }
        return {
            **base, "state": "starting" if tracked else "stopped", "running": tracked,
            "adb_state": "disconnected", "adb_latency_ms": adb_latency,
            "message": "The emulator process is starting; waiting for ADB." if tracked else "The Android emulator is not running.",
        }

    def _android_memory(self, emulator: Mapping[str, Any]) -> dict[str, Any] | None:
        if emulator.get("adb_state") != "device":
            return None
        adb = self._adb_path()
        device = str(emulator.get("device") or "")
        if not adb or not device:
            return None
        try:
            system = self.runner(
                [str(adb), "-s", device, "shell", "dumpsys", "meminfo"], cwd=self.root, timeout=20
            )
            if system.returncode != 0:
                return None
            memory = parse_android_memory_summary(system.stdout)
            if not isinstance(memory.get("total_bytes"), int) or not isinstance(memory.get("used_bytes"), int):
                return None
            package = str(self.settings.get("gigamoney.package") or "lb.whale.hkwinner.android")
            app = self.runner(
                [str(adb), "-s", device, "shell", "dumpsys", "meminfo", package],
                cwd=self.root, timeout=15,
            )
            if app.returncode == 0:
                memory.update(parse_android_app_meminfo(app.stdout))
            return memory
        except (OSError, subprocess.SubprocessError, TypeError, ValueError):
            return None

    @staticmethod
    def _metric(metric_id: str, label: str, value: str, detail: str, level: str) -> dict[str, str]:
        return {
            "id": metric_id, "label": label, "value": value, "detail": detail,
            "level": level, "level_label": LEVEL_LABELS[level],
        }

    def health_status(self, emulator: Mapping[str, Any], checked_at: datetime) -> dict[str, Any]:
        try:
            machine = self.machine_sampler()
        except Exception:
            machine = {}
        api_backend = execution_backend(self.settings) == "api"
        android = None if api_backend else self._android_memory(emulator)
        cpu = machine.get("cpu_percent")
        app_pss = android.get("app_pss_bytes") if android else None
        with self._health_lock:
            if checked_at.timestamp() - self._last_history_at >= 30:
                if isinstance(cpu, (int, float)):
                    self._cpu_history.append(float(cpu))
                if isinstance(app_pss, int):
                    self._app_pss_history.append(app_pss)
                self._last_history_at = checked_at.timestamp()
            cpu_history, app_history = list(self._cpu_history), list(self._app_pss_history)
        metrics: list[dict[str, str]] = []

        if isinstance(cpu, (int, float)):
            average = sum(cpu_history) / len(cpu_history) if cpu_history else float(cpu)
            level = "restart_recommended" if len(cpu_history) >= 3 and average >= 90 else (
                "under_load" if cpu >= 80 or (len(cpu_history) >= 3 and average >= 75) else "healthy"
            )
            metrics.append(self._metric("machine_cpu", "Machine CPU", f"{cpu:.0f}%", f"{len(cpu_history)}-check average: {average:.0f}%.", level))
        else:
            metrics.append(self._metric("machine_cpu", "Machine CPU", "Unavailable", "Linux CPU counters are unavailable.", "under_load"))

        memory_percent = machine.get("memory_percent")
        total = machine.get("memory_total_bytes")
        available = machine.get("memory_available_bytes")
        if isinstance(memory_percent, (int, float)) and isinstance(total, int) and isinstance(available, int):
            level = "restart_recommended" if memory_percent >= 93 or available < 0.75 * 1024**3 else (
                "under_load" if memory_percent >= 85 or available < 1.5 * 1024**3 else "healthy"
            )
            metrics.append(self._metric(
                "machine_memory", "Machine memory", f"{memory_percent:.0f}%",
                f"{format_bytes(total - available)} used · {format_bytes(available)} available.", level,
            ))
        else:
            metrics.append(self._metric("machine_memory", "Machine memory", "Unavailable", "Linux memory counters are unavailable.", "under_load"))

        if api_backend:
            overall = max((metric["level"] for metric in metrics), key=LEVEL_RANK.__getitem__)
            summary = ("Machine health is normal. Trading uses the direct API." if overall == "healthy"
                       else "Machine load or memory needs attention. Trading uses the direct API.")
            return {"overall_level": overall, "overall_label": LEVEL_LABELS[overall],
                    "summary": summary, "metrics": metrics}

        if android:
            guest_total, guest_used = android.get("total_bytes"), android.get("used_bytes")
            used_percent = 100 * guest_used / guest_total if guest_total else 0
            swap_used, swap_total = android.get("swap_used_bytes", 0), android.get("swap_total_bytes", 0)
            swap_percent = 100 * swap_used / swap_total if swap_total else 0
            status = str(android.get("status") or "").casefold()
            growth = app_history[-1] - app_history[0] if len(app_history) >= 3 else 0
            level = "restart_recommended" if (
                status in {"critical", "low"} or used_percent >= 92 or swap_percent >= 80 or growth >= 1024**3
            ) else ("under_load" if used_percent >= 82 or swap_percent >= 60 or growth >= 512 * 1024**2 else "healthy")
            details = [f"{format_bytes(guest_used)} of {format_bytes(guest_total)} used"]
            if swap_total:
                details.append(f"swap {swap_percent:.0f}%")
            if isinstance(app_pss, int):
                details.append(f"GigaMoney {format_bytes(app_pss)} PSS")
            metrics.append(self._metric("emulator_memory", "Android memory", f"{used_percent:.0f}% used", " · ".join(details) + ".", level))
        else:
            level = "under_load" if emulator.get("running") else "restart_recommended"
            metrics.append(self._metric("emulator_memory", "Android memory", "Unavailable", "Android memory pressure could not be read.", level))

        adb_state, adb_latency = str(emulator.get("adb_state") or "unavailable"), emulator.get("adb_latency_ms")
        if adb_state == "device" and isinstance(adb_latency, (int, float)):
            level = "under_load" if adb_latency >= 1000 else "healthy"
            metrics.append(self._metric("adb_health", "ADB health", f"{adb_latency:.0f} ms", "ADB can see the configured emulator.", level))
        else:
            level = "under_load" if emulator.get("running") else "restart_recommended"
            metrics.append(self._metric("adb_health", "ADB health", adb_state.title(), "ADB cannot reach a ready device.", level))

        shell_latency = emulator.get("shell_latency_ms")
        if isinstance(shell_latency, (int, float)):
            level = "restart_recommended" if shell_latency >= 3000 else ("under_load" if shell_latency >= 1500 else "healthy")
            metrics.append(self._metric("android_response", "Android response", f"{shell_latency:.0f} ms", "Timed with a boot-state shell query.", level))
        else:
            level = "under_load" if emulator.get("running") else "restart_recommended"
            metrics.append(self._metric("android_response", "Android response", "Unavailable", "Android did not answer the health query.", level))

        overall = max((metric["level"] for metric in metrics), key=LEVEL_RANK.__getitem__)
        summaries = {
            "healthy": "All five health signals are within normal ranges.",
            "under_load": "At least one signal shows elevated load or reduced visibility.",
            "restart_recommended": "At least one signal indicates that the emulator should be started or restarted.",
        }
        return {"overall_level": overall, "overall_label": LEVEL_LABELS[overall], "summary": summaries[overall], "metrics": metrics}

    def collect_status(self, interval_seconds: int) -> dict[str, Any]:
        checked = utc_now()
        script = self.script_status(checked)
        emulator = ({"state": "optional", "running": False, "message": "The API runner does not require Android."}
                    if execution_backend(self.settings) == "api" else self.emulator_status(checked))
        return {
            "account": self.public_config(), "script": script,
            "holdings_queries": self.holdings_query_performance(script),
            "emulator": emulator, "health": self.health_status(emulator, checked),
            "checked_at": iso_utc(checked),
            "next_check_at": iso_utc(datetime.fromtimestamp(checked.timestamp() + interval_seconds, tz=timezone.utc)),
            "interval_seconds": interval_seconds,
        }

    def start_script(self) -> ActionResult:
        if self.script_status().get("running"):
            return ActionResult(True, "The GigaMoney trading runner is already running.")
        value = self.settings.get("monitor.runnerLog")
        log_path = self.settings.resolve_path(value) if value else self.settings.log_dir / "runner.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, "-m", "gigamoney.cli", "--config", str(self.settings.path), "runner"]
        environment = os.environ.copy()
        environment["GIGAMONEY_RUN_LOG"] = str(log_path)
        try:
            with log_path.open("ab", buffering=0) as output:
                process = self.popen(
                    command, cwd=str(self.root), stdin=subprocess.DEVNULL,
                    stdout=output, stderr=subprocess.STDOUT, env=environment, start_new_session=True,
                )
        except OSError as exc:
            return ActionResult(False, f"Could not start the trading runner: {exc}")
        for _ in range(20):
            time.sleep(0.1)
            if self.script_status().get("running"):
                return ActionResult(True, f"Started the GigaMoney trading runner with PID {process.pid}.")
            if process.poll() is not None:
                return ActionResult(False, f"The trading runner exited during startup with code {process.returncode}.")
        return ActionResult(True, f"Started runner process {process.pid}; it is still initializing.")

    def stop_script(self) -> ActionResult:
        status = self.script_status()
        if not status.get("running"):
            return ActionResult(True, "The GigaMoney trading runner is already stopped.")
        result = request_runner_stop(self.settings, force=True)
        if not result.get("requested"):
            return ActionResult(False, str(result.get("reason") or "Could not request a runner stop."))
        return ActionResult(True, f"Stop requested for trading runner PID {result['pid']}.")

    def _start_emulator(self) -> ActionResult:
        current = self.emulator_status()
        if current.get("running"):
            return ActionResult(True, "The Android emulator is already running or starting.")
        emulator, avd = self._emulator_path(), self._avd_name()
        if not emulator:
            return ActionResult(False, "The Android emulator executable was not found.")
        if not avd:
            return ActionResult(False, "No unique AVD was found. Set monitor.avd in the config.")
        raw_args = self.settings.get(
            "monitor.emulatorArgs",
            ["-netdelay", "none", "-netspeed", "full", "-no-snapshot-load"],
        )
        extra_args = [str(value) for value in raw_args] if isinstance(raw_args, list) else []
        if not {"-no-snapshot-load", "-no-snapshot", "-no-snapstorage"}.intersection(extra_args):
            extra_args.append("-no-snapshot-load")
        started = utc_now()
        log_path = self.settings.log_dir / f"emulator-{started.strftime('%Y%m%dT%H%M%SZ')}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        configured_display = str(self.settings.get("monitor.display") or "").strip()
        if configured_display and not environment.get("DISPLAY"):
            environment["DISPLAY"] = configured_display
        try:
            with log_path.open("ab", buffering=0) as output:
                process = self.popen(
                    [str(emulator), "-avd", avd, *extra_args], cwd=str(self.root),
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                    env=environment, start_new_session=True,
                )
            prior_metadata = read_json(self.emulator_run_path)
            restart_metadata = {
                name: prior_metadata[name]
                for name in (
                    "lastRestartAt", "lastRestartSource", "lastRestartState",
                    "lastRestartCompletedAt", "lastRestartMessage",
                )
                if name in prior_metadata
            }
            write_json(self.emulator_run_path, {
                "pid": process.pid, "startedAt": started.isoformat(), "avd": avd,
                "log": str(log_path), **restart_metadata,
            })
        except OSError as exc:
            return ActionResult(False, f"Could not start the Android emulator: {exc}")
        return ActionResult(True, f"Started Android emulator {avd}. It may take a minute to become ready.")

    def start_emulator(self, *, restart_source: str | None = None) -> ActionResult:
        if not restart_source:
            return self._start_emulator()
        if self.emulator_status().get("running"):
            return ActionResult(True, "The Android emulator is already running or starting.")
        self._record_restart_started(restart_source, utc_now())
        return self._record_restart_finished(self._start_emulator())

    def stop_emulator(self) -> ActionResult:
        adb, public = self._adb_path(), self.public_config()
        adb_error = ""
        if adb:
            try:
                result = self.runner(
                    [str(adb), "-s", public["device"], "emu", "kill"], cwd=self.root, timeout=12
                )
                if result.returncode == 0:
                    return ActionResult(True, f"Stop signal sent to Android emulator {public['device']}.")
                adb_error = (result.stderr or result.stdout).strip()
            except (OSError, subprocess.SubprocessError) as exc:
                adb_error = str(exc)
        tracked, pid, _ = self._emulator_tracking()
        if tracked and pid:
            try:
                os.kill(pid, signal.SIGTERM)
                return ActionResult(True, f"Stopped tracked emulator process {pid}.")
            except OSError as exc:
                return ActionResult(False, f"Could not stop tracked emulator process: {exc}")
        if self.emulator_status().get("running"):
            return ActionResult(False, f"ADB could not stop the emulator: {adb_error or 'unknown error'}")
        return ActionResult(True, "The Android emulator is already stopped.")

    def _wait_for_script_stopped(self, waiter: Callable[[float], bool]) -> ActionResult:
        deadline = time.monotonic() + float(self.settings.get("monitor.restart.runnerStopTimeoutSeconds", RUNNER_STOP_TIMEOUT_SECONDS))
        while self.script_status().get("running"):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ActionResult(
                    False,
                    "The trading runner did not finish its current operation within six minutes; "
                    "the emulator was left running.",
                )
            if waiter(min(float(self.settings.get("monitor.restart.runnerPollSeconds", RUNNER_STATE_POLL_SECONDS)), remaining)):
                return ActionResult(
                    False,
                    "The restart was cancelled while waiting for the trading runner to stop; "
                    "the emulator was left running.",
                )
        return ActionResult(True, "The trading runner stopped cleanly.")

    def _wait_for_emulator_ready(self, waiter: Callable[[float], bool]) -> ActionResult:
        timeout = max(
            0.0,
            float(self.settings.get("monitor.emulatorBootTimeoutSeconds", EMULATOR_BOOT_TIMEOUT_SECONDS)),
        )
        deadline = time.monotonic() + timeout
        wait_budget = timeout
        last_message = "The emulator has not reported a status yet."
        while True:
            status = self.emulator_status()
            last_message = str(status.get("message") or status.get("state") or "Status unavailable.")
            if status.get("state") == "running" and status.get("adb_state") == "device":
                return ActionResult(True, "The Android emulator completed its cold boot.")
            if not status.get("running"):
                return ActionResult(
                    False,
                    f"The Android emulator process exited before Android became ready. {last_message}",
                )

            remaining = min(wait_budget, deadline - time.monotonic())
            if remaining <= 0:
                return ActionResult(
                    False,
                    f"The Android emulator did not become ready within {timeout:g} seconds. "
                    f"Last status: {last_message}",
                )
            interval = min(float(self.settings.get("monitor.restart.emulatorBootPollSeconds", EMULATOR_BOOT_POLL_SECONDS)), remaining)
            if waiter(interval):
                return ActionResult(False, "The restart was cancelled while waiting for the emulator to boot.")
            wait_budget -= interval

    @staticmethod
    def _foreground_packages(output: str) -> set[str]:
        packages: set[str] = set()
        for line in output.splitlines():
            if not any(marker in line for marker in (
                "mCurrentFocus", "mFocusedApp", "mResumedActivity", "topResumedActivity",
            )):
                continue
            packages.update(re.findall(r"([A-Za-z0-9_.]+)/[A-Za-z0-9_.$]+", line))
        return packages

    def _foreground_gigamoney(self, waiter: Callable[[float], bool]) -> ActionResult:
        adb = self._adb_path()
        if not adb:
            return ActionResult(False, "ADB was not found; GigaMoney could not be brought to the foreground.")
        public = self.public_config()
        device = public["device"]
        package = str(self.settings.get("gigamoney.package") or "lb.whale.hkwinner.android").strip()
        last_detail = "Android did not report a foreground application."
        max_tries = int(self.settings.get("monitor.restart.foregroundMaxTries", GIGAMONEY_FOREGROUND_MAX_TRIES))
        for attempt in range(1, max_tries + 1):
            try:
                launch = self.runner(
                    [
                        str(adb), "-s", device, "shell", "monkey", "-p", package,
                        "-c", "android.intent.category.LAUNCHER", "1",
                    ],
                    cwd=self.root, timeout=15,
                )
                if launch.returncode != 0:
                    last_detail = (launch.stderr or launch.stdout).strip() or "The launch command failed."
                if waiter(float(self.settings.get("monitor.restart.foregroundRetrySeconds", GIGAMONEY_FOREGROUND_RETRY_SECONDS))):
                    return ActionResult(False, "The restart was cancelled while waiting for GigaMoney.")
                if launch.returncode == 0:
                    windows = self.runner(
                        [str(adb), "-s", device, "shell", "dumpsys", "window", "windows"],
                        cwd=self.root, timeout=10,
                    )
                    activities = self.runner(
                        [str(adb), "-s", device, "shell", "dumpsys", "activity", "activities"],
                        cwd=self.root, timeout=10,
                    )
                    foreground = self._foreground_packages(
                        (windows.stdout if windows.returncode == 0 else "")
                        + "\n"
                        + (activities.stdout if activities.returncode == 0 else "")
                    )
                    if package in foreground:
                        return ActionResult(True, f"GigaMoney reached the foreground on try {attempt}.")
                    last_detail = (
                        f"Foreground application: {', '.join(sorted(foreground)) or 'unknown'}."
                    )
            except (OSError, subprocess.SubprocessError) as exc:
                last_detail = str(exc)
        return ActionResult(
            False,
            f"GigaMoney did not reach the foreground after {max_tries} tries. "
            f"{last_detail}",
        )

    def _restart_system_locked(
        self, *, source: str, started_at: datetime, waiter: Callable[[float], bool],
    ) -> ActionResult:
        self._record_restart_started(source, started_at)
        try:
            script_stop = self.stop_script()
            if not script_stop.ok:
                return self._record_restart_finished(ActionResult(
                    False, f"Restart failed while stopping the trading runner: {script_stop.message}",
                ))
            script_stopped = self._wait_for_script_stopped(waiter)
            if not script_stopped.ok:
                return self._record_restart_finished(script_stopped)

            lock_timeout = float(self.settings.get("automation.lockTimeoutSeconds", 300))
            with EmulatorLock(self.settings.work_dir / "emulator.lock", lock_timeout):
                emulator_stop = self.stop_emulator()
                if not emulator_stop.ok:
                    return self._record_restart_finished(ActionResult(
                        False, f"Restart failed while stopping the emulator: {emulator_stop.message}",
                    ))
                if waiter(float(self.settings.get("monitor.restart.emulatorStopSettleSeconds", EMULATOR_STOP_SETTLE_SECONDS))):
                    return self._record_restart_finished(ActionResult(
                        False, "The restart was cancelled before emulator start.",
                    ))
                if self.emulator_status().get("running"):
                    self.stop_emulator()
                    return self._record_restart_finished(ActionResult(
                        False,
                        "The emulator was still running after the one-minute shutdown period; "
                        "the listener was left stopped to avoid an unsafe partial restart.",
                    ))

                emulator_start = self._start_emulator()
                if not emulator_start.ok:
                    return self._record_restart_finished(ActionResult(
                        False, f"Restart failed while starting the emulator: {emulator_start.message}",
                    ))
                emulator_ready = self._wait_for_emulator_ready(waiter)
                if not emulator_ready.ok:
                    final_stop = self.stop_emulator()
                    stop_detail = "" if final_stop.ok else f" Final emulator stop also failed: {final_stop.message}"
                    return self._record_restart_finished(ActionResult(
                        False, emulator_ready.message + stop_detail,
                    ))

                foreground = self._foreground_gigamoney(waiter)
                if not foreground.ok:
                    final_stop = self.stop_emulator()
                    stop_detail = "" if final_stop.ok else f" Final emulator stop also failed: {final_stop.message}"
                    return self._record_restart_finished(ActionResult(
                        False, foreground.message + stop_detail,
                    ))

                script_start = self.start_script()
                if not script_start.ok:
                    return self._record_restart_finished(ActionResult(
                        False,
                        f"GigaMoney was foregrounded, but the trading runner could not start: "
                        f"{script_start.message}",
                    ))
        except Exception as exc:
            LOG.exception("Restart sequence failed unexpectedly")
            return self._record_restart_finished(ActionResult(
                False, f"Restart failed unexpectedly: {exc}",
            ))
        return self._record_restart_finished(ActionResult(
            True, "The emulator, GigaMoney application, and trading runner restarted successfully.",
        ))

    def scheduled_restart_if_due(
        self, now: datetime | None = None, *, waiter: Callable[[float], bool] | None = None,
    ) -> ActionResult | None:
        if execution_backend(self.settings) == "api":
            return None
        checked_at = now or utc_now()
        if runner_status(self.settings).get("state") == "authentication_required":
            return None
        if not in_scheduled_restart_window(checked_at, self.settings):
            return None
        last_restart = self._last_emulator_restart()
        minimum = float(self.settings.get("monitor.restart.minIntervalSeconds", RESTART_MIN_INTERVAL_SECONDS))
        if last_restart and (checked_at - last_restart).total_seconds() <= minimum:
            return None
        if not self._action_lock.acquire(blocking=False):
            return None
        try:
            last_restart = self._last_emulator_restart()
            if last_restart and (checked_at - last_restart).total_seconds() <= minimum:
                return None
            effective_waiter = waiter
            if effective_waiter is None:
                def effective_waiter(seconds: float) -> bool:
                    time.sleep(seconds)
                    return False
            restart_started = time.perf_counter()
            LOG.info("Scheduled restart started.")
            result = self._restart_system_locked(
                source="scheduled", started_at=checked_at, waiter=effective_waiter,
            )
            LOG.log(logging.INFO if result.ok else logging.ERROR,
                    "Scheduled restart finished outcome=%s elapsed_s=%.3f message=%r.",
                    "success" if result.ok else "failure", time.perf_counter() - restart_started,
                    result.message)
            return result
        finally:
            self._action_lock.release()

    def perform(self, action: str) -> ActionResult:
        actions = {
            "script/start": self.start_script, "script/stop": self.stop_script,
            "emulator/start": lambda: self.start_emulator(restart_source="web_ui"),
            "emulator/stop": self.stop_emulator,
        }
        selected = actions.get(action)
        if not selected:
            return ActionResult(False, "Unknown control action.")
        if not self._action_lock.acquire(blocking=False):
            return ActionResult(False, "Another control action is already in progress.")
        started = time.perf_counter()
        result = None
        LOG.info("Control action started action=%s.", action)
        try:
            result = selected()
            return result
        except Exception as exc:
            LOG.exception("Control action %s failed", action)
            result = ActionResult(False, f"Control action failed: {exc}")
            return result
        finally:
            if result is not None:
                LOG.log(logging.INFO if result.ok else logging.ERROR,
                        "Control action finished action=%s outcome=%s elapsed_s=%.3f message=%r.",
                        action, "success" if result.ok else "failure",
                        time.perf_counter() - started, result.message)
            self._action_lock.release()


class StatusMonitor:
    def __init__(self, controller: GigaMoneyController, interval_seconds: int = 60) -> None:
        self.controller = controller
        self.interval_seconds = interval_seconds
        self._value: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._restart_thread: threading.Thread | None = None

    def refresh(self) -> dict[str, Any]:
        try:
            value = self.controller.collect_status(self.interval_seconds)
        except Exception as exc:
            LOG.exception("Status refresh failed")
            checked = utc_now()
            value = {
                "account": self.controller.public_config(),
                "script": {"state": "attention", "running": False, "message": str(exc)},
                "emulator": {"state": "attention", "running": False, "message": "Status unavailable."},
                "holdings_queries": {"available": False, "sample_count": 0, "message": "Status unavailable."},
                "health": {"overall_level": "under_load", "overall_label": "Under load", "summary": "Status unavailable.", "metrics": []},
                "checked_at": iso_utc(checked), "next_check_at": iso_utc(checked),
                "interval_seconds": self.interval_seconds,
            }
        with self._lock:
            self._value = value
        return value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._value)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.refresh()

    def _run_restart_scheduler(self) -> None:
        while not self._stop.is_set():
            try:
                result = self.controller.scheduled_restart_if_due(waiter=self._stop.wait)
                if result:
                    self.refresh()
            except Exception:
                LOG.exception("Scheduled restart check failed")
            if self._stop.wait(float(self.controller.settings.get("monitor.restart.checkIntervalSeconds", RESTART_CHECK_INTERVAL_SECONDS))):
                break

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.refresh()
        self._thread = threading.Thread(target=self._run, name="gigamoney-status", daemon=True)
        self._thread.start()
        if execution_backend(self.controller.settings) != "api":
            self._restart_thread = threading.Thread(
                target=self._run_restart_scheduler,
                name="gigamoney-restart-scheduler",
                daemon=True,
            )
            self._restart_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._restart_thread:
            self._restart_thread.join(timeout=5)


class SessionStore:
    def __init__(self, lifetime_seconds: int = SESSION_LIFETIME_SECONDS) -> None:
        self.lifetime_seconds = lifetime_seconds
        self._sessions: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        self._sessions = {
            token: session for token, session in self._sessions.items()
            if float(session["expires_epoch"]) > now
        }

    def create(self, username: str) -> tuple[str, dict[str, Any]]:
        now = time.time()
        token = secrets.token_urlsafe(32)
        session = {
            "username": username,
            "csrf_token": secrets.token_urlsafe(32),
            "expires_epoch": now + self.lifetime_seconds,
        }
        with self._lock:
            self._prune(now)
            self._sessions[token] = session
        return token, dict(session)

    def get(self, token: str | None) -> dict[str, Any] | None:
        if not token:
            return None
        now = time.time()
        with self._lock:
            self._prune(now)
            session = self._sessions.get(token)
            return dict(session) if session else None

    def destroy(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self._sessions.pop(token, None)


class LoginLimiter:
    def __init__(self, window=LOGIN_WINDOW_SECONDS, lockout=LOGIN_LOCKOUT_SECONDS, maximum=MAX_LOGIN_FAILURES) -> None:
        self.window, self.lockout, self.maximum = window, lockout, maximum
        self._failures: dict[str, deque[float]] = {}
        self._lockouts: dict[str, float] = {}
        self._lock = threading.Lock()

    def retry_after(self, client: str) -> int:
        now = time.time()
        with self._lock:
            until = self._lockouts.get(client, 0)
            if until <= now:
                self._lockouts.pop(client, None)
                return 0
            return max(1, int(until - now))

    def record_failure(self, client: str) -> int:
        now = time.time()
        cutoff = now - self.window
        with self._lock:
            attempts = self._failures.setdefault(client, deque())
            while attempts and attempts[0] < cutoff:
                attempts.popleft()
            attempts.append(now)
            if len(attempts) >= self.maximum:
                until = now + self.lockout
                self._lockouts[client] = until
                attempts.clear()
                return self.lockout
            return 0

    def clear(self, client: str) -> None:
        with self._lock:
            self._failures.pop(client, None)
            self._lockouts.pop(client, None)


class GigaMoneyServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self, address: tuple[str, int], monitor: StatusMonitor, *, admin_password: str,
        settings: Settings | None = None,
    ) -> None:
        super().__init__(address, GigaMoneyRequestHandler)
        self.monitor = monitor
        self.admin_password = admin_password
        get = settings.get if settings else lambda key, default: default
        self.admin_username = get("monitor.adminUsername", ADMIN_USERNAME)
        self.sessions = SessionStore(get("monitor.sessionLifetimeSeconds", SESSION_LIFETIME_SECONDS))
        self.login_limiter = LoginLimiter(get("monitor.loginWindowSeconds", LOGIN_WINDOW_SECONDS),
                                          get("monitor.loginLockoutSeconds", LOGIN_LOCKOUT_SECONDS),
                                          get("monitor.maxLoginFailures", MAX_LOGIN_FAILURES))
        self.is_tls = False
        self.tls_context: ssl.SSLContext | None = None

    def get_request(self):
        connection, client_address = super().get_request()
        if not self.tls_context:
            return connection, client_address
        try:
            connection.settimeout(10)
            secure = self.tls_context.wrap_socket(
                connection, server_side=True, do_handshake_on_connect=False
            )
            return secure, client_address
        except Exception:
            connection.close()
            raise


class RedirectServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], public_host: str, https_port: int) -> None:
        super().__init__(address, RedirectRequestHandler)
        self.public_host = public_host
        self.https_port = https_port


class RedirectRequestHandler(BaseHTTPRequestHandler):
    server: RedirectServer
    server_version = "GigaMoneyRedirect/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        LOG.info("%s redirect %s", self.client_address[0], format_string % args)

    def _redirect(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path if parsed.path.startswith("/") else "/"
        if parsed.query:
            path += f"?{parsed.query}"
        location = https_url(self.server.public_host, self.server.https_port).rstrip("/") + path
        self.send_response(HTTPStatus.PERMANENT_REDIRECT)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()

    do_GET = _redirect
    do_HEAD = _redirect
    do_POST = _redirect


class GigaMoneyRequestHandler(BaseHTTPRequestHandler):
    server: GigaMoneyServer
    server_version = "GigaMoneyMonitor/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        LOG.info("%s %s", self.client_address[0], format_string % args)

    def _headers(self, content_type: str, length: int) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        if self.server.is_tls:
            self.send_header("Strict-Transport-Security", "max-age=31536000")
        self.send_header("Content-Security-Policy", "default-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")

    def _json(
        self, payload: Mapping[str, Any], status: HTTPStatus = HTTPStatus.OK,
        *, headers: Mapping[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._headers("application/json; charset=utf-8", len(body))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _file(self, name: str, content_type: str) -> None:
        try:
            body = (WEB_ROOT / name).read_bytes()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_response(HTTPStatus.OK)
        self._headers(content_type, len(body))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _same_origin(self) -> bool:
        origin = self.headers.get("Origin")
        if not origin:
            return True
        parsed = urlparse(origin)
        expected_scheme = "https" if self.server.is_tls else "http"
        return parsed.scheme == expected_scheme and parsed.netloc.casefold() == self.headers.get("Host", "").casefold()

    def _session_token(self) -> str | None:
        raw = self.headers.get("Cookie")
        if not raw:
            return None
        try:
            cookie = SimpleCookie()
            cookie.load(raw)
            value = cookie.get(SESSION_COOKIE_NAME)
            return value.value if value else None
        except Exception:
            return None

    def _session(self) -> dict[str, Any] | None:
        return self.server.sessions.get(self._session_token())

    def _require_session(self, *, api: bool) -> dict[str, Any] | None:
        session = self._session()
        if session:
            return session
        if api:
            self._json({"ok": False, "message": "Authentication required."}, HTTPStatus.UNAUTHORIZED)
        else:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", "/login")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
        return None

    def _read_json_body(self) -> dict[str, Any] | None:
        if not self.headers.get("Content-Type", "").lower().startswith("application/json"):
            self._json({"ok": False, "message": "JSON request required."}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            return None
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            size = -1
        if size < 0 or size > 4096:
            self._json({"ok": False, "message": "Invalid request size."}, HTTPStatus.BAD_REQUEST)
            return None
        try:
            value = json.loads(self.rfile.read(size) if size else b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._json({"ok": False, "message": "Invalid JSON request."}, HTTPStatus.BAD_REQUEST)
            return None
        if not isinstance(value, dict):
            self._json({"ok": False, "message": "JSON object required."}, HTTPStatus.BAD_REQUEST)
            return None
        return value

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/healthz":
            self._json({"ok": True, "service": "gigamoney-monitor"})
        elif path == "/login":
            if self._session():
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Location", "/")
                self.end_headers()
            else:
                self._file("login.html", "text/html; charset=utf-8")
        elif path == "/login.js":
            self._file("login.js", "text/javascript; charset=utf-8")
        elif path == "/api/session":
            session = self._require_session(api=True)
            if session:
                self._json({
                    "ok": True, "username": session["username"],
                    "csrf_token": session["csrf_token"],
                    "expires_at": iso_utc(datetime.fromtimestamp(session["expires_epoch"], tz=timezone.utc)),
                })
        elif path == "/api/status":
            if not self._require_session(api=True):
                return
            refresh = urlparse(self.path).query == "refresh=1"
            status = self.server.monitor.refresh() if refresh else self.server.monitor.snapshot()
            self._json(status or self.server.monitor.refresh())
        elif path in {"/", "/index.html"}:
            if self._require_session(api=False):
                self._file("index.html", "text/html; charset=utf-8")
        elif path == "/app.js":
            if self._require_session(api=False):
                self._file("app.js", "text/javascript; charset=utf-8")
        elif path == "/styles.css":
            self._file("styles.css", "text/css; charset=utf-8")
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def _login(self) -> None:
        if not self._same_origin():
            self._json({"ok": False, "message": "Login request rejected."}, HTTPStatus.FORBIDDEN)
            return
        payload = self._read_json_body()
        if payload is None:
            return
        client = self.client_address[0]
        retry_after = self.server.login_limiter.retry_after(client)
        if retry_after:
            self._json(
                {"ok": False, "message": "Too many failed attempts. Try again later.", "retry_after": retry_after},
                HTTPStatus.TOO_MANY_REQUESTS, headers={"Retry-After": str(retry_after)},
            )
            return
        if not verify_admin_credentials(
            payload.get("username"), payload.get("password"), self.server.admin_password,
            self.server.admin_username,
        ):
            retry_after = self.server.login_limiter.record_failure(client)
            status = HTTPStatus.TOO_MANY_REQUESTS if retry_after else HTTPStatus.UNAUTHORIZED
            headers = {"Retry-After": str(retry_after)} if retry_after else None
            message = "Too many failed attempts. Try again later." if retry_after else "Invalid username or password."
            self._json({"ok": False, "message": message}, status, headers=headers)
            return
        self.server.login_limiter.clear(client)
        self.server.sessions.destroy(self._session_token())
        token, session = self.server.sessions.create(self.server.admin_username)
        cookie = f"{SESSION_COOKIE_NAME}={token}; Path=/; Secure; HttpOnly; SameSite=Strict"
        self._json(
            {"ok": True, "username": self.server.admin_username, "csrf_token": session["csrf_token"]},
            headers={"Set-Cookie": cookie},
        )

    def _logout(self) -> None:
        session = self._require_session(api=True)
        if not session:
            return
        if not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""), session["csrf_token"]):
            self._json({"ok": False, "message": "Control request rejected."}, HTTPStatus.FORBIDDEN)
            return
        self.server.sessions.destroy(self._session_token())
        cookie = f"{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Strict"
        self._json({"ok": True, "message": "Signed out."}, headers={"Set-Cookie": cookie})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/login":
            self._login()
            return
        if not parsed.path.startswith("/api/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if parsed.path == "/api/logout":
            if not self._same_origin():
                self._json({"ok": False, "message": "Control request rejected."}, HTTPStatus.FORBIDDEN)
                return
            self._logout()
            return
        session = self._require_session(api=True)
        if not session:
            return
        if (
            not self._same_origin()
            or self.headers.get("X-GigaMoney-Action") != "1"
            or not secrets.compare_digest(self.headers.get("X-CSRF-Token", ""), session["csrf_token"])
        ):
            self._json({"ok": False, "message": "Control request rejected."}, HTTPStatus.FORBIDDEN)
            return
        if self._read_json_body() is None:
            return
        action = parsed.path.removeprefix("/api/")
        result = self.server.monitor.controller.perform(action)
        status = self.server.monitor.refresh()
        self._json(
            {"ok": result.ok, "message": result.message, "status": status},
            HTTPStatus.OK if result.ok else HTTPStatus.CONFLICT,
        )


def monitor_status(settings: Settings) -> dict[str, Any]:
    path = settings.work_dir / "monitor.pid.json"
    record = read_json(path)
    try:
        pid = int(record["pid"])
    except (KeyError, TypeError, ValueError):
        return {"running": False}
    details = process_probe(pid)
    if not details.get("running") or "gigamoney.monitor" not in str(details.get("command", "")).casefold():
        return {"running": False, "stalePid": pid}
    return {"running": True, **record}


def _configured_path(root: Path, value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    return (path if path.is_absolute() else root / path).resolve()


def detect_machine_ipv4() -> str | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("1.1.1.1", 443))
            address = str(probe.getsockname()[0]).strip()
    except OSError:
        return None
    return address if address and not address.startswith("127.") else None


def https_url(host: str, port: int) -> str:
    display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"https://{display_host}{'' if port == 443 else f':{port}'}/"


def start_monitor(
    settings: Settings, *, host: str | None = None, port: int | None = None,
    redirect_http_port: int | None = None, interval: int | None = None, cert_file: str | None = None,
    key_file: str | None = None, open_browser: bool = False,
) -> dict[str, Any]:
    host = str(host or settings.get("monitor.host") or "127.0.0.1")
    port = int(port if port is not None else settings.get("monitor.port", 8787))
    redirect_http_port = int(
        redirect_http_port if redirect_http_port is not None
        else settings.get("monitor.redirectHttpPort", 0)
    )
    interval = int(interval if interval is not None else settings.get("monitor.intervalSeconds", 60))
    certificate = _configured_path(settings.root, cert_file or settings.get("monitor.certFile"))
    private_key = _configured_path(settings.root, key_file or settings.get("monitor.keyFile"))
    if not certificate or not certificate.is_file() or not private_key or not private_key.is_file():
        return {"started": False, "reason": "HTTPS certificate or private key is missing."}
    public_host = str(settings.get("monitor.publicHost") or "").strip()
    if not public_host:
        public_host = detect_machine_ipv4() or ("localhost" if host in {"0.0.0.0", "::"} else host)
    public_url = https_url(public_host, port)
    current = monitor_status(settings)
    if current.get("running"):
        if open_browser:
            webbrowser.open(str(current.get("url") or public_url))
        return {"started": False, "reason": "monitor is already running", **current}
    log_path = settings.log_dir / "monitor.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable, "-m", "gigamoney.monitor", "--config", str(settings.path),
        "--host", host, "--port", str(port), "--interval", str(interval),
        "--cert-file", str(certificate), "--key-file", str(private_key),
    ]
    if redirect_http_port:
        command.extend(("--redirect-http-port", str(redirect_http_port)))
    if any(0 < value < 1024 for value in (port, redirect_http_port)) and os.geteuid() != 0:
        authbind = shutil.which("authbind")
        if not authbind:
            return {"started": False, "reason": "authbind is required to use a privileged port as a non-root user."}
        command = [authbind, "--deep", *command]
    with log_path.open("ab", buffering=0) as output:
        process = subprocess.Popen(
            command, cwd=str(settings.root), stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
    probe_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    probe_url = https_url(probe_host, port)
    tls_context = ssl._create_unverified_context()
    ready = False
    for _ in range(75):
        time.sleep(0.2)
        if process.poll() is not None:
            return {"started": False, "reason": f"monitor exited with code {process.returncode}", "log": str(log_path)}
        try:
            with urllib.request.urlopen(probe_url + "healthz", timeout=0.4, context=tls_context) as response:
                ready = response.status == 200 and bool(response.read())
            if ready:
                break
        except (OSError, urllib.error.URLError):
            pass
    if open_browser and ready:
        webbrowser.open(public_url)
    return {"started": True, "pid": process.pid, "url": public_url, "ready": ready, "log": str(log_path)}


def stop_monitor(settings: Settings) -> dict[str, Any]:
    status = monitor_status(settings)
    if not status.get("running"):
        return {"requested": False, "reason": "monitor is not running"}
    os.kill(int(status["pid"]), signal.SIGTERM)
    return {"requested": True, "pid": int(status["pid"])}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local GigaMoney command center.")
    parser.add_argument("--config", help="configuration JSON (or set GIGAMONEY_CONFIG)")
    parser.add_argument("--host", help="address to bind (defaults to monitor.host)")
    parser.add_argument("--port", type=int, help="HTTPS port (defaults to monitor.port)")
    parser.add_argument("--redirect-http-port", type=int, help="optional HTTP port that redirects to HTTPS")
    parser.add_argument("--interval", type=int, help="health refresh interval in seconds")
    parser.add_argument("--cert-file", help="PEM TLS certificate")
    parser.add_argument("--key-file", help="PEM TLS private key")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings(args.config)
    host = str(args.host or settings.get("monitor.host") or "127.0.0.1")
    port = int(args.port if args.port is not None else settings.get("monitor.port", 8787))
    redirect_http_port = int(
        args.redirect_http_port if args.redirect_http_port is not None
        else settings.get("monitor.redirectHttpPort", 0)
    )
    interval = int(args.interval if args.interval is not None else settings.get("monitor.intervalSeconds", 60))
    certificate = _configured_path(settings.root, args.cert_file or settings.get("monitor.certFile"))
    private_key = _configured_path(settings.root, args.key_file or settings.get("monitor.keyFile"))
    if (
        not 1 <= port <= 65535
        or redirect_http_port < 0
        or redirect_http_port > 65535
        or redirect_http_port == port
        or not 10 <= interval <= 86400
    ):
        print("Port must be 1-65535 and interval must be 10-86400 seconds.", file=sys.stderr)
        return 2
    if not certificate or not certificate.is_file() or not private_key or not private_key.is_file():
        print("HTTPS requires an existing --cert-file and --key-file.", file=sys.stderr)
        return 2
    if not all((WEB_ROOT / name).is_file() for name in ("index.html", "app.js", "styles.css", "login.html", "login.js")):
        print(f"Command-center assets are missing from {WEB_ROOT}.", file=sys.stderr)
        return 2
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    controller = GigaMoneyController(settings)
    monitor = StatusMonitor(controller, interval)
    public_host = str(settings.get("monitor.publicHost") or "").strip()
    if not public_host:
        public_host = detect_machine_ipv4() or ("localhost" if host in {"0.0.0.0", "::"} else host)
    redirect_server: RedirectServer | None = None
    redirect_thread: threading.Thread | None = None
    try:
        admin_password = str(settings.get("monitor.adminPassword") or "")
        if not admin_password:
            raise ValueError("monitor.adminPassword must be set in the live config.")
        server = GigaMoneyServer((host, port), monitor, admin_password=admin_password, settings=settings)
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.minimum_version = ssl.TLSVersion.TLSv1_2
        tls_context.options |= ssl.OP_NO_COMPRESSION
        tls_context.load_cert_chain(certfile=certificate, keyfile=private_key)
        server.tls_context = tls_context
        server.is_tls = True
        if redirect_http_port:
            redirect_server = RedirectServer((host, redirect_http_port), public_host, port)
    except (OSError, ssl.SSLError) as exc:
        if "server" in locals():
            server.server_close()
        if redirect_server:
            redirect_server.server_close()
        print(f"Could not start command center: {exc}", file=sys.stderr)
        return 1
    url = https_url(public_host, server.server_port)
    write_json(settings.work_dir / "monitor.pid.json", {
        "pid": os.getpid(), "startedAt": utc_now().isoformat(), "url": url,
        "config": str(settings.path),
    })
    monitor.start()
    if redirect_server:
        redirect_thread = threading.Thread(
            target=redirect_server.serve_forever,
            kwargs={"poll_interval": 0.5},
            name="gigamoney-http-redirect",
            daemon=True,
        )
        redirect_thread.start()
    stopping = threading.Event()

    def request_shutdown(signum, frame) -> None:
        if stopping.is_set():
            return
        stopping.set()
        threading.Thread(target=server.shutdown, name="gigamoney-monitor-stop", daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    print(f"GigaMoney Command Center: {url}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        if redirect_server:
            redirect_server.shutdown()
            redirect_server.server_close()
        if redirect_thread:
            redirect_thread.join(timeout=5)
        monitor.stop()
        record = read_json(settings.work_dir / "monitor.pid.json")
        if record.get("pid") == os.getpid():
            (settings.work_dir / "monitor.pid.json").unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
