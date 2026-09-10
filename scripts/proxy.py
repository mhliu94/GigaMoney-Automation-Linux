#!/usr/bin/env python3
"""Manage the private GigaMoney emulator traffic capture on this Linux host."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "work" / "proxy"
sys.path.insert(0, str(ROOT))

from gigamoney.adb import Adb
from gigamoney.config import load_settings
from gigamoney.lock import EmulatorLock
from gigamoney.runner import runner_status

CONFIG = None


def settings_for_capture():
    return load_settings(CONFIG)


def executable():
    configured = settings_for_capture().get("proxy.pythonPath")
    return settings_for_capture().proxy_python.with_name("mitmdump") if configured else STATE / "venv/bin/mitmdump"


def read_state():
    path = STATE / "capture.json"
    return json.loads(path.read_text()) if path.exists() else {}


def is_running(state):
    try:
        cmdline = Path(f"/proc/{int(state['pid'])}/cmdline").read_bytes().split(b"\0")
        return (str(executable()).encode() in cmdline and
                str(Path(state["directory"]) / "flows.mitm").encode() in cmdline)
    except (OSError, KeyError, ValueError):
        return False


def status():
    state = read_state()
    if state:
        targets = Path(state["directory"]) / "targets.json"
        if targets.exists():
            state.update(json.loads(targets.read_text()))
    trust = read_trust()
    running = is_running(state)
    transport_ready = (running and trust.get("phase") == "ready"
                       and bool(state.get("pids"))
                       and trust_running(trust.get("supervisor_pid")))
    evidence = {}
    cutoff = state.get("target_since")
    if state.get("directory"):
        directory = Path(state["directory"])
        try:
            evidence = json.loads((directory / "api-health.json").read_text())
        except (OSError, ValueError):
            pass
        try:
            cutoff = max(cutoff or 0, json.loads((directory / "reconnect.json").read_text())["started"])
        except (OSError, ValueError, KeyError):
            pass
    verified = bool(transport_ready and cutoff and
                    state.get("app_pid") == trust.get("app_pid") and
                    evidence.get("target_since") == state.get("target_since") and
                    evidence.get("request_started", 0) >= cutoff and
                    0 <= time.time() - evidence.get("response_ended", 0) <= settings_for_capture().get("proxy.evidenceMaxAgeSeconds", 300))
    return {**state, "running": running, "trust": trust,
            "transport_ready": bool(transport_ready), "capture_ready": verified,
            "api_evidence": evidence,
            "capture_check": ("Recent account/order response captured." if verified else
                              "Open Orders or refresh account data to verify capture; reconnect if no API response appears.")}


def read_trust():
    path = STATE / "trust.json"
    return json.loads(path.read_text()) if path.exists() else {}


def trust_running(pid):
    try:
        command = Path(f"/proc/{int(pid)}/cmdline").read_bytes().split(b"\0")
        return str(ROOT / "scripts/proxy_trust.py").encode() in command
    except (OSError, TypeError, ValueError):
        return False


def stop_trust(pid):
    if trust_running(pid):
        os.kill(int(pid), signal.SIGTERM)
        deadline = time.monotonic() + float(settings_for_capture().get("proxy.startupTimeoutSeconds", 15))
        while trust_running(pid) and time.monotonic() < deadline:
            time.sleep(.2)
        if trust_running(pid):
            raise RuntimeError("Certificate supervisor has not stopped; inspect work/proxy/trust.log.")


def require_stopped_runner(settings):
    if runner_status(settings).get("running"):
        raise RuntimeError("Stop the trading runner before changing capture connections: gigamoney runner-stop --force")


def start_trust(adb, settings):
    previous = read_trust()
    stop_trust(previous.get("supervisor_pid"))
    env = {**os.environ, "GIGAMONEY_CAPTURE_PACKAGE": adb.package,
           "GIGAMONEY_CAPTURE_SERIAL": adb.serial, "GIGAMONEY_CAPTURE_ADB": str(adb.executable),
           "GIGAMONEY_CAPTURE_LAUNCH": "1", "GIGAMONEY_CONFIG": str(settings.path),
           "GIGAMONEY_PROXY_STATE_DIR": str(STATE)}
    with EmulatorLock(settings.work_dir / "emulator.lock", 30):
        with (STATE / "trust.log").open("ab", buffering=0) as log:
            process = subprocess.Popen([str(settings.proxy_python), str(ROOT / "scripts/proxy_trust.py")],
                                       cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Certificate supervisor exited; inspect work/proxy/trust.log.")
                trust = read_trust()
                if trust.get("supervisor_pid") == process.pid and trust.get("phase") == "ready":
                    return process.pid
                time.sleep(.2)
            raise RuntimeError("GigaMoney did not accept the capture certificate; inspect work/proxy/trust.log.")
        except BaseException:
            stop_trust(process.pid)
            raise


def reconnect_wifi(adb, settings):
    # Existing sockets predate the eBPF capture. Reconnect during maintenance so
    # subsequent workflow traffic opens new sockets through the proxy.
    wifi = adb.shell("cmd", "wifi", "status").lower()
    if "wifi is enabled" not in wifi and "wi-fi is enabled" not in wifi:
        raise RuntimeError("Capture startup expects emulator Wi-Fi to be enabled.")
    with EmulatorLock(settings.work_dir / "emulator.lock", 30):
        try:
            adb.shell("svc", "wifi", "disable")
            time.sleep(1)
        finally:
            adb.shell("svc", "wifi", "enable")
        time.sleep(3)


def reconnect():
    settings = settings_for_capture()
    require_stopped_runner(settings)
    current = status()
    if not current["transport_ready"]:
        raise RuntimeError("Start the proxy and certificate hook before reconnecting.")
    directory = Path(current["directory"])
    temporary = directory / "reconnect.tmp"
    temporary.write_text(json.dumps({"started": time.time()}))
    temporary.replace(directory / "reconnect.json")
    reconnect_wifi(Adb(settings), settings)
    return status()


def start():
    current = status()
    if current["running"]:
        return current
    settings = settings_for_capture()
    require_stopped_runner(settings)
    program = executable()
    if not program.exists():
        raise RuntimeError("Install the proxy environment as described in docs/proxy.md first.")
    # Refuse interception if an emulator boot lost the system trust installation.
    adb = Adb(settings)
    certificate_hash = (STATE / "certificate-hash.txt").read_text().strip()
    remote = adb.shell("cat", f"/apex/com.android.conscrypt/cacerts/{certificate_hash}.0")
    expected = (STATE / "ca/mitmproxy-ca-cert.pem").read_text()
    if remote.strip() != expected.strip():
        raise RuntimeError("The emulator does not trust this proxy certificate.")
    trust_pid = start_trust(adb, settings)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    directory = STATE / "captures" / stamp
    directory.mkdir(parents=True, mode=0o700)
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "GIGAMONEY_CAPTURE_DIR": str(directory)}
    sdk = Path(adb.executable).resolve().parent.parent
    env["GIGAMONEY_CAPTURE_EXECUTABLES"] = json.dumps([
        str(sdk / "emulator/netsimd"),
        str(sdk / "emulator/qemu/linux-x86_64/qemu-system-x86_64"),
    ])
    # Ubuntu's sudo-rs emits an unsupported -E warning which mitmproxy treats
    # as a startup error. Select the installed original sudo for this child only.
    if Path("/usr/bin/sudo.ws").exists():
        bindir = STATE / "bin"
        bindir.mkdir(exist_ok=True)
        sudo = bindir / "sudo"
        if not sudo.exists():
            sudo.symlink_to("/usr/bin/sudo.ws")
        env["PATH"] = str(bindir) + os.pathsep + env.get("PATH", "")
    command = [str(program), "--mode", "local:2147483647",
               "--set", f"confdir={STATE / 'ca'}", "--set", "flow_detail=0",
               "--certs", f"*.lbkrs.com={STATE / 'ca/api-cert.pem'}",
               "--ignore-hosts", r"^(127\.|\[?::1\]?:)",
               "-s", str(ROOT / "scripts/proxy_capture.py"),
               "-w", str(directory / "flows.mitm")]
    with (directory / "proxy.log").open("ab", buffering=0) as log:
        process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT,
                                   env=env, start_new_session=True)
    state = {"pid": process.pid, "directory": str(directory),
             "trust_pid": trust_pid,
             "started_at": datetime.now(timezone.utc).isoformat(),
             "mode": "emulator processes (PID discovery every second)"}
    temporary = STATE / "capture.tmp"
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    temporary.replace(STATE / "capture.json")
    deadline = time.monotonic() + float(settings_for_capture().get("proxy.startupTimeoutSeconds", 15))
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Proxy exited; inspect {directory / 'proxy.log'}")
            events = directory / "events.jsonl"
            if events.exists() and '"capture_started"' in events.read_text():
                time.sleep(1)
                if process.poll() is None:
                    reconnect_wifi(adb, settings)
                    for _ in range(75):
                        current = status()
                        if current["transport_ready"]:
                            return current
                        if process.poll() is not None:
                            break
                        time.sleep(.2)
                    raise RuntimeError("Capture did not become ready after reconnecting; inspect work/proxy/trust.log.")
            time.sleep(.2)
        raise RuntimeError(f"Proxy startup timed out; inspect {directory / 'proxy.log'}")
    except BaseException:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            process.wait(timeout=10)
        stop_trust(trust_pid)
        raise


def stop():
    require_stopped_runner(settings_for_capture())
    state = read_state()
    if is_running(state):
        os.kill(int(state["pid"]), signal.SIGINT)
        deadline = time.monotonic() + float(settings_for_capture().get("proxy.startupTimeoutSeconds", 15))
        while is_running(state) and time.monotonic() < deadline:
            time.sleep(.2)
        if is_running(state):
            raise RuntimeError("Proxy has not stopped; inspect its log before retrying.")
    stop_trust(state.get("trust_pid"))
    return {**state, "running": False}


def report():
    state = read_state()
    if not state:
        return {"running": False, "responses": 0}
    events = Path(state["directory"]) / "events.jsonl"
    endpoints = Counter()
    errors = Counter()
    times = {}
    if events.exists():
        for line in events.read_text().splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue  # The active writer may not have completed its last line.
            if row["event"] == "response":
                key = (row["method"], row["host"], row["path"], row["status"])
                endpoints[key] += 1
                times.setdefault(key, []).append(row["elapsed_ms"])
            elif "error" in row["event"] or "failed" in row["event"]:
                errors[(row["event"], row.get("host", ""))] += 1
    return {**status(), "responses": sum(endpoints.values()),
            "endpoints": [dict(method=k[0], host=k[1], path=k[2], status=k[3],
                               count=n, mean_ms=round(sum(times[k]) / n, 2))
                          for k, n in endpoints.most_common()],
            "errors": [dict(event=k[0], host=k[1], count=n)
                       for k, n in errors.most_common()]}


def main():
    global CONFIG, STATE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("action", choices=("start", "stop", "status", "report", "reconnect"))
    args = parser.parse_args()
    CONFIG = args.config
    STATE = settings_for_capture().proxy_dir
    os.umask(0o077)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    STATE.chmod(0o700)
    with (STATE / "control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        result = globals()[args.action]()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
