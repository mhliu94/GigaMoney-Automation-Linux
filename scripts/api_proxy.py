#!/usr/bin/env python3
"""Manage private HTTP capture for the emulator-free API client.

The loopback reverse proxy records requests/responses and uses verified TLS to
one fixed upstream host. It never produces or replays trading commands.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "work/proxy"
sys.path.insert(0, str(ROOT))
from gigamoney.api import ApiClient, SessionError, save_session
from gigamoney.config import load_settings

CONFIG = None


def settings():
    return load_settings(CONFIG)


def executable():
    return settings().proxy_python.with_name("mitmdump") if settings().get("proxy.pythonPath") else STATE / "venv/bin/mitmdump"


def read_state():
    path = STATE / "api-capture.json"
    return json.loads(path.read_text()) if path.exists() else {}


def is_running(state):
    try:
        command = Path(f"/proc/{int(state['pid'])}/cmdline").read_bytes().split(b"\0")
        return (str(executable()).encode() in command and
                str(Path(state["directory"]) / "flows.mitm").encode() in command)
    except (OSError, ValueError, KeyError):
        return False


def session_path():
    return settings().session_path


def client():
    value = ApiClient.from_settings(settings())
    value.session_path = session_path()
    return value


def status():
    state = read_state()
    running = is_running(state)
    configured = False
    try:
        with client()._session() as session:
            configured = session.get("capture_proxy") == state.get("route") and bool(state.get("route"))
    except SessionError:
        pass
    evidence = {}
    if state.get("directory"):
        try:
            evidence = json.loads((Path(state["directory"]) / "api-health.json").read_text())
        except (OSError, ValueError):
            pass
    verified = bool(running and configured and 0 <= time.time() - evidence.get("response_ended", 0) <= settings().get("proxy.evidenceMaxAgeSeconds", 300)
                    and evidence.get("request_started", 0) >= state.get("started_epoch", float("inf")))
    return {**state, "running": running, "route_configured": configured, "capture_ready": verified,
            "api_evidence": evidence}


def start(port=None):
    port = int(settings().get("proxy.apiPort", 18080)) if port is None else port
    previous = read_state()
    if is_running(previous):
        with client()._session() as session:
            if session["host"] != previous["route"]["upstream"] or previous["port"] != port:
                raise RuntimeError("Stop the existing API proxy before changing its port or upstream.")
            session["capture_proxy"] = previous["route"]
            save_session(session_path(), session)
        return status()
    if not 1024 <= port <= 65535:
        raise ValueError("Use a local port between 1024 and 65535.")
    program = executable()
    if not program.is_file():
        raise RuntimeError("Install the proxy environment described in docs/proxy.md.")
    with client()._session() as session:
        upstream = session["host"]
    # Detect conflicts before creating a capture or changing client routing.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    directory = STATE / "captures" / (stamp + "-api")
    directory.mkdir(mode=0o700, parents=True)
    environment = {**os.environ, "PYTHONUNBUFFERED": "1", "GIGAMONEY_CAPTURE_MODE": "api",
                   "GIGAMONEY_CAPTURE_DIR": str(directory)}
    command = [str(program), "--mode", f"reverse:https://{upstream}@127.0.0.1:{port}",
               "--set", f"confdir={STATE / 'ca'}", "--set", "keep_host_header=true",
               "--set", "connection_strategy=lazy", "--set", "flow_detail=0",
               "-s", str(ROOT / "scripts/proxy_capture.py"), "-w", str(directory / "flows.mitm")]
    started = time.time()
    with (directory / "proxy.log").open("ab", buffering=0) as log:
        process = subprocess.Popen(command, cwd=ROOT, env=environment, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True, umask=0o077)
    state = {"pid": process.pid, "directory": str(directory), "port": port,
             "started_at": datetime.fromtimestamp(started, timezone.utc).isoformat(), "started_epoch": started,
             "route": {"url": f"http://127.0.0.1:{port}", "upstream": upstream}}
    try:
        deadline = time.monotonic() + float(settings().get("proxy.startupTimeoutSeconds", 15))
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("API proxy exited during startup; inspect its private proxy.log.")
            if (directory / "events.jsonl").exists():
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.25):
                        break
                except OSError:
                    pass
            time.sleep(.1)
        else:
            raise RuntimeError("API proxy listener did not become ready.")
        save_session(STATE / "api-capture.json", state)
        with client()._session() as session:
            if session["host"] != upstream:
                raise RuntimeError("Selected account host changed during proxy startup.")
            session["capture_proxy"] = state["route"]
            save_session(session_path(), session)
    except BaseException:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            process.wait(timeout=10)
        raise
    return status()


def stop():
    state = read_state()
    # Serialize with in-flight API calls, then restore direct verified HTTPS.
    with client()._session() as session:
        if session.get("capture_proxy") == state.get("route"):
            session.pop("capture_proxy", None)
            save_session(session_path(), session)
    if is_running(state):
        os.kill(int(state["pid"]), signal.SIGINT)
        deadline = time.monotonic() + float(settings().get("proxy.startupTimeoutSeconds", 15))
        while is_running(state) and time.monotonic() < deadline:
            time.sleep(.1)
        if is_running(state):
            raise RuntimeError("API proxy did not stop; inspect its private log.")
    return status()


def report():
    state = status()
    counts, errors, totals, latest = Counter(), Counter(), Counter(), None
    events = Path(state["directory"]) / "events.jsonl" if state.get("directory") else None
    if events and events.exists():
        with events.open() as stream:
            for line in stream:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get("event") == "response":
                    key = (row["method"], row["path"], row["status"], row.get("api_code"))
                    counts[key] += 1
                    totals[key] += row["elapsed_ms"]
                    latest = row
                elif "error" in row.get("event", "") or "failed" in row.get("event", ""):
                    errors[row["event"]] += 1
    return {**state, "responses": sum(counts.values()), "errors": dict(errors), "last_response": latest,
            "endpoints": [{"method": k[0], "path": k[1], "http_status": k[2], "api_code": k[3],
                           "count": n, "mean_ms": round(totals[k] / n, 2)} for k, n in counts.items()]}


def main(argv=None):
    global CONFIG, STATE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status", "report"))
    parser.add_argument("--port", type=int)
    parser.add_argument("--config")
    args = parser.parse_args(argv)
    CONFIG = args.config
    STATE = settings().proxy_dir
    os.umask(0o077)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (STATE / "api-control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            result = start(args.port) if args.action == "start" else globals()[args.action]()
        except (SessionError, RuntimeError, OSError, ValueError) as error:
            print(json.dumps({"ok": False, "error_type": type(error).__name__,
                              "message": str(error) if isinstance(error, (SessionError, RuntimeError)) else "API proxy operation failed."}))
            return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
