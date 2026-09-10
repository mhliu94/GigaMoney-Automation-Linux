#!/usr/bin/env python3
"""Keep the capture server certificate in GigaMoney's Rust client pin list."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import queue
import signal
import ssl
import subprocess
import threading
import time

import frida
import frida_tools

ROOT = Path(__file__).resolve().parent.parent
PROXY_DIR = Path(os.environ.get("GIGAMONEY_PROXY_STATE_DIR", ROOT / "work/proxy"))
STATE = PROXY_DIR / "trust.json"
PACKAGE = os.environ["GIGAMONEY_CAPTURE_PACKAGE"]
SERIAL = os.environ["GIGAMONEY_CAPTURE_SERIAL"]
ADB = os.environ["GIGAMONEY_CAPTURE_ADB"]
STOP = threading.Event()
WRITE_LOCK = threading.Lock()


def state(phase, **fields):
    record = {"phase": phase, "time": time.time(), "supervisor_pid": os.getpid(), **fields}
    with WRITE_LOCK:
        temporary = STATE.with_suffix(".tmp")
        temporary.write_text(json.dumps(record))
        temporary.replace(STATE)
    print(json.dumps(record), flush=True)


def adb(*arguments):
    return subprocess.run([ADB, "-s", SERIAL, *arguments], capture_output=True,
                          text=True, timeout=10, stdin=subprocess.DEVNULL)


def main():
    os.umask(0o077)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: STOP.set())
    bridge = Path(frida_tools.__file__).parent / "bridges/java.js"
    certificate = ssl.PEM_cert_to_DER_cert((PROXY_DIR / "ca/api-cert-public.pem").read_text())
    script_body = (ROOT / "scripts/proxy_trust.js").read_text().replace(
        "__CAPTURE_CA_DER__", json.dumps(base64.b64encode(certificate).decode()))
    source = bridge.read_text() + "\nglobalThis.Java = bridge;\n" + script_body
    launch = os.environ.get("GIGAMONEY_CAPTURE_LAUNCH") == "1"
    while not STOP.is_set():
        device = None
        sessions = {}
        pending = queue.Queue()
        lost = threading.Event()
        try:
            state("connecting")
            # Frida only listens inside the emulator. No host port is exposed.
            if not adb("shell", "pidof", "gigamoney-frida-server").stdout.strip():
                started = adb("shell", "/debug_ramdisk/su -c '/data/adb/gigamoney-proxy/gigamoney-frida-server -D -l 127.0.0.1:27042' </dev/null >/dev/null 2>&1 &")
                if started.returncode:
                    raise RuntimeError("Could not start the emulator instrumentation service")
                for _ in range(20):
                    if adb("shell", "pidof", "gigamoney-frida-server").stdout.strip():
                        break
                    if STOP.wait(.2):
                        return
            device = frida.get_device(SERIAL, timeout=3)
            device.on("lost", lambda: lost.set())
            device.on("spawn-added", lambda spawn: pending.put(spawn))
            device.enable_spawn_gating()
            state("waiting_for_app")

            def resume(pid):
                # Spawn notifications can outlive a short-lived process or refer
                # to the app we already resumed during explicit startup.
                try:
                    device.resume(pid)
                except (frida.ProcessNotFoundError, frida.InvalidArgumentError, frida.InvalidOperationError):
                    pass

            def attach(pid, resume=False):
                if pid in sessions:
                    return
                session = device.attach(pid)
                script = session.create_script(source)
                installed = threading.Event()

                def message(data, _binary):
                    event = data.get("payload", {}).get("event") if data.get("type") == "send" else None
                    if event == "rust_ca_hook_installed":
                        installed.set()
                        state("hook_installed", app_pid=pid)
                    elif event == "rust_ca_applied":
                        state("ready", app_pid=pid)
                    elif data.get("type") == "error":
                        state("hook_error", app_pid=pid, error=data.get("description"))

                script.on("message", message)
                script.load()
                sessions[pid] = (session, script)
                if resume:
                    device.resume(pid)
                if not installed.wait(10):
                    raise RuntimeError("GigaMoney certificate hook did not become ready")

            if launch:
                adb("shell", "am", "force-stop", PACKAGE)
                attach(device.spawn([PACKAGE]), resume=True)
                launch = False
            while not STOP.wait(.2) and not lost.is_set():
                while not pending.empty():
                    spawn = pending.get_nowait()
                    if spawn.identifier == PACKAGE:
                        try:
                            attach(spawn.pid, resume=True)
                        except frida.ProcessNotFoundError:
                            pass
                        except Exception:
                            resume(spawn.pid)
                            raise
                    else:
                        resume(spawn.pid)
                result = adb("shell", "pidof", PACKAGE)
                pids = {int(p) for p in result.stdout.split() if p.isdigit()}
                for pid in set(sessions) - pids:
                    session, _script = sessions.pop(pid)
                    try:
                        session.detach()
                    except Exception:
                        pass
                    state("waiting_for_app")
                for pid in pids - set(sessions):
                    try:
                        attach(pid)
                    except frida.ProcessNotFoundError:
                        pass
        except Exception as exc:
            state("disconnected", error=str(exc))
        finally:
            if device is not None:
                try:
                    device.disable_spawn_gating()
                    for spawn in device.enumerate_pending_spawn():
                        device.resume(spawn.pid)
                except Exception:
                    pass
            for session, _script in list(sessions.values()):
                try:
                    session.detach()
                except Exception:
                    pass
        if not STOP.is_set():
            STOP.wait(2)
    state("stopped")


if __name__ == "__main__":
    main()
