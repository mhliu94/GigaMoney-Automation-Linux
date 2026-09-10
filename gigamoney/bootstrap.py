"""Create an account-bound session from that account's verified emulator.

Capture inspection and credential extraction are local. No order is sent, and
phone verification remains an explicit action in the app.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import sys
import subprocess
import threading
import time

from .api import API_HOSTS, GENERATED_HEADERS, READ_PATHS, SessionError, load_session, save_session
from .config import find_adb, load_settings
from .signing import signature, token_expiry


def capture_records(path):
    from mitmproxy.io import FlowReader
    from mitmproxy.exceptions import FlowReadException
    def payload(message):
        try:
            value = json.loads(message.content)
            return value if isinstance(value, dict) else {}
        except (ValueError, TypeError, AttributeError):
            return {}
    with Path(path).open("rb") as handle:
        try:
            for flow in FlowReader(handle).stream():
                if flow.type == "http" and flow.request.pretty_host in API_HOSTS and flow.response:
                    yield {"time": flow.request.timestamp_start, "host": flow.request.pretty_host,
                           "method": flow.request.method, "path": flow.request.path,
                           "headers": {k.lower(): v for k, v in flow.request.headers.items()}, "body": payload(flow.request),
                           "status": flow.response.status_code, "response": payload(flow.response)}
        except FlowReadException:
            raise SessionError("Capture is incomplete; flush or stop capture before importing it.") from None


def inspect_records(records):
    routes = set()
    for row in records:
        if row["status"] != 200 or row["response"].get("code") != 0:
            continue
        headers = row["headers"]
        data = row["response"].get("data", {})
        channel = data.get("channel_infos", {}) if isinstance(data, dict) else {}
        route = (headers.get("x-target-aaid") or str(channel.get("aaid") or ""),
                 headers.get("x-target-org-id") or str(channel.get("org_id") or ""),
                 headers.get("account-channel") or str(channel.get("account_channel") or ""))
        if all(route):
            routes.add(route)
        if isinstance(data, dict):
            for row in [*data.get("orders", []), *(data.get("status", []) if isinstance(data.get("status"), list) else [])]:
                if not isinstance(row, dict):
                    continue
                route = (str(row.get("aaid") or ""), str(row.get("org_id") or row.get("org", {}).get("org_id") or ""),
                         str(row.get("account_channel") or ""))
                if all(route):
                    routes.add(route)
    return [{"accountAaid": aaid, "orgId": org, "accountChannel": channel}
            for aaid, org, channel in sorted(routes)]


def build_session(records, profile, settings, *, credentials=None, now=None):
    now = time.time() if now is None else now
    secret = profile.get("api_secret")
    if not isinstance(secret, str) or not secret:
        raise SessionError("The private app profile needs the reviewed signing secret for this app version.")
    route = {header: str(settings.require(key)) for header, key in (
        ("x-target-aaid", "api.accountAaid"), ("x-target-org-id", "api.orgId"),
        ("account-channel", "api.accountChannel"))}
    maximum_age = float(settings.get("api.bootstrap.maxCaptureAgeSeconds", 1800))
    records = [row for row in records if 0 <= now - row["time"] <= maximum_age]
    candidates = []
    pairs = {"main": {}, "trade": {}}
    tags = set()
    for row in records:
        if row["host"] not in API_HOSTS or row["status"] != 200 or row["response"].get("code") != 0:
            continue
        headers, response = row["headers"], row["response"].get("data", {})
        if not isinstance(response, dict):
            continue
        path = row["path"].split("?", 1)[0]
        signed = False
        try:
            signed = hmac.compare_digest(signature(secret, row["method"], row["path"],
                headers["x-timestamp"], headers["authorization"], row["body"] if row["method"] != "GET" else None),
                headers["x-api-signature"])
        except (ValueError, KeyError):
            pass
        if signed and path.startswith(("/auth/", "/member/", "/v1/member/", "/v3/member/")):
            for kind, key in (("main", "credentials"), ("trade", "token")):
                tokens = response.get(key)
                if isinstance(tokens, dict) and all(isinstance(tokens.get(k), str) for k in ("token", "refresh_token")):
                    pairs[kind][tokens["token"]] = tokens["refresh_token"]
        if (signed and path == "/v3/member/trade/auth" and row["body"].get("android_device_tag") and
                (headers.get("x-target-aaid") == route["x-target-aaid"] or str(response.get("aaid")) == route["x-target-aaid"])):
            tags.add(row["body"]["android_device_tag"])
        if (row["method"] == "GET" and path in {READ_PATHS[k] for k in ("positions", "cash", "orders")} and
                headers.get("x-target-aaid") == route["x-target-aaid"] and
                headers.get("x-target-org-id") in (None, route["x-target-org-id"]) and
                headers.get("account-channel") == route["account-channel"]):
            try:
                computed = signature(secret, row["method"], row["path"], headers["x-timestamp"],
                                     headers["authorization"], None)
                if hmac.compare_digest(computed, headers["x-api-signature"]):
                    candidates.append(row)
            except (ValueError, KeyError):
                pass
    if not candidates:
        raise SessionError("No recent successful signed account read matches this account/profile. Refresh Orders in the verified app and capture again.")
    selected = max(candidates, key=lambda row: row["time"])
    headers = selected["headers"]
    access, trade = headers.get("authorization"), headers.get("x-authorization")
    if credentials:
        if credentials.get("authorization") != access or credentials.get("trade_token") != trade:
            raise SessionError("Emulator credentials changed after the captured read. Refresh Orders and capture again.")
        refresh, trade_refresh = credentials.get("refresh_token"), credentials.get("trade_refresh_token")
    else:
        refresh, trade_refresh = pairs["main"].get(access), pairs["trade"].get(trade)
    if len(tags) != 1:
        raise SessionError("Capture must include successful trade authentication from exactly one device for this account.")
    for token in (refresh, trade_refresh):
        try:
            if token_expiry(token) <= now:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise SessionError("Refresh credentials are missing or expired. Export them from this account's verified emulator.") from None
    password = str(settings.require("gigamoney.tradePassword"))
    return {"version": 1, "account_id": str(settings.require("gigamoney.accountId")),
            "host": selected["host"], "api_secret": secret,
            "authorization": access, "refresh_token": refresh,
            "trade_token": trade, "trade_refresh_token": trade_refresh,
            "headers": {**{k: v for k, v in headers.items() if k not in GENERATED_HEADERS}, **route},
            "templates": {}, "updated_at": now,
            "trade_auth": {"type": "trade", "android_device_tag": next(iter(tags)),
                           "password_md5": hashlib.md5(password.encode()).hexdigest()}}


def emulator_credentials(settings, profile):
    """Read the logged-in app's credential getters; never invoke login or trading."""
    import frida
    import frida_tools
    serial = settings.require("adb.serial")
    device = frida.get_device(serial, timeout=float(settings.get("adb.commandTimeoutSeconds", 30)))
    process = subprocess.run([find_adb(settings.get("adb.path", ""), root=settings.root), "-s", serial, "shell", "pidof",
                              settings.get("gigamoney.package", "lb.whale.hkwinner.android")],
                             capture_output=True, text=True, timeout=float(settings.get("adb.commandTimeoutSeconds", 30)))
    pids = process.stdout.split()
    if process.returncode or len(pids) != 1 or not pids[0].isdigit():
        raise SessionError("Open the verified GigaMoney app on the configured emulator before importing its session.")
    session = device.attach(int(pids[0]))
    bridge = Path(frida_tools.__file__).parent / "bridges/java.js"
    source = bridge.read_text() + "\nglobalThis.Java = bridge;\n" + """
Java.perform(function () {
    try {
        const config = Java.use(ENGINE_CLASS).$new();
        const result = {};
        for (const key of ['token', 'refresh_token', 'trade_token', 'trade_refresh_token']) {
            const value = config.get(key);
            result[key === 'token' ? 'authorization' : key] = value === null ? null : String(value);
        }
        send({event: 'credentials', data: result});
    } catch (_) { send({event: 'failed'}); }
});
""".replace("ENGINE_CLASS", json.dumps(profile.get("engineConfigClass", "com.longbridge.common.rust.b")))
    ready, result = threading.Event(), {}
    def receive(message, _):
        payload = message.get("payload", {})
        if message.get("type") == "send" and payload.get("event") == "credentials":
            result.update(payload["data"])
        ready.set()
    try:
        script = session.create_script(source)
        script.on("message", receive)
        script.load()
        if not ready.wait(float(settings.get("adb.commandTimeoutSeconds", 30))) or not result:
            raise SessionError("Could not read the verified emulator session; check the app profile/version and instrumentation setup.")
        return result
    finally:
        session.detach()


def require_stopped(settings):
    path = settings.work_dir / "runner.pid.json"
    if path.exists():
        try:
            record = json.loads(path.read_text())
            os.kill(int(record["pid"]), 0)
        except (OSError, ValueError, KeyError):
            return
        raise SessionError("Stop the runner before importing account credentials.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("action", choices=("inspect", "import", "export-profile"))
    parser.add_argument("--from-emulator", action="store_true")
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.config)
        profile_path = settings.resolve_path(settings.require("api.bootstrap.appProfilePath"))
        if args.action == "export-profile":
            if profile_path.exists():
                raise SessionError("App profile already exists; choose a new api.bootstrap.appProfilePath.")
            state = load_session(settings.session_path)
            # The profile contains app-version signing material, never account
            # tokens, device identity, routing, or trading-password credentials.
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            with open(profile_path, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as handle:
                json.dump({"version": 1, "api_secret": state["api_secret"],
                           "engineConfigClass": "com.longbridge.common.rust.b"}, handle, indent=2)
            print(json.dumps({"ok": True, "profile": str(profile_path), "account_credentials_exported": False}))
            return 0
        source = settings.resolve_path(settings.require("api.bootstrap.capturePath"))
        records = list(capture_records(source))
        if args.action == "inspect":
            print(json.dumps({"routes": inspect_records(records)}))
            return 0
        require_stopped(settings)
        if settings.session_path.exists():
            previous = load_session(settings.session_path)
            if previous.get("account_id") != settings.get("gigamoney.accountId"):
                raise SessionError("The destination session belongs to a different or unbound account; choose a separate sessionPath/workDir.")
        if profile_path.stat().st_mode & 0o077:
            raise SessionError("App profile must be private (mode 600).")
        profile = json.loads(profile_path.read_text())
        credentials = emulator_credentials(settings, profile) if args.from_emulator else None
        state = build_session(records, profile, settings, credentials=credentials)
        # Empty templates use the current endpoint definitions and configured
        # routing, never another account's captured query strings.
        state["templates"] = {name: {"path": READ_PATHS[name]} for name in ("accounts", "orders", "positions", "cash", "trade_check")}
        save_session(settings.session_path, state)
        print(json.dumps({"ok": True, "account_id": state["account_id"], "session": str(settings.session_path),
                          "network_requests_sent": 0}))
        return 0
    except Exception as error:
        print(json.dumps({"ok": False, "error_type": type(error).__name__,
                          "message": str(error) if isinstance(error, SessionError) else "Account setup failed; check configuration and local capture prerequisites."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
