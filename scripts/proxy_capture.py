"""mitmproxy addon: metadata plus durable messages for long-lived connections.

Full HTTP requests/responses are saved by mitmproxy's -w option. All output is
private runtime data; nothing here changes or replays a request.
"""
from __future__ import annotations

import base64
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

from mitmproxy import ctx


class Capture:
    def __init__(self):
        self.events = None
        self.messages = None
        self.watcher = None
        self.directory = None
        self.target_key = None
        self.target_since = None
        self.api_mode = False

    def running(self):
        directory = Path(os.environ["GIGAMONEY_CAPTURE_DIR"])
        self.directory = directory
        self.events = (directory / "events.jsonl").open("a", buffering=1)
        self.messages = (directory / "messages.jsonl").open("a", buffering=1)
        self.api_mode = os.environ.get("GIGAMONEY_CAPTURE_MODE") == "api"
        self.emit("capture_started")
        if self.api_mode:
            self.target_since = datetime.now(timezone.utc).timestamp()
        else:
            self.watcher = asyncio.create_task(self.watch_emulator(directory))

    async def watch_emulator(self, directory):
        # Match process IDs: emulator network threads have names such as
        # slirp_poll, so matching only the netsimd name misses their sockets.
        executables = set(json.loads(os.environ["GIGAMONEY_CAPTURE_EXECUTABLES"]))
        while True:
            pids = []
            for process in Path("/proc").iterdir():
                if process.name.isdigit():
                    try:
                        if str((process / "exe").resolve(strict=True)) in executables:
                            pids.append(int(process.name))
                    except OSError:
                        pass
            # Let the app connect normally if its certificate hook is unavailable.
            # This is explicitly logged as a capture gap, rather than breaking TLS.
            trust_path = directory.parent.parent / "trust.json"
            try:
                trust = json.loads(trust_path.read_text())
                owner = Path(f"/proc/{int(trust['supervisor_pid'])}/cmdline").read_bytes()
                ready = trust.get("phase") == "ready" and b"proxy_trust.py" in owner
            except (OSError, ValueError, KeyError):
                ready = False
            selected = sorted(pids) if ready else []
            mode = "local:" + ",".join(map(str, selected or [2147483647]))
            app_pid = trust.get("app_pid") if ready else None
            key = (tuple(selected), app_pid)
            if key != self.target_key:
                if ctx.options.mode != [mode]:
                    ctx.options.update(mode=[mode])
                self.target_key = key
                self.target_since = datetime.now(timezone.utc).timestamp()
                self.emit("capture_targets", pids=selected, app_pid=app_pid,
                          transport_ready=bool(selected), capture_ready=False)
                temporary = directory / "targets.tmp"
                temporary.write_text(json.dumps({"pids": selected, "mode": mode,
                                                "app_pid": app_pid,
                                                "target_since": self.target_since}))
                temporary.replace(directory / "targets.json")
            await asyncio.sleep(1)

    def emit(self, event, flow=None, **fields):
        if self.events is None:
            return
        record = {"time": datetime.now(timezone.utc).isoformat(), "event": event}
        if flow is not None:
            record.update(flow_id=flow.id, protocol=flow.type)
            if getattr(flow, "request", None) is not None:
                request = flow.request
                record.update(method=request.method, host=request.pretty_host,
                              path=request.path.split("?", 1)[0])
                record["request_id"] = getattr(request, "headers", {}).get("x-request-id")
                record["command_id"] = getattr(flow, "metadata", {}).get("gigamoney_command_id")
            else:
                record["server"] = flow.server_conn.address
        record.update(fields)
        self.events.write(json.dumps(record, separators=(",", ":")) + "\n")

    def request(self, flow):
        if self.api_mode:
            command_id = flow.request.headers.pop("x-gigamoney-command-id", None)
            flow.metadata["gigamoney_command_id"] = unquote(command_id) if command_id is not None else None
        self.emit("request", flow, bytes=len(flow.request.raw_content or b""))

    def response(self, flow):
        try:
            payload = json.loads(flow.response.content)
            api_code = payload.get("code") if isinstance(payload, dict) else None
        except (ValueError, TypeError, AttributeError):
            api_code = None
        self.emit("response", flow, status=flow.response.status_code,
                  api_code=api_code,
                  bytes=len(flow.response.raw_content or b""),
                  elapsed_ms=round(1000 * (flow.response.timestamp_end -
                                          flow.request.timestamp_start), 2))
        # A live redirector and certificate hook do not prove that existing
        # sockets pass through capture. Require a real account/order response
        # opened after the current target generation, not pings or telemetry.
        request = flow.request
        path = request.path.split("?", 1)[0]
        core_path = path.startswith(("/v6/portfolio/", "/v5/portfolio/", "/v3/account/")) or (
            "/orders/" in path and path.startswith("/v"))
        if (self.directory is None or self.target_since is None or
                request.timestamp_start < self.target_since or
                request.pretty_host not in {"api-gl.lbkrs.com", "api.lbkrs.com"} or
                not core_path or flow.response.status_code != 200):
            return
        try:
            body = json.loads(flow.response.content)
        except (ValueError, TypeError):
            return
        if not isinstance(body, dict) or body.get("code") != 0:
            return
        evidence = {"target_since": self.target_since,
                    "request_started": request.timestamp_start,
                    "response_ended": flow.response.timestamp_end,
                    "host": request.pretty_host, "path": path, "status": 200}
        temporary = self.directory / "api-health.tmp"
        temporary.write_text(json.dumps(evidence))
        temporary.replace(self.directory / "api-health.json")

    def error(self, flow):
        self.emit("error", flow, error=str(flow.error))

    def tls_failed_client(self, data):
        self.emit("tls_failed_client", host=data.context.client.sni,
                  error=str(data.context.client.error))

    def tls_failed_server(self, data):
        self.emit("tls_failed_server", host=data.context.server.sni,
                  error=str(data.context.server.error))

    def save_message(self, flow, message):
        if self.messages is None:
            return
        record = {"timestamp": message.timestamp, "flow_id": flow.id,
                  "protocol": "websocket" if getattr(flow, "websocket", None) else flow.type,
                  "from_client": message.from_client,
                  "server": flow.server_conn.address,
                  "content_base64": base64.b64encode(message.content).decode("ascii")}
        self.messages.write(json.dumps(record, separators=(",", ":")) + "\n")

    def websocket_start(self, flow):
        self.emit("websocket_start", flow)

    def websocket_message(self, flow):
        self.save_message(flow, flow.websocket.messages[-1])

    def websocket_end(self, flow):
        self.emit("websocket_end", flow)

    def tcp_start(self, flow):
        self.emit("tcp_start", flow)

    def tcp_message(self, flow):
        self.save_message(flow, flow.messages[-1])

    def tcp_end(self, flow):
        self.emit("tcp_end", flow)

    def tcp_error(self, flow):
        self.error(flow)

    def udp_message(self, flow):
        self.save_message(flow, flow.messages[-1])

    def udp_error(self, flow):
        self.error(flow)

    def done(self):
        if self.watcher is not None:
            self.watcher.cancel()
        self.emit("capture_stopped")
        for output in (self.events, self.messages):
            if output is not None:
                output.close()


addons = [Capture()]
