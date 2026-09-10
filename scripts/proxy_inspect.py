#!/usr/bin/env python3
"""Inspect order evidence, or probe one captured read-only request from Linux.

This is not a trading client: it cannot submit, replace, or cancel an order,
generate signatures, refresh credentials, or follow HTTP redirects.
Run with work/proxy/venv/bin/python to use the capture reader.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from gigamoney.config import load_settings

API_HOSTS = {"api-gl.lbkrs.com", "api.lbkrs.com"}
READ_PATHS = {"accounts": "/v3/account/list", "orders": "/v5/orders/today",
              "positions": "/v6/portfolio/detail", "cash": "/v5/portfolio/cash_info"}


def is_order_endpoint(path):
    # The app's replacement endpoint lives outside /orders/.
    return "/orders/" in path or path == "/v1/tube/replace"


def utc(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def flows(path):
    from mitmproxy.io import FlowReader
    from mitmproxy.exceptions import FlowReadException

    with path.open("rb") as stream:
        try:
            yield from FlowReader(stream).stream()
        except FlowReadException as exc:
            # Do not silently treat corrupt or truncated evidence as complete.
            raise RuntimeError("Capture ends in an unreadable flow; retry or stop capture to flush it.") from exc


def payload(message):
    try:
        result = json.loads(message.content)
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError, AttributeError):
        return {}


def order_evidence(source):
    records = []
    for flow in source:
        if flow.type != "http":
            continue
        request = flow.request
        path = request.path.split("?", 1)[0]
        if request.pretty_host in API_HOSTS and is_order_endpoint(path) and request.method != "GET":
            response = payload(flow.response)
            records.append({"source": "full_http", "time": utc(request.timestamp_start),
                            "method": request.method, "path": path,
                            "request_fields": sorted(payload(request)),
                            "http_status": flow.response.status_code if flow.response else None,
                            "api_code": response.get("code"),
                            "response_fields": sorted(response)})
        if request.pretty_host != "performance-data.lbkrs.com":
            continue
        for record in payload(request).get("records", []):
            try:
                raw = record["data"]
                try:
                    event = json.loads(raw)
                except ValueError:
                    event = json.loads(base64.b64decode(raw))
                data = event["data"]
                method, _, url = data.get("object", "").partition(":")
                parts = urlsplit(url)
                if (parts.hostname not in API_HOSTS or not is_order_endpoint(parts.path) or
                        method not in {"POST", "PUT", "PATCH", "DELETE"}):
                    continue
                ext = data.get("ext", {})
                ext = json.loads(ext) if isinstance(ext, str) else ext
                query = ext.get("query", {})
                query = json.loads(query) if isinstance(query, str) else query
                records.append({"source": "app_telemetry", "time": utc(int(event["timestamp"]) / 1000),
                                "method": method, "path": parts.path,
                                "request_fields": sorted(query) if isinstance(query, dict) else [],
                                "http_status": ext.get("http_code"), "api_code": ext.get("server_code")})
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
    return sorted(records, key=lambda row: row["time"])


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_request(flow):
    request = flow.request
    if (request.method != "GET" or request.scheme != "https" or request.port != 443 or
            request.pretty_host not in API_HOSTS or request.raw_content or
            request.path.split("?", 1)[0] not in READ_PATHS.values()):
        raise ValueError("Only the four allowlisted read-only HTTPS endpoints can be probed.")
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in {"host", "content-length", "connection", "accept-encoding"}}
    # Transparent capture's request.host/url may contain the destination IP.
    # Use the HTTP authority for DNS and TLS SNI, with certificate verification.
    return Request("https://" + request.pretty_host + request.path, method="GET", headers=headers)


def probe(flow, opener=None):
    request = read_request(flow)
    opener = opener or build_opener(ProxyHandler({}), HTTPSHandler(), NoRedirects())
    started = time.perf_counter()
    try:
        response = opener.open(request, timeout=15)
    except HTTPError as exc:
        response = exc
    with response:
        body = response.read(4 * 1024 * 1024)
        try:
            data = json.loads(body)
        except ValueError:
            data = {}
        data = data if isinstance(data, dict) else {}
        return {"path": flow.request.path.split("?", 1)[0], "http_status": response.code,
                "api_code": data.get("code"),
                "ok": response.code == 200 and data.get("code") == 0,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                "template_age_seconds": round(time.time() - flow.request.timestamp_start, 1),
                "uses_captured_signature": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("action", choices=("orders", "probe"))
    parser.add_argument("--capture", type=Path, help="flows.mitm file; defaults to the active session")
    parser.add_argument("--endpoint", choices=READ_PATHS, default="orders")
    args = parser.parse_args()
    path = args.capture
    if path is None:
        state = json.loads((load_settings(args.config).proxy_dir / "capture.json").read_text())
        path = Path(state["directory"]) / "flows.mitm"
    try:
        if args.action == "orders":
            result = {"orders": order_evidence(flows(path)),
                      "note": "Telemetry reports requests and result codes; it does not contain full responses or signing headers."}
        else:
            candidate = None
            for flow in flows(path):
                if (flow.type == "http" and flow.request.method == "GET" and
                        flow.request.pretty_host in API_HOSTS and
                        flow.request.path.split("?", 1)[0] == READ_PATHS[args.endpoint] and
                        flow.response and flow.response.status_code == 200 and payload(flow.response).get("code") == 0):
                    candidate = flow
            if candidate is None:
                raise ValueError("No successful capture exists for this read-only endpoint.")
            result = probe(candidate)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok", True) else 1
    except (OSError, ValueError, RuntimeError, URLError) as exc:
        # Network/capture errors can contain URLs; emit no credentials or bodies.
        print(json.dumps({"ok": False, "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
