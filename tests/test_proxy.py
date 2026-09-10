from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.proxy_boot import merge_cpio
from scripts import proxy
from scripts import proxy_inspect
from gigamoney.config import Settings


def load_capture():
    path = Path(__file__).resolve().parent.parent / "scripts/proxy_capture.py"
    spec = importlib.util.spec_from_file_location("capture_under_test", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"mitmproxy": SimpleNamespace(ctx=None)}):
        spec.loader.exec_module(module)
    return module.Capture


def cpio_entry(name, content=b"", mode=0o100644):
    encoded = name.encode() + b"\0"
    fields = [1, mode, 0, 0, 1, 0, len(content), 0, 0, 0, 0, len(encoded), 0]
    record = b"070701" + b"".join(f"{x:08x}".encode() for x in fields) + encoded
    record += b"\0" * (-len(record) % 4)
    record += content
    return record + b"\0" * (-len(record) % 4)


class ProxyCaptureTests(unittest.TestCase):
    def setUp(self):
        settings = Settings({}, Path("unused-test-config.json"), Path(__file__).resolve().parent)
        replacement = patch.object(proxy, "settings_for_capture", return_value=settings)
        replacement.start()
        self.addCleanup(replacement.stop)

    def test_api_capture_needs_no_emulator_and_strips_local_command_header(self):
        capture = load_capture()()
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "GIGAMONEY_CAPTURE_DIR": directory, "GIGAMONEY_CAPTURE_MODE": "api"}):
            capture.running()
            self.assertIsNone(capture.watcher)
            self.assertIsNotNone(capture.target_since)
            flow = SimpleNamespace(id="api1", type="http", metadata={}, request=SimpleNamespace(
                method="POST", pretty_host="api-gl.lbkrs.com", path="/v3/orders/submit", raw_content=b"private",
                headers={"x-request-id": "request1", "x-gigamoney-command-id": "command%20%2F%20one"}))
            capture.request(flow)
            self.assertNotIn("x-gigamoney-command-id", flow.request.headers)
            self.assertEqual(flow.metadata["gigamoney_command_id"], "command / one")
            capture.done()
            rows = [json.loads(x) for x in (Path(directory) / "events.jsonl").read_text().splitlines()]
            self.assertEqual(rows[1]["command_id"], "command / one")
            self.assertEqual(rows[1]["request_id"], "request1")
            self.assertNotIn("private", json.dumps(rows))

    def test_health_requires_successful_core_response_opened_after_target_change(self):
        capture = load_capture()()
        with tempfile.TemporaryDirectory() as directory:
            capture.directory = Path(directory)
            capture.target_since = 100.0
            flow = SimpleNamespace(id="f1", type="http", request=SimpleNamespace(
                method="GET", pretty_host="api-gl.lbkrs.com", path="/v5/orders/today?secret=hidden",
                timestamp_start=99.0), response=SimpleNamespace(
                status_code=200, raw_content=b'{"code":0}', content=b'{"code":0}', timestamp_end=102.0))
            capture.response(flow)
            health = capture.directory / "api-health.json"
            self.assertFalse(health.exists())
            flow.request.timestamp_start = 101.0
            flow.request.path = "/_ping"
            capture.response(flow)
            self.assertFalse(health.exists())
            flow.request.path = "/v5/orders/today?secret=hidden"
            flow.response.content = b'{"code":402000}'
            capture.response(flow)
            self.assertFalse(health.exists())
            flow.response.content = b'{"code":0,"data":{"token":"hidden"}}'
            capture.response(flow)
            self.assertNotIn("hidden", health.read_text())
            self.assertEqual(json.loads(health.read_text())["path"], "/v5/orders/today")

    def test_status_invalidates_evidence_after_restart_reconnect_or_idle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "capture.json").write_text(json.dumps({"pid": 12, "directory": str(root)}))
            (root / "targets.json").write_text(json.dumps({"pids": [20], "app_pid": 30, "target_since": 100}))
            trust = {"phase": "ready", "app_pid": 30, "supervisor_pid": 40}
            (root / "trust.json").write_text(json.dumps(trust))
            evidence = {"target_since": 100, "request_started": 101, "response_ended": 102}
            with patch.object(proxy, "STATE", root), patch.object(proxy, "is_running", return_value=True), \
                    patch.object(proxy, "trust_running", return_value=True), patch.object(proxy.time, "time", return_value=103):
                self.assertTrue(proxy.status()["transport_ready"])
                self.assertFalse(proxy.status()["capture_ready"])
                (root / "api-health.json").write_text(json.dumps(evidence))
                self.assertTrue(proxy.status()["capture_ready"])
                (root / "trust.json").write_text(json.dumps({**trust, "app_pid": 31}))
                self.assertFalse(proxy.status()["capture_ready"])
                (root / "trust.json").write_text(json.dumps(trust))
                (root / "reconnect.json").write_text(json.dumps({"started": 103}))
                self.assertFalse(proxy.status()["capture_ready"])
                (root / "reconnect.json").unlink()
                with patch.object(proxy.time, "time", return_value=403):
                    self.assertFalse(proxy.status()["capture_ready"])

    def test_metadata_omits_credentials_and_bodies(self):
        capture = load_capture()()
        capture.events = io.StringIO()
        flow = SimpleNamespace(id="f1", type="http", request=SimpleNamespace(
            method="GET", pretty_host="api.example", path="/positions?token=secret",
            raw_content=b"private request", timestamp_start=10.0), response=SimpleNamespace(
            status_code=200, raw_content=b"private response", timestamp_end=10.25))
        capture.request(flow)
        capture.response(flow)
        output = capture.events.getvalue()
        self.assertNotIn("secret", output)
        self.assertNotIn("private", output)
        result = json.loads(output.splitlines()[1])
        self.assertEqual(result["elapsed_ms"], 250.0)
        self.assertEqual(result["bytes"], len(b"private response"))
        self.assertEqual(flow.response.raw_content, b"private response")

    def test_live_binary_messages_are_lossless_in_both_directions(self):
        capture = load_capture()()
        capture.messages = io.StringIO()
        flow = SimpleNamespace(id="ws1", type="http", websocket=True,
                               server_conn=SimpleNamespace(address=("example", 443)))
        payload = bytes(range(256))
        for direction in (True, False):
            capture.save_message(flow, SimpleNamespace(timestamp=123.0, from_client=direction, content=payload))
        rows = [json.loads(x) for x in capture.messages.getvalue().splitlines()]
        self.assertEqual([r["from_client"] for r in rows], [True, False])
        self.assertTrue(all(r["protocol"] == "websocket" for r in rows))
        self.assertTrue(all(base64.b64decode(r["content_base64"]) == payload for r in rows))

    def test_tls_failure_has_host_and_time(self):
        capture = load_capture()()
        capture.events = io.StringIO()
        capture.tls_failed_client(SimpleNamespace(context=SimpleNamespace(
            client=SimpleNamespace(sni="api.example", error="unknown ca"))))
        result = json.loads(capture.events.getvalue())
        self.assertEqual(result["host"], "api.example")
        self.assertEqual(result["error"], "unknown ca")
        self.assertIn("time", result)

    def test_report_handles_partial_last_line(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "capture.json").write_text(json.dumps({"pid": 99999999, "directory": str(root)}))
            row = {"event": "response", "method": "GET", "host": "example", "path": "/cash", "status": 200, "elapsed_ms": 10}
            (root / "events.jsonl").write_text(json.dumps(row) + '\n{"event":')
            with patch.object(proxy, "STATE", root):
                report = proxy.report()
            self.assertEqual(report["responses"], 1)
            self.assertFalse(report["running"])

    def test_unrelated_pid_is_not_treated_as_capture(self):
        with patch.object(Path, "read_bytes", return_value=b"python\0some-other-process\0"):
            self.assertFalse(proxy.is_running({"pid": 123, "directory": "/tmp/capture"}))
            self.assertFalse(proxy.trust_running(123))


class ReadOnlyProbeTests(unittest.TestCase):
    def flow(self, **overrides):
        fields = dict(method="GET", scheme="https", port=443, host="192.0.2.1",
                      pretty_host="api-gl.lbkrs.com", path="/v5/orders/today?limit=20",
                      raw_content=b"", timestamp_start=100,
                      headers={"authorization": "private-token", "x-api-signature": "private-signature",
                               "host": "192.0.2.1", "accept-encoding": "gzip"})
        return SimpleNamespace(request=SimpleNamespace(**(fields | overrides)))

    def test_preserves_signed_read_but_uses_authority_for_tls(self):
        request = proxy_inspect.read_request(self.flow())
        self.assertEqual(request.full_url, "https://api-gl.lbkrs.com/v5/orders/today?limit=20")
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Authorization"), "private-token")
        self.assertIsNone(request.get_header("Host"))
        self.assertIsNone(request.get_header("Accept-encoding"))

    def test_refuses_mutations_other_hosts_and_bodies(self):
        for change in ({"method": "POST"}, {"path": "/v3/orders/submit"},
                       {"path": "/v2/orders/withdraw"}, {"path": "/v1/tube/replace"},
                       {"pretty_host": "other.example"},
                       {"scheme": "http"}, {"port": 80}, {"raw_content": b"payload"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                proxy_inspect.read_request(self.flow(**change))

    def test_probe_output_has_no_credentials_or_account_body(self):
        response = io.BytesIO(b'{"code":0,"data":{"account":"private-account"}}')
        response.code = 200
        opener = SimpleNamespace(open=lambda request, timeout: response)
        result = proxy_inspect.probe(self.flow(), opener=opener)
        self.assertTrue(result["ok"])
        self.assertTrue(result["uses_captured_signature"])
        self.assertNotIn("private", json.dumps(result))

    def test_redirect_cannot_forward_authorization(self):
        handler = proxy_inspect.NoRedirects()
        self.assertIsNone(handler.redirect_request(proxy_inspect.read_request(self.flow()),
                                                  None, 302, "", {}, "https://other.example"))

    def test_telemetry_is_labelled_and_contains_only_field_names(self):
        record = {"timestamp": "1788826852272", "data": {
            "object": "POST:https://api-gl.lbkrs.com/v3/orders/submit?private=hidden",
            "ext": {"query": json.dumps({"client_request_id": "hidden", "quantity": "1"}),
                    "http_code": "200", "server_code": "0"}}}
        for raw in (json.dumps(record), base64.b64encode(json.dumps(record).encode()).decode()):
            flow = SimpleNamespace(type="http", request=SimpleNamespace(
                pretty_host="performance-data.lbkrs.com", path="/streams/event_tracking_log/records",
                content=json.dumps({"records": [{"data": raw}]}).encode()))
            evidence = proxy_inspect.order_evidence([flow])
            self.assertEqual(evidence[0]["source"], "app_telemetry")
            self.assertEqual(evidence[0]["request_fields"], ["client_request_id", "quantity"])
            self.assertNotIn("hidden", json.dumps(evidence))

    def test_replace_outside_orders_path_is_reported_in_http_and_telemetry(self):
        path = "/v1/tube/replace"
        request = {"order_id": "private-id", "order_id_str": "private-id", "price": "2.28", "quantity": "2"}
        http = SimpleNamespace(type="http", request=SimpleNamespace(
            method="POST", pretty_host="api-gl.lbkrs.com", path=path, timestamp_start=100,
            content=json.dumps(request).encode()), response=SimpleNamespace(
                status_code=200, content=b'{"code":0,"data":{"order_id":"private-id","approval":false}}'))
        event = {"timestamp": "100000", "data": {"object": "POST:https://api-gl.lbkrs.com" + path,
                 "ext": {"query": json.dumps(request), "http_code": "200", "server_code": "0"}}}
        telemetry = SimpleNamespace(type="http", request=SimpleNamespace(
            pretty_host="performance-data.lbkrs.com", path="/streams/event_tracking_log/records",
            content=json.dumps({"records": [{"data": json.dumps(event)}]}).encode()))
        records = proxy_inspect.order_evidence([http, telemetry])
        self.assertEqual([record["source"] for record in records], ["full_http", "app_telemetry"])
        self.assertTrue(all(record["path"] == path for record in records))
        self.assertTrue(all(record["request_fields"] == sorted(request) for record in records))
        self.assertNotIn("private-id", json.dumps(records))


class RamdiskTests(unittest.TestCase):
    def test_merges_archives_and_keeps_last_duplicate(self):
        first = cpio_entry("init", b"first") + cpio_entry("TRAILER!!!")
        second = cpio_entry("init", b"last") + cpio_entry("vendor/file", b"keep") + cpio_entry("TRAILER!!!")
        merged = merge_cpio(first + b"\0" * (-len(first) % 512) + second)
        self.assertNotIn(b"first", merged)
        self.assertIn(b"last", merged)
        self.assertIn(b"keep", merged)
        self.assertEqual(merged.count(b"TRAILER!!!"), 1)
        self.assertEqual(len(merged) % 512, 0)

    def test_rejects_unsafe_names_and_truncated_input(self):
        with self.assertRaises(ValueError):
            merge_cpio(cpio_entry("../init") + cpio_entry("TRAILER!!!"))
        with self.assertRaises(ValueError):
            merge_cpio(cpio_entry("init", b"long body")[:-8])

    def test_requires_init_and_trailer(self):
        with self.assertRaises(ValueError):
            merge_cpio(cpio_entry("other") + cpio_entry("TRAILER!!!"))
        with self.assertRaises(ValueError):
            merge_cpio(cpio_entry("init"))
