from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gigamoney.api import load_session, save_session
from gigamoney.config import Settings
from scripts import api_proxy
from tests.test_api import state as unbound_state


def state():
    return {**unbound_state(), "account_id": "TEST"}


class ApiProxyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.session = self.root / "session.json"
        self.route = {"url": "http://127.0.0.1:18080", "upstream": "api-gl.lbkrs.com"}
        self.original = {**state(), "capture_proxy": self.route}
        save_session(self.session, self.original)
        self.capture = {"pid": 123, "port": 18080, "directory": str(self.root), "route": self.route, "started_epoch": 100}
        (self.root / "api-capture.json").write_text(json.dumps(self.capture))
        self.settings = Settings({"gigamoney": {"accountId": "TEST"}}, self.root / "config.json", self.root)
        for target, value in (("STATE", self.root), ("session_path", lambda: self.session), ("settings", lambda: self.settings)):
            p = patch.object(api_proxy, target, value)
            p.start(); self.addCleanup(p.stop)

    def test_ready_requires_matching_route_process_and_recent_account_evidence(self):
        with patch.object(api_proxy, "is_running", return_value=True), patch.object(api_proxy.time, "time", return_value=103):
            self.assertFalse(api_proxy.status()["capture_ready"])
            (self.root / "api-health.json").write_text(json.dumps({"request_started": 101, "response_ended": 102}))
            self.assertTrue(api_proxy.status()["capture_ready"])
            with patch.object(api_proxy.time, "time", return_value=403):
                self.assertFalse(api_proxy.status()["capture_ready"])
            save_session(self.session, state())
            self.assertFalse(api_proxy.status()["capture_ready"])

    def test_stop_removes_only_capture_route_and_preserves_latest_credentials(self):
        with patch.object(api_proxy, "is_running", return_value=False):
            result = api_proxy.stop()
        self.assertFalse(result["route_configured"])
        self.assertEqual(load_session(self.session), state())

    def test_existing_start_reattaches_without_spawning_and_rejects_port_change(self):
        save_session(self.session, state())
        with patch.object(api_proxy, "is_running", return_value=True), patch.object(api_proxy.subprocess, "Popen") as popen:
            self.assertTrue(api_proxy.start()["route_configured"])
            with self.assertRaisesRegex(RuntimeError, "changing"):
                api_proxy.start(18081)
            popen.assert_not_called()

    def test_report_counts_api_failures_and_timing_without_reading_bodies(self):
        rows = [{"event": "response", "method": "POST", "path": "/v3/orders/submit", "status": 200,
                 "api_code": 403001, "elapsed_ms": 100, "request_id": "test1", "command_id": "command1"},
                {"event": "error", "error": "private diagnostics are not included in report"}]
        (self.root / "events.jsonl").write_text("\n".join(json.dumps(x) for x in rows) + '\n{"partial":')
        with patch.object(api_proxy, "is_running", return_value=False):
            report = api_proxy.report()
        self.assertEqual(report["responses"], 1)
        self.assertEqual(report["endpoints"][0]["api_code"], 403001)
        self.assertEqual(report["errors"], {"error": 1})
        self.assertNotIn("private diagnostics", json.dumps(report))

    def test_proxy_cannot_modify_another_accounts_session(self):
        self.settings.data["gigamoney"]["accountId"] = "ANOTHER"
        with self.assertRaisesRegex(RuntimeError, "bound"):
            api_proxy.stop()
        self.assertEqual(load_session(self.session), self.original)


if __name__ == "__main__":
    unittest.main()
