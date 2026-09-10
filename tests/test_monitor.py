import http.client
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from gigamoney.config import Settings
from gigamoney.monitor import (
    ActionResult,
    GigaMoneyController,
    GigaMoneyServer,
    LoginLimiter,
    RedirectServer,
    StatusMonitor,
    in_scheduled_restart_window,
    parse_android_app_meminfo,
    parse_android_memory_summary,
    verify_admin_credentials,
)


class ScheduledRestartWindowTests(unittest.TestCase):
    def test_window_uses_daylight_aware_eastern_time(self):
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 1, 15, 1, 1, tzinfo=timezone.utc)))

    def test_window_includes_only_the_900_pm_minute(self):
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 8, 15, 0, 0, 59, tzinfo=timezone.utc)))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 8, 15, 1, 0, 59, tzinfo=timezone.utc)))
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 8, 15, 1, 1, tzinfo=timezone.utc)))


class FakeRunner:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def __call__(self, arguments, *, cwd, timeout):
        self.calls.append(list(arguments))
        if self.responses:
            return self.responses.pop(0)
        return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()


class FakeProcess:
    def __init__(self, pid=2468, returncode=None):
        self.pid = pid
        self.returncode = returncode

    def poll(self):
        return self.returncode


class MonitorTests(unittest.TestCase):
    def test_api_health_and_status_do_not_probe_android(self):
        self.settings.data["gigamoney"]["backend"] = "api"
        controller = GigaMoneyController(self.settings, machine_sampler=lambda: {
            "cpu_percent": 20, "memory_percent": 30, "memory_total_bytes": 16 * 1024**3,
            "memory_available_bytes": 11 * 1024**3})
        with patch.object(controller, "emulator_status", side_effect=AssertionError("must not probe Android")), \
             patch.object(controller, "_android_memory", side_effect=AssertionError("must not use ADB")):
            status = controller.collect_status(60)
        self.assertEqual(status["account"]["backend"], "api")
        self.assertEqual(status["health"]["overall_level"], "healthy")
        self.assertEqual([row["id"] for row in status["health"]["metrics"]], ["machine_cpu", "machine_memory"])

    def test_api_scheduled_restart_restarts_only_runner(self):
        self.settings.data["gigamoney"]["backend"] = "api"
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "stopped"))
        controller._wait_for_script_stopped = Mock(return_value=ActionResult(True, "stopped"))
        controller.start_script = Mock(return_value=ActionResult(True, "started"))
        with patch.object(controller, "stop_emulator", side_effect=AssertionError("must not stop Android")), \
             patch.object(controller, "_start_emulator", side_effect=AssertionError("must not start Android")):
            result = controller.scheduled_restart_if_due(datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc), waiter=lambda _: False)
        self.assertTrue(result.ok)
        controller.start_script.assert_called_once_with()

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "logs").mkdir()
        (self.root / "work").mkdir()
        self.settings = Settings({
            "gigamoney": {"accountId": "GIGA-A", "package": "lb.whale.hkwinner.android"},
            "adb": {"serial": "emulator-5554", "path": ""},
            "monitor": {"avd": "Pixel_10", "runnerLog": "logs/runner.log"},
        }, self.root / "config.json", self.root)

    def test_holdings_stats_are_current_session_latest_ten(self):
        started = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)
        lines = ["2026-08-10 07:59:59,000 INFO Holdings query completed in 999.000 seconds."]
        lines += [
            f"2026-08-10 08:{number:02d}:00,000 INFO Holdings query completed in {number:.3f} seconds."
            for number in range(1, 13)
        ]
        (self.root / "logs" / "runner.log").write_text("\n".join(lines), encoding="utf-8")
        controller = GigaMoneyController(self.settings)

        result = controller.holdings_query_performance({
            "running": True, "pid": 123, "started_at": started.isoformat(),
        })

        self.assertTrue(result["available"])
        self.assertEqual(result["attempts_in_session"], 12)
        self.assertEqual(result["sample_count"], 10)
        self.assertEqual(result["average_seconds"], 7.5)
        self.assertEqual(result["fastest_seconds"], 3.0)
        self.assertEqual(result["slowest_seconds"], 12.0)

    def test_holdings_stats_do_not_reuse_stopped_session(self):
        result = GigaMoneyController(self.settings).holdings_query_performance({
            "running": False, "started_at": None,
        })
        self.assertFalse(result["available"])
        self.assertEqual(result["sample_count"], 0)

    def test_holdings_stats_fall_back_to_publication_schedule_for_old_runner(self):
        (self.root / "logs" / "runner.log").write_text(
            "2026-08-10 08:00:05,000 INFO Published holdings with 2 positions to partition=0 offset=1.\n"
            "2026-08-10 08:02:07,000 INFO Published holdings with 2 positions to partition=0 offset=2.\n"
            "2026-08-10 08:04:09,000 INFO Published holdings with 2 positions to partition=0 offset=3.\n",
            encoding="utf-8",
        )
        self.settings.data["kafka"] = {"holdingsIntervalSeconds": 120}
        result = GigaMoneyController(self.settings).holdings_query_performance({
            "running": True, "pid": 123, "started_at": "2026-08-10T08:00:00Z",
        })
        self.assertTrue(result["available"])
        self.assertTrue(result["estimated"])
        self.assertEqual(result["average_seconds"], 7.0)
        self.assertEqual(result["fastest_seconds"], 5.0)
        self.assertEqual(result["slowest_seconds"], 9.0)

    def test_emulator_status_reports_booted_adb_device(self):
        adb = self.root / "sdk" / "platform-tools" / "adb"
        adb.parent.mkdir(parents=True)
        adb.write_bytes(b"")
        adb.chmod(0o755)
        self.settings.data["adb"]["path"] = str(adb)
        runner = FakeRunner([
            type("Result", (), {"returncode": 0, "stdout": "List of devices attached\nemulator-5554\tdevice\n", "stderr": ""})(),
            type("Result", (), {"returncode": 0, "stdout": "1\n", "stderr": ""})(),
        ])
        controller = GigaMoneyController(self.settings, runner=runner)

        status = controller.emulator_status()

        self.assertTrue(status["running"])
        self.assertEqual(status["state"], "running")
        self.assertEqual(status["adb_state"], "device")

    def test_health_reports_same_five_signals(self):
        controller = GigaMoneyController(
            self.settings,
            machine_sampler=lambda: {
                "cpu_percent": 24.0, "memory_percent": 50.0,
                "memory_total_bytes": 16 * 1024**3,
                "memory_available_bytes": 8 * 1024**3,
            },
        )
        controller._android_memory = lambda emulator: {
            "total_bytes": 4 * 1024**3, "used_bytes": 2 * 1024**3,
            "free_reclaimable_bytes": 2 * 1024**3, "status": "normal",
            "swap_used_bytes": 100 * 1024**2, "swap_total_bytes": 2 * 1024**3,
            "app_pss_bytes": 300 * 1024**2,
        }

        health = controller.health_status({
            "running": True, "state": "running", "adb_state": "device",
            "adb_latency_ms": 80, "shell_latency_ms": 120,
        }, datetime.now(timezone.utc))

        self.assertEqual(health["overall_level"], "healthy")
        self.assertEqual([metric["id"] for metric in health["metrics"]], [
            "machine_cpu", "machine_memory", "emulator_memory", "adb_health", "android_response",
        ])

    def test_android_memory_parsers(self):
        system = parse_android_memory_summary("""
          Total RAM: 4,194,304K (status normal)
           Free RAM: 2,097,152K (cached and free)
           Used RAM: 2,097,152K (pss and kernel)
               ZRAM: 10,000K physical used for 20,000K in swap (1,000,000K total swap)
        """)
        app = parse_android_app_meminfo("TOTAL PSS: 300000 TOTAL RSS: 500000 TOTAL SWAP (KB): 10")
        self.assertEqual(system["used_bytes"], 2_097_152 * 1024)
        self.assertEqual(system["status"], "normal")
        self.assertEqual(app["app_pss_bytes"], 300_000 * 1024)

    def test_perform_exposes_only_four_controls(self):
        controller = GigaMoneyController(self.settings)
        controller.start_script = lambda: ActionResult(True, "started")
        self.assertTrue(controller.perform("script/start").ok)
        self.assertFalse(controller.perform("orders/place").ok)

    def test_control_logs_one_outcome_and_elapsed_time(self):
        for response in (ActionResult(True, "started"), ActionResult(False, "could not start"), RuntimeError("failed")):
            with self.subTest(response=response):
                controller = GigaMoneyController(self.settings)
                controller.start_script = Mock()
                if isinstance(response, Exception):
                    controller.start_script.side_effect = response
                else:
                    controller.start_script.return_value = response
                with self.assertLogs("gigamoney.monitor", level="INFO") as logs:
                    result = controller.perform("script/start")
                outcomes = [record.getMessage() for record in logs.records
                            if record.getMessage().startswith("Control action finished")]
                self.assertEqual(len(outcomes), 1)
                self.assertIn("outcome=success" if result.ok else "outcome=failure", outcomes[0])
                self.assertIn("elapsed_s=", outcomes[0])
                self.assertFalse(controller._action_lock.locked())

    def test_start_script_uses_venv_python_config_and_detached_process(self):
        calls = []

        def popen(arguments, **kwargs):
            calls.append((arguments, kwargs))
            return FakeProcess()

        controller = GigaMoneyController(self.settings, popen=popen)
        with patch.object(controller, "script_status", side_effect=[{"running": False}, {"running": True}]):
            result = controller.start_script()

        self.assertTrue(result.ok)
        self.assertEqual(calls[0][0][1:3], ["-m", "gigamoney.cli"])
        self.assertIn(str(self.settings.path), calls[0][0])
        self.assertTrue(calls[0][1]["start_new_session"])
        self.assertEqual(calls[0][1]["env"]["GIGAMONEY_RUN_LOG"], str(self.root / "logs" / "runner.log"))

    def test_stop_script_requests_validated_signal(self):
        controller = GigaMoneyController(self.settings)
        with patch.object(controller, "script_status", return_value={"running": True, "pid": 4321}), patch(
            "gigamoney.monitor.request_runner_stop", return_value={"requested": True, "pid": 4321}
        ) as request:
            result = controller.stop_script()

        self.assertTrue(result.ok)
        request.assert_called_once_with(self.settings, force=True)

    def test_start_emulator_tracks_only_process_it_created(self):
        emulator = self.root / "sdk" / "emulator" / "emulator"
        emulator.parent.mkdir(parents=True)
        emulator.write_bytes(b"")
        emulator.chmod(0o755)
        self.settings.data["monitor"]["emulatorPath"] = str(emulator)
        calls = []

        def popen(arguments, **kwargs):
            calls.append((arguments, kwargs))
            return FakeProcess(pid=9876)

        controller = GigaMoneyController(self.settings, popen=popen)
        with patch.object(controller, "emulator_status", return_value={"running": False}):
            result = controller.start_emulator()

        self.assertTrue(result.ok)
        self.assertEqual(calls[0][0][:3], [str(emulator), "-avd", "Pixel_10"])
        self.assertIn("-no-snapshot-load", calls[0][0])
        self.assertTrue(calls[0][1]["start_new_session"])
        self.assertEqual(json.loads((self.root / "work" / "emulator.pid.json").read_text())["pid"], 9876)

    def test_web_ui_emulator_start_records_failed_attempt(self):
        controller = GigaMoneyController(self.settings)
        with patch.object(controller, "emulator_status", return_value={"running": False}), patch.object(
            controller, "_start_emulator", return_value=ActionResult(False, "start failed")
        ):
            result = controller.perform("emulator/start")

        self.assertFalse(result.ok)
        metadata = json.loads((self.root / "work" / "emulator.pid.json").read_text())
        self.assertEqual(metadata["lastRestartSource"], "web_ui")
        self.assertEqual(metadata["lastRestartState"], "failed")
        self.assertIsNotNone(metadata["lastRestartAt"])
        self.assertIsNotNone(metadata["lastRestartCompletedAt"])

    def test_web_ui_emulator_start_preserves_attempt_metadata_on_success(self):
        emulator = self.root / "sdk" / "emulator" / "emulator"
        emulator.parent.mkdir(parents=True)
        emulator.write_bytes(b"")
        emulator.chmod(0o755)
        self.settings.data["monitor"]["emulatorPath"] = str(emulator)
        controller = GigaMoneyController(
            self.settings, popen=lambda _arguments, **_kwargs: FakeProcess(pid=9876),
        )
        with patch.object(controller, "emulator_status", return_value={"running": False}):
            result = controller.perform("emulator/start")

        self.assertTrue(result.ok)
        metadata = json.loads((self.root / "work" / "emulator.pid.json").read_text())
        self.assertEqual(metadata["pid"], 9876)
        self.assertEqual(metadata["lastRestartSource"], "web_ui")
        self.assertEqual(metadata["lastRestartState"], "succeeded")

    def test_scheduled_restart_observes_order_delays_and_clean_runner_stop(self):
        now = datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)
        controller = GigaMoneyController(self.settings)
        events = []
        controller.stop_script = Mock(
            side_effect=lambda: events.append("stop runner") or ActionResult(True, "done")
        )
        controller.script_status = Mock(side_effect=[{"running": True}, {"running": False}])
        controller.stop_emulator = Mock(
            side_effect=lambda: events.append("stop emulator") or ActionResult(True, "done")
        )
        controller.emulator_status = Mock(side_effect=[
            {"running": False},
            {
                "running": True, "state": "booting", "adb_state": "device",
                "message": "Android is finishing its boot sequence.",
            },
            {
                "running": True, "state": "running", "adb_state": "device",
                "message": "The Android emulator is ready.",
            },
        ])
        controller._start_emulator = Mock(
            side_effect=lambda: events.append("start emulator") or ActionResult(True, "done")
        )
        controller._foreground_gigamoney = Mock(
            side_effect=lambda _waiter: events.append("foreground GigaMoney") or ActionResult(True, "done")
        )
        controller.start_script = Mock(
            side_effect=lambda: events.append("start runner") or ActionResult(True, "done")
        )
        waits = []

        result = controller.scheduled_restart_if_due(
            now, waiter=lambda seconds: waits.append(seconds) or False,
        )

        self.assertTrue(result.ok)
        self.assertEqual(events, [
            "stop runner", "stop emulator", "start emulator", "foreground GigaMoney", "start runner",
        ])
        self.assertEqual(waits, [0.5, 60, 5])
        metadata = json.loads((self.root / "work" / "emulator.pid.json").read_text())
        self.assertEqual(metadata["lastRestartAt"], "2026-08-15T00:01:00Z")
        self.assertEqual(metadata["lastRestartSource"], "scheduled")
        self.assertEqual(metadata["lastRestartState"], "succeeded")

    def test_scheduled_restart_guard_uses_web_ui_attempt_time(self):
        now = datetime(2026, 8, 15, 0, 30, tzinfo=timezone.utc)
        (self.root / "work" / "emulator.pid.json").write_text(json.dumps({
            "lastRestartAt": (now - timedelta(minutes=30)).isoformat(),
            "lastRestartSource": "web_ui",
        }))
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "done"))

        result = controller.scheduled_restart_if_due(now, waiter=lambda _seconds: False)

        self.assertIsNone(result)
        controller.stop_script.assert_not_called()

    def test_runner_stop_timeout_leaves_emulator_running(self):
        now = datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "requested"))
        controller.script_status = Mock(return_value={"running": True})
        controller.stop_emulator = Mock(return_value=ActionResult(True, "done"))

        with patch("gigamoney.monitor.RUNNER_STOP_TIMEOUT_SECONDS", 0):
            result = controller.scheduled_restart_if_due(now, waiter=lambda _seconds: False)

        self.assertFalse(result.ok)
        self.assertIn("emulator was left running", result.message)
        controller.stop_emulator.assert_not_called()
        metadata = json.loads((self.root / "work" / "emulator.pid.json").read_text())
        self.assertEqual(metadata["lastRestartState"], "failed")

    def test_emulator_readiness_has_a_hard_timeout(self):
        self.settings.data["monitor"]["emulatorBootTimeoutSeconds"] = 10
        controller = GigaMoneyController(self.settings)
        controller.emulator_status = Mock(return_value={
            "running": True, "state": "booting", "adb_state": "device",
            "message": "Android is finishing its boot sequence.",
        })
        waits = []

        result = controller._wait_for_emulator_ready(
            lambda seconds: waits.append(seconds) or False,
        )

        self.assertFalse(result.ok)
        self.assertIn("within 10 seconds", result.message)
        self.assertEqual(waits, [5, 5])

    def test_emulator_readiness_detects_an_exited_process(self):
        controller = GigaMoneyController(self.settings)
        controller.emulator_status = Mock(return_value={
            "running": False, "state": "stopped", "adb_state": "disconnected",
            "message": "The Android emulator is not running.",
        })
        waiter = Mock(return_value=False)

        result = controller._wait_for_emulator_ready(waiter)

        self.assertFalse(result.ok)
        self.assertIn("process exited", result.message)
        waiter.assert_not_called()

    def test_boot_timeout_stops_partial_emulator_and_leaves_runner_stopped(self):
        now = datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)
        self.settings.data["monitor"]["emulatorBootTimeoutSeconds"] = 0
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "done"))
        controller.script_status = Mock(return_value={"running": False})
        controller.stop_emulator = Mock(return_value=ActionResult(True, "done"))
        controller.emulator_status = Mock(side_effect=[
            {"running": False},
            {
                "running": True, "state": "booting", "adb_state": "device",
                "message": "Android is finishing its boot sequence.",
            },
        ])
        controller._start_emulator = Mock(return_value=ActionResult(True, "done"))
        controller._foreground_gigamoney = Mock(return_value=ActionResult(True, "done"))
        controller.start_script = Mock(return_value=ActionResult(True, "done"))

        result = controller.scheduled_restart_if_due(now, waiter=lambda _seconds: False)

        self.assertFalse(result.ok)
        self.assertIn("within 0 seconds", result.message)
        self.assertEqual(controller.stop_emulator.call_count, 2)
        controller._foreground_gigamoney.assert_not_called()
        controller.start_script.assert_not_called()

    def test_foreground_failure_stops_emulator_again_and_leaves_runner_stopped(self):
        now = datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "done"))
        controller.script_status = Mock(return_value={"running": False})
        controller.stop_emulator = Mock(return_value=ActionResult(True, "done"))
        controller.emulator_status = Mock(side_effect=[
            {"running": False},
            {
                "running": True, "state": "running", "adb_state": "device",
                "message": "The Android emulator is ready.",
            },
        ])
        controller._start_emulator = Mock(return_value=ActionResult(True, "done"))
        controller._foreground_gigamoney = Mock(
            return_value=ActionResult(False, "GigaMoney did not reach the foreground after 5 tries.")
        )
        controller.start_script = Mock(return_value=ActionResult(True, "done"))

        result = controller.scheduled_restart_if_due(now, waiter=lambda _seconds: False)

        self.assertFalse(result.ok)
        self.assertEqual(controller.stop_emulator.call_count, 2)
        controller.start_script.assert_not_called()
        metadata = json.loads((self.root / "work" / "emulator.pid.json").read_text())
        self.assertEqual(metadata["lastRestartState"], "failed")

    def test_restart_does_not_launch_a_second_emulator_when_shutdown_is_unconfirmed(self):
        now = datetime(2026, 8, 15, 0, 1, tzinfo=timezone.utc)
        controller = GigaMoneyController(self.settings)
        controller.stop_script = Mock(return_value=ActionResult(True, "done"))
        controller.script_status = Mock(return_value={"running": False})
        controller.stop_emulator = Mock(return_value=ActionResult(True, "signal sent"))
        controller.emulator_status = Mock(return_value={"running": True})
        controller._start_emulator = Mock(return_value=ActionResult(True, "done"))

        result = controller.scheduled_restart_if_due(now, waiter=lambda _seconds: False)

        self.assertFalse(result.ok)
        self.assertIn("still running", result.message)
        controller._start_emulator.assert_not_called()
        self.assertEqual(controller.stop_emulator.call_count, 2)

    def test_foreground_check_launches_and_verifies_configured_package(self):
        adb = self.root / "sdk" / "platform-tools" / "adb"
        adb.parent.mkdir(parents=True)
        adb.write_bytes(b"")
        adb.chmod(0o755)
        self.settings.data["adb"]["path"] = str(adb)
        success = type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})
        focused = type("Result", (), {
            "returncode": 0,
            "stdout": "mCurrentFocus=Window{123 u0 lb.whale.hkwinner.android/.ui.MainActivity}",
            "stderr": "",
        })
        runner = FakeRunner([success(), focused(), success()])
        controller = GigaMoneyController(self.settings, runner=runner)

        result = controller._foreground_gigamoney(lambda _seconds: False)

        self.assertTrue(result.ok)
        self.assertIn("monkey", runner.calls[0])
        self.assertIn("lb.whale.hkwinner.android", runner.calls[0])


class DummyController:
    def __init__(self):
        self.actions = []

    def collect_status(self, interval):
        return self.status()

    def public_config(self):
        return {"account_id": "GIGA-A", "server_id": "test", "device": "emulator-5554", "avd": "Pixel_10"}

    def status(self):
        return {
            "account": self.public_config(),
            "script": {"running": True, "state": "running", "pid": 123},
            "emulator": {"running": True, "state": "running"},
            "holdings_queries": {"sample_count": 1, "average_seconds": 4.0},
            "health": {"overall_level": "healthy", "metrics": []},
            "checked_at": "2026-08-10T08:00:00Z", "next_check_at": "2026-08-10T08:01:00Z",
        }

    def perform(self, action):
        self.actions.append(action)
        return ActionResult(True, "done")


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.controller = DummyController()
        self.monitor = StatusMonitor(self.controller, 60)
        self.monitor.refresh()
        self.server = GigaMoneyServer(
            ("127.0.0.1", 0), self.monitor, admin_password="test-dashboard-password",
        )
        self.server.is_tls = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def connection(self):
        return http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)

    def login(self, connection):
        body = json.dumps({"username": "admin", "password": "test-dashboard-password"})
        connection.request("POST", "/api/login", body=body, headers={
            "Content-Type": "application/json", "Content-Length": str(len(body)),
        })
        response = connection.getresponse()
        payload = json.loads(response.read())
        self.assertEqual(response.status, 200)
        cookie = response.getheader("Set-Cookie")
        self.assertIn("Secure", cookie)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        return cookie.split(";", 1)[0], payload["csrf_token"]

    def test_health_and_status_routes(self):
        connection = self.connection()
        try:
            connection.request("GET", "/healthz")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(json.loads(response.read())["ok"])
            connection.request("GET", "/api/status")
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 401)
            cookie, _ = self.login(connection)
            connection.request("GET", "/api/status", headers={"Cookie": cookie})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())["account"]["account_id"], "GIGA-A")
        finally:
            connection.close()

    def test_control_requires_custom_header(self):
        connection = self.connection()
        try:
            cookie, csrf = self.login(connection)
            connection.request("POST", "/api/script/start", body="{}", headers={
                "Cookie": cookie, "Content-Type": "application/json", "Content-Length": "2",
            })
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 403)

            connection.request("POST", "/api/script/start", body="{}", headers={
                "Cookie": cookie, "Content-Type": "application/json", "Content-Length": "2",
                "X-GigaMoney-Action": "1", "X-CSRF-Token": csrf,
            })
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(json.loads(response.read())["ok"])
            self.assertEqual(self.controller.actions, ["script/start"])
        finally:
            connection.close()

    def test_credentials_and_login_lockout(self):
        self.assertTrue(verify_admin_credentials("admin", "test-password", "test-password"))
        self.assertFalse(verify_admin_credentials("admin", "wrong", "test-password"))
        limiter = LoginLimiter()
        for _ in range(4):
            self.assertEqual(limiter.record_failure("192.0.2.1"), 0)
        self.assertGreater(limiter.record_failure("192.0.2.1"), 0)
        self.assertGreater(limiter.retry_after("192.0.2.1"), 0)

    def test_http_redirect_uses_standard_https_without_explicit_port(self):
        redirect = RedirectServer(("127.0.0.1", 0), "monitor.example.test", 443)
        thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection("127.0.0.1", redirect.server_port, timeout=5)
        try:
            connection.request("GET", "/dashboard?view=live")
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 308)
            self.assertEqual(
                response.getheader("Location"),
                "https://monitor.example.test/dashboard?view=live",
            )
        finally:
            connection.close()
            redirect.shutdown()
            redirect.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
