import copy
import ast
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gigamoney.config import DEFAULTS, Settings, find_adb, load_settings, validate_config
from gigamoney.errors import ConfigurationError
from gigamoney.setup import init_config, render_service, build_android
from gigamoney.monitor import in_scheduled_restart_window
from datetime import datetime, timezone


class ConfigTests(unittest.TestCase):
    def test_legacy_api_order_caps_still_load(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({"api": {"maxOrderQuantity": 200, "maxOrders": 100}}))
            settings = load_settings(path)
            self.assertEqual(settings.get("api.maxOrderQuantity"), 200)
            self.assertEqual(settings.get("api.maxOrders"), 100)

    def test_price_cache_accepts_legacy_age_and_requires_positive_poll_interval(self):
        validate_config({"kafka": {"priceBookMaxAgeSeconds": None, "priceBookPollIntervalSeconds": 30}})
        with self.assertRaises(ConfigurationError):
            validate_config({"kafka": {"priceBookPollIntervalSeconds": 0}})

    def test_all_literal_operator_settings_are_in_the_catalog(self):
        root = Path(__file__).resolve().parents[1]
        for file in [*(root / "gigamoney").glob("*.py"), *(root / "scripts").glob("*.py")]:
            for node in ast.walk(ast.parse(file.read_text())):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr not in {"get", "require"} or not node.args:
                    continue
                if ast.unparse(node.func.value) not in {"settings", "self.settings", "self.controller.settings", "settings()", "settings_for_capture()"}:
                    continue
                key = node.args[0]
                if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                    continue
                value = DEFAULTS
                for part in key.value.split("."):
                    with self.subTest(file=file.name, setting=key.value):
                        self.assertIn(part, value)
                        value = value[part]

    def test_example_lists_every_config_field_without_machine_paths(self):
        path = Path(__file__).resolve().parents[1] / "config/gigamoney.config.example.json"
        example = json.loads(path.read_text())
        self.assertEqual(example, DEFAULTS)
        self.assertNotIn("/home/ubuntu", path.read_text())
        validate_config(example)

    def test_invalid_or_misspelled_fields_fail_without_exposing_values(self):
        for value in ({"api": {"requestTimeoutSecond": "secret"}},
                      {"kafka": {"holdingsIntervalSeconds": 0}},
                      {"api": {"readAttempts": 0}},
                      {"monitor": {"restart": {"timezone": "invalid"}}},
                      {"kafka": {"clientConfig": {"enable.auto.commit": True}}}):
            with self.subTest(value=value), self.assertRaises(ConfigurationError) as error:
                validate_config(value)
            self.assertNotIn("secret", str(error.exception))

    def test_config_selection_and_paths_are_independent_of_current_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            init_config(config)
            data = json.loads(config.read_text())
            data["paths"] = {"workDir": str(root / "account-a-state"), "logDir": str(root / "account-a-logs")}
            config.write_text(json.dumps(data))
            with patch.dict(os.environ, {"GIGAMONEY_CONFIG": str(config)}):
                settings = load_settings()
            self.assertEqual(settings.session_path, root / "account-a-state/api/session.json")
            self.assertEqual(settings.proxy_dir, root / "account-a-state/proxy")
            self.assertEqual(settings.log_dir, root / "account-a-logs")
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                init_config(config)

    def test_loaded_accounts_do_not_share_mutable_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "account.json"
            path.write_text('{}')
            first, second = load_settings(path), load_settings(path)
            first.data["api"]["bootstrap"]["capturePath"] = "private-account-a"
            self.assertEqual(second.get("api.bootstrap.capturePath"), "")
            self.assertEqual(DEFAULTS["api"]["bootstrap"]["capturePath"], "")

    def test_service_uses_selected_interpreter_config_and_paths_with_spaces(self):
        with tempfile.TemporaryDirectory(prefix="account setup ") as directory:
            root = Path(directory)
            settings = Settings({"paths": {"workDir": str(root / "state"), "logDir": str(root / "logs")},
                                 "service": {"name": "account-a", "restartSeconds": 17}}, root / "account a.json", root)
            with patch("gigamoney.setup.sys.executable", str(root / ".venv/bin/python")):
                result = render_service(settings)
            unit = Path(result["unit"]).read_text()
            self.assertIn('--config "' + str(settings.path) + '" runner', unit)
            self.assertIn("RestartSec=17", unit)
            self.assertFalse(result["started"])
            self.assertNotIn("/home/ubuntu", unit)

    def test_restart_timezone_disabled_and_midnight_window(self):
        settings = Settings({"monitor": {"restart": {"enabled": True, "timezone": "UTC", "start": "23:30", "end": "00:30"}}}, Path("x"), Path("."))
        self.assertTrue(in_scheduled_restart_window(datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc), settings))
        settings.data["monitor"]["restart"]["enabled"] = False
        self.assertFalse(in_scheduled_restart_window(datetime(2026, 9, 10, 0, 10, tzinfo=timezone.utc), settings))

    def test_android_build_uses_configured_sdk_and_jdk(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ["sdk/platforms/android-36/android.jar", "sdk/platforms/android-36/uiautomator.jar",
                         "sdk/platforms/android-36/optional/android.test.runner.jar", "sdk/build-tools/36.0.0/d8",
                         "jdk/bin/javac", "jdk/bin/jar", "junit.jar"]:
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.touch()
            settings = Settings({"androidBuild": {"sdkRoot": "sdk", "javaHome": "jdk",
                                  "junitJar": "junit.jar", "outputJar": "helper.jar"}}, root / "config.json", root)
            calls = []
            result = build_android(settings, run=lambda args, **kwargs: calls.append((args, kwargs)))
            self.assertEqual(len(calls), 4)
            self.assertEqual(calls[0][0][0], str(root / "jdk/bin/javac"))
            self.assertEqual(calls[2][0][0], str(root / "sdk/build-tools/36.0.0/d8"))
            self.assertEqual(result["jar"], str(root / "helper.jar"))

    def test_relative_adb_path_resolves_from_project_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "tools/adb"
            executable.parent.mkdir()
            executable.write_text('#!/bin/sh\nexit 0\n')
            executable.chmod(0o700)
            self.assertEqual(find_adb("tools/adb", root=root), str(executable))


if __name__ == "__main__":
    unittest.main()
