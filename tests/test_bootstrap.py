import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

from gigamoney.api import ApiClient, AuthenticationRequired, SessionError, save_session
from gigamoney.bootstrap import build_session, inspect_records
from gigamoney.config import Settings
from gigamoney.signing import signature
from tests.test_api import jwt, Opener


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = {"api_secret": "synthetic-signing-secret"}

    def account(self, name):
        settings = Settings({"paths": {"workDir": name},
            "gigamoney": {"accountId": name, "tradePassword": "synthetic-password"},
            "api": {"accountAaid": name + "-broker", "orgId": "1", "accountChannel": "9"}},
            self.root / (name + ".json"), self.root)
        credentials = {"authorization": jwt(1600, name), "refresh_token": jwt(4000, name + "-refresh"),
                       "trade_token": jwt(1300, name + "-trade"), "trade_refresh_token": jwt(2800, name + "-trade-refresh")}
        headers = {"authorization": credentials["authorization"], "x-authorization": credentials["trade_token"],
                   "x-api-key": "synthetic-key", "x-target-aaid": name + "-broker", "x-target-org-id": "1",
                   "account-channel": "9", "x-timestamp": "999000.123", "x-request-id": "synthetic-request"}
        rows = []
        for path, method, body, response in (
            ("/auth/refresh_token", "POST", {}, {"credentials": {"token": credentials["authorization"], "refresh_token": credentials["refresh_token"]}}),
            ("/v3/member/trade/auth", "POST", {"android_device_tag": name + "-device"},
             {"aaid": name + "-broker", "token": {"token": credentials["trade_token"], "refresh_token": credentials["trade_refresh_token"]}}),
            ("/v6/portfolio/detail", "GET", {}, {}),
        ):
            signed = {**headers, "x-api-signature": signature(self.profile["api_secret"], method, path,
                      headers["x-timestamp"], headers["authorization"], body if method == "POST" else None)}
            rows.append({"time": 999, "host": "api-gl.lbkrs.com", "method": method, "path": path,
                         "headers": signed, "body": body, "status": 200, "response": {"code": 0, "data": response}})
        return settings, rows, credentials

    def test_two_accounts_import_separate_bound_sessions_and_keep_device_identity(self):
        a, rows_a, _ = self.account("account-a")
        b, rows_b, _ = self.account("account-b")
        records = rows_a + rows_b
        self.assertEqual(len(inspect_records(records)), 2)
        for settings in (a, b):
            result = build_session(records, self.profile, settings, now=1000)
            name = settings.get("gigamoney.accountId")
            self.assertEqual(result["account_id"], name)
            self.assertEqual(result["trade_auth"]["android_device_tag"], name + "-device")
            self.assertEqual(result["trade_auth"]["password_md5"], hashlib.md5(b"synthetic-password").hexdigest())
            self.assertNotIn("capture_proxy", result)
            self.assertNotIn("x-api-signature", result["headers"])
            save_session(settings.session_path, result)
            with ApiClient.from_settings(settings)._session() as saved:
                self.assertEqual(saved["account_id"], name)
        a.data["api"]["sessionPath"] = str(b.session_path)
        opener = Opener()
        with self.assertRaises(AuthenticationRequired):
            ApiClient.from_settings(a, opener=opener).read("positions")
        self.assertEqual(opener.requests, [])

    def test_emulator_credentials_can_supply_refresh_pairs_missing_from_capture(self):
        settings, records, credentials = self.account("a")
        records = records[1:]
        result = build_session(records, self.profile, settings, credentials=credentials, now=1000)
        self.assertEqual(result["refresh_token"], credentials["refresh_token"])
        wrong = {**credentials, "authorization": jwt(1600, "another-device")}
        with self.assertRaisesRegex(SessionError, "changed"):
            build_session(records, self.profile, settings, credentials=wrong, now=1000)

    def test_wrong_profile_stale_or_unverified_capture_cannot_replace_session(self):
        settings, records, _ = self.account("a")
        with self.assertRaisesRegex(SessionError, "signed account read"):
            build_session(records, {"api_secret": "wrong"}, settings, now=1000)
        with self.assertRaises(SessionError):
            build_session(records, self.profile, settings, now=5000)
        unverified = copy.deepcopy(records)
        unverified[-1]["response"] = {"code": 402001}
        with self.assertRaises(SessionError):
            build_session(unverified, self.profile, settings, now=1000)
        with self.assertRaisesRegex(SessionError, "device"):
            build_session(records[::2], self.profile, settings, now=1000)


if __name__ == "__main__":
    unittest.main()
