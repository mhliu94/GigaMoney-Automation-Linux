from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError, URLError
from unittest.mock import patch

from gigamoney.api import AuthenticationRequired, ApiClient, ApiError, NoRedirects, SessionError, load_session, save_session
from gigamoney.signing import canonical_body, request_timestamp, signature, token_expiry
from scripts.api import main, verify_capture


def jwt(expiry, tag="access"):
    claims = base64.urlsafe_b64encode(json.dumps({"exp": expiry, "tag": tag}).encode()).decode().rstrip("=")
    return "eyJhbGciOiJub25lIn0." + claims + ".dummy"


def state(now=1000):
    return {"version": 1, "host": "api-gl.lbkrs.com", "api_secret": "synthetic-secret",
            "headers": {"x-api-key": "synthetic-key", "x-target-aaid": "fake-account"},
            "authorization": jwt(now + 600), "refresh_token": jwt(now + 3000, "main-refresh"),
            "trade_token": jwt(now + 100, "trade"), "trade_refresh_token": jwt(now + 1800, "trade-refresh"),
            "templates": {"orders": {"path": "/v5/orders/today?limit=20"}}}


class Response(io.BytesIO):
    def __init__(self, payload=None, status=200):
        super().__init__(json.dumps(payload if payload is not None else {"code": 0, "data": {}}).encode())
        self.code = status


class Opener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


def refreshed(kind, now=1000):
    pair = {"token": jwt(now + 600, kind + "-rotated"),
            "refresh_token": jwt(now + 3000, kind + "-refresh-rotated")}
    return {"code": 0, "data": {"credentials" if kind == "main" else "token": pair}}


class SigningTests(unittest.TestCase):
    def test_signing_uses_exact_query_and_token_case_and_text_key(self):
        message = b"get|/v5/orders/today?q=A%2FB&n=1|1000000.123|token=Token.AbC"
        expected = base64.b64encode(hmac.new(b"abc123", message, hashlib.sha256).digest()).decode()
        self.assertEqual(signature("abc123", "GET", "/v5/orders/today?q=A%2FB&n=1", "1000000.123", "Token.AbC"), expected)
        self.assertNotEqual(signature("abc123", "GET", "/v5/orders/today?n=1&q=A%2FB", "1000000.123", "Token.AbC"), expected)

    def test_post_nested_arrays_sort_by_complete_key_and_preserve_raw_values(self):
        body = {"z": [], "items": list(range(12)), "a": {"text": "A&B=中", "flag": False}, "n": None}
        expected = ("a[flag]=false&a[text]=A&B=中&items[0]=0&items[10]=10&items[11]=11&items[1]=1&"
                    "items[2]=2&items[3]=3&items[4]=4&items[5]=5&items[6]=6&items[7]=7&items[8]=8&items[9]=9")
        self.assertEqual(canonical_body(body), expected)
        value = "post|/test|123.000|token=JWT|" + base64.b64encode(expected.encode()).decode()
        digest = base64.b64encode(hmac.new(b"key", value.encode(), hashlib.sha256).digest()).decode()
        self.assertEqual(signature("key", "POST", "/test", "123.000", "JWT", body), digest)

    def test_order_strings_booleans_and_empty_cards(self):
        body = {"quantity": "2", "price": "2.28", "card_ids": [], "commission_free": False, "order_type": "LO"}
        self.assertEqual(canonical_body(body), "commission_free=false&order_type=LO&price=2.28&quantity=2")
        self.assertEqual(signature("k", "POST", "/x", "1", "", {}), signature("k", "POST", "/x", "1", ""))
        for bad in ([1], {"price": 2.28}, {1: "value"}):
            with self.assertRaises(ValueError):
                canonical_body(bad)

    def test_timestamp_and_expiry_hint(self):
        with patch("gigamoney.signing.time.time_ns", return_value=1788834000123456789):
            self.assertEqual(request_timestamp(), "1788834000123.456")
        self.assertEqual(token_expiry(jwt(1234)), 1234)
        for bad in ("invalid", jwt(True), jwt(float("inf"))):
            with self.assertRaises(ValueError):
                token_expiry(bad)


class ApiTests(unittest.TestCase):
    def test_capture_route_preserves_signed_path_body_and_remote_host(self):
        from gigamoney.diagnostics import command_context
        self.original["capture_proxy"] = {"url": "http://127.0.0.1:18080", "upstream": self.original["host"]}
        save_session(self.path, self.original)
        client, opener = self.client(Response(), Response(refreshed("trade")))
        token = command_context.set("test / 中\ncommand")
        try:
            client.read()
            client.refresh("trade")
        finally:
            command_context.reset(token)
        for request in opener.requests:
            headers = {k.lower(): v for k, v in request.header_items()}
            self.assertTrue(request.full_url.startswith("http://127.0.0.1:18080/"))
            self.assertEqual(headers["host"], self.original["host"])
            self.assertNotIn("\n", headers["x-gigamoney-command-id"])
            body = json.loads(request.data) if request.data else None
            self.assertEqual(headers["x-api-signature"], signature(self.original["api_secret"], request.method,
                request.selector, headers["x-timestamp"], headers["authorization"], body))
        self.assertEqual(json.loads(opener.requests[-1].data), {"refresh_token": self.original["trade_refresh_token"]})

    def test_capture_route_rejects_nonlocal_or_ambiguous_destinations(self):
        for url in ("http://example.com:18080", "https://127.0.0.1:18080", "http://localhost:18080",
                    "http://127.0.0.1:18080/submit", "http://127.0.0.1:18080?x=y", "http://user@127.0.0.1:18080",
                    "http://127.0.0.1", "http://127.0.0.1:99999", "http://127.0.0.1:18080#anything"):
            with self.subTest(url=url):
                save_session(self.path, {**self.original, "capture_proxy": {"url": url, "upstream": self.original["host"]}})
                client, opener = self.client()
                with self.assertRaises(SessionError):
                    client.read()
                self.assertEqual(opener.requests, [])
        save_session(self.path, {**self.original, "capture_proxy": {"url": "http://127.0.0.1:18080", "upstream": "api.lbkrs.com"}})
        with self.assertRaises(SessionError):
            load_session(self.path)

    def test_capture_failure_does_not_fall_back_to_an_uncaptured_request(self):
        self.original["capture_proxy"] = {"url": "http://127.0.0.1:18080", "upstream": self.original["host"]}
        save_session(self.path, self.original)
        client, opener = self.client(URLError("Connection refused"))
        with self.assertRaises(SessionError):
            client.read()
        self.assertEqual(len(opener.requests), 1)
        self.assertTrue(opener.requests[0].full_url.startswith("http://127.0.0.1:18080/"))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "session.json"
        self.original = state()
        save_session(self.path, self.original)

    def client(self, *responses):
        opener = Opener(*responses)
        return ApiClient(self.path, opener=opener, clock=lambda: 1000), opener

    def test_fresh_signatures_and_ids_are_generated_for_each_read(self):
        client, opener = self.client(Response(), Response())
        with patch("gigamoney.api.request_timestamp", side_effect=["1000.001", "1000.002"]):
            client.read(); client.read()
        headers = [dict((k.lower(), v) for k, v in r.header_items()) for r in opener.requests]
        self.assertNotEqual(headers[0]["x-api-signature"], headers[1]["x-api-signature"])
        self.assertNotEqual(headers[0]["x-request-id"], headers[1]["x-request-id"])
        for request, header in zip(opener.requests, headers):
            self.assertEqual(request.full_url, "https://api-gl.lbkrs.com/v5/orders/today?limit=20")
            self.assertEqual(header["x-api-signature"], signature(self.original["api_secret"], "GET",
                             "/v5/orders/today?limit=20", header["x-timestamp"], header["authorization"]))
            self.assertIsNone(request.data)

    def test_expiring_sessions_rotate_in_order_before_read_and_persist(self):
        self.original.update(authorization=jwt(999), trade_token=jwt(1020))
        save_session(self.path, self.original)
        client, opener = self.client(Response(refreshed("main")), Response(refreshed("trade")), Response())
        client.read()
        self.assertEqual([r.method for r in opener.requests], ["POST", "POST", "GET"])
        self.assertTrue(opener.requests[0].full_url.endswith("/auth/refresh_token"))
        self.assertTrue(opener.requests[1].full_url.endswith("/member/trade/refresh_token"))
        saved = load_session(self.path)
        self.assertEqual(saved["authorization"], refreshed("main")["data"]["credentials"]["token"])
        self.assertEqual(saved["trade_refresh_token"], refreshed("trade")["data"]["token"]["refresh_token"])
        header = dict((k.lower(), v) for k, v in opener.requests[-1].header_items())
        self.assertEqual(header["authorization"], saved["authorization"])
        self.assertEqual(header["x-authorization"], saved["trade_token"])
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)
        self.assertEqual(list(self.path.parent.glob(".session-*")), [])
        # A separate process/client resumes with the rotated pair, without a refresh.
        resumed, resumed_opener = self.client(Response())
        resumed.read()
        self.assertEqual(len(resumed_opener.requests), 1)

    def test_expired_refresh_token_requires_signin_without_network_request(self):
        self.original.update(trade_token=jwt(999), trade_refresh_token=jwt(999))
        save_session(self.path, self.original)
        client, opener = self.client()
        with self.assertRaisesRegex(SessionError, "sign-in"):
            client.read()
        self.assertEqual(opener.requests, [])

    def test_failure_and_malformed_refresh_never_overwrite_valid_state(self):
        for response in (Response({"code": 401}, 401), Response({"code": 0, "data": {"token": {}}})):
            client, opener = self.client(response)
            with self.assertRaises((SessionError, ApiError)):
                client.refresh("trade")
            self.assertEqual(load_session(self.path), self.original)
            self.assertEqual(len(opener.requests), 1)

    def test_read_auth_recovery_is_bounded_to_one_retry(self):
        client, opener = self.client(Response({"code": 401}, 401), Response(refreshed("main")),
                                     Response(refreshed("trade")), Response({"code": 401}, 401))
        with self.assertRaises(AuthenticationRequired):
            client.read()
        self.assertEqual([r.method for r in opener.requests], ["GET", "POST", "POST", "GET"])

    def test_transport_failure_does_not_retry_or_leak_secrets(self):
        client, opener = self.client(URLError("private-secret-and-URL"))
        with self.assertLogs("gigamoney.api", level="INFO") as logs:
            with self.assertRaises(SessionError) as error:
                client.read()
        text = str(error.exception) + "\n".join(logs.output)
        for secret in ("private-secret-and-URL", "synthetic-secret", self.original["authorization"], "limit=20"):
            self.assertNotIn(secret, text)
        self.assertEqual(len(opener.requests), 1)
        self.assertIn("elapsed_ms=", text)

    def test_mutation_paths_redirects_and_foreign_hosts_are_rejected(self):
        client, opener = self.client()
        for path in ("/v3/orders/submit", "/v2/orders/withdraw", "/v1/tube/replace"):
            with self.assertRaises(ValueError):
                client._send(self.original, "POST", path, {"quantity": "1"})
        for bad in ("https://evil.example/v5/orders/today", "//evil.example/v5/orders/today", "/v3/orders/submit"):
            modified = {**self.original, "templates": {"orders": {"path": bad}}}
            save_session(self.path, modified)
            with self.assertRaises(SessionError):
                client.read()
        self.assertIsNone(NoRedirects().redirect_request(None, None, 302, "", {}, "https://evil.example"))
        self.assertEqual(opener.requests, [])

    def test_private_file_permissions_and_log_redaction(self):
        os.chmod(self.path, 0o644)
        with self.assertRaises(SessionError):
            load_session(self.path)
        os.chmod(self.path, 0o600)
        client, _ = self.client(Response({"code": 4001, "message": "secret-server-message"}, 200))
        with self.assertLogs("gigamoney.api", level="INFO") as logs, self.assertRaises(ApiError) as error:
            client.read()
        self.assertNotIn("secret-server-message", repr(error.exception.result))
        self.assertNotIn("secret-server-message", str(error.exception))
        self.assertNotIn("secret-server-message", "\n".join(logs.output))

    def test_status_and_cli_do_not_send_requests_or_print_tokens(self):
        config = self.path.parent / "config.json"
        config.write_text(json.dumps({"gigamoney": {"accountId": "TEST"}}))
        with patch("scripts.api.ApiClient") as fake, patch("sys.stdout", new_callable=io.StringIO) as output:
            fake.from_settings.return_value.status.return_value = {"expires_in_seconds": {"authorization": 600}}
            self.assertEqual(main(["--config", str(config), "--session", str(self.path), "status"]), 0)
            fake.from_settings.return_value.read.assert_not_called()
            fake.from_settings.return_value.refresh.assert_not_called()
            self.assertNotIn(self.original["authorization"], output.getvalue())


class TradeAuthenticationTests(unittest.TestCase):
    setUp = ApiTests.setUp
    client = ApiTests.client
    def enable_auth(self):
        self.original["trade_auth"] = {"type": "trade", "android_device_tag": "synthetic-device", "password_md5": "0" * 32}
        self.original.update(trade_token=jwt(999), trade_refresh_token=jwt(999))
        save_session(self.path, self.original)

    def test_expired_trade_refresh_reauthenticates_with_fresh_encryption_and_signature(self):
        from gigamoney.signing import encrypt_trade_password
        self.enable_auth()
        client, opener = self.client(Response(refreshed("trade")), Response())
        with patch("gigamoney.api.request_timestamp", side_effect=["1000000.123", "1000000.124"]):
            client.read()
        request = opener.requests[0]
        body = json.loads(request.data)
        self.assertTrue(request.full_url.endswith("/v3/member/trade/auth"))
        self.assertEqual(body, {"type": "trade", "android_device_tag": "synthetic-device",
            "trade_password": "ThVl98qBNH2RSJr/Pm5ND+UvR/YmTOFYBL/3W1fTYlTLJfApqbcFzTUzpHIQvXI5"})
        self.assertNotEqual(body["trade_password"], encrypt_trade_password("0" * 32, "1000000.124"))
        headers = dict((k.lower(), v) for k, v in request.header_items())
        self.assertEqual(headers["x-api-signature"], signature(self.original["api_secret"], "POST", "/v3/member/trade/auth",
                         "1000000.123", self.original["authorization"], body))
        self.assertEqual(load_session(self.path)["trade_token"], refreshed("trade")["data"]["token"]["token"])

    def test_revoked_trade_refresh_falls_back_to_password_once(self):
        self.enable_auth()
        self.original["trade_refresh_token"] = jwt(2000)
        save_session(self.path, self.original)
        client, opener = self.client(Response({"code": 402003}, 401), Response(refreshed("trade")), Response())
        client.read()
        self.assertEqual([r.full_url.split(".com")[-1].split("?")[0] for r in opener.requests],
                         ["/member/trade/refresh_token", "/v3/member/trade/auth", "/v5/orders/today"])

    def test_402_revocation_recovers_immediately_with_unexpired_tokens(self):
        self.enable_auth()
        self.original.update(trade_token=jwt(1300), trade_refresh_token=jwt(2800))
        save_session(self.path, self.original)
        client, opener = self.client(Response({"code": 402001}, 402), Response(refreshed("trade")), Response())
        self.assertTrue(client.read().ok)
        self.assertEqual([r.full_url.split(".com")[-1].split("?")[0] for r in opener.requests],
                         ["/v5/orders/today", "/v3/member/trade/auth", "/v5/orders/today"])

    def test_repeated_402_stops_after_one_authentication_attempt(self):
        self.enable_auth()
        self.original.update(trade_token=jwt(1300), trade_refresh_token=jwt(2800))
        save_session(self.path, self.original)
        client, opener = self.client(Response({"code": 402001}, 402), Response(refreshed("trade")), Response({"code": 402001}, 402))
        with self.assertRaises(AuthenticationRequired):
            client.read()
        self.assertEqual(len(opener.requests), 3)

    def test_main_refresh_phone_challenge_preserves_existing_session(self):
        self.original["authorization"] = jwt(999)
        save_session(self.path, self.original)
        client, opener = self.client(Response({"code": 0, "data": {"need_sms": True}}))
        with self.assertRaises(AuthenticationRequired):
            client.read()
        self.assertEqual(load_session(self.path), self.original)
        self.assertEqual(len(opener.requests), 1)

    def test_mfa_or_wrong_password_stops_without_overwriting_or_looping(self):
        self.enable_auth()
        for payload, status, error in (({"code": 4001}, 400, AuthenticationRequired),
                ({"code": 0, "data": {"need_totp": True}}, 200, SessionError)):
            client, opener = self.client(Response(payload, status))
            with self.assertRaises(error):
                client.read()
            self.assertEqual(len(opener.requests), 1)
            self.assertEqual(load_session(self.path), self.original)

    def test_malformed_trade_auth_is_rejected_without_network(self):
        self.enable_auth()
        self.original["trade_auth"]["password_md5"] = "not-a-digest"
        save_session(self.path, self.original)
        client, opener = self.client()
        with self.assertRaises(SessionError):
            client.read()
        self.assertEqual(opener.requests, [])

    def test_missing_route_uses_only_unique_active_matching_account_and_persists(self):
        response = {"code": 0, "data": {"status": [
            {"aaid": "fake-account", "org": {"org_id": "1"}, "account_channel": "9", "trade": True, "disable": False},
            {"aaid": "different", "org": {"org_id": "2"}, "account_channel": "4", "trade": True, "disable": False}]}}
        client, opener = self.client(Response(response))
        self.assertEqual(client.account_context(), {"aaid": "fake-account", "org_id": "1", "account_channel": "9"})
        self.assertEqual(client.account_context()["org_id"], "1")
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(load_session(self.path)["headers"]["x-target-org-id"], "1")

    def test_ambiguous_routing_never_picks_first_account(self):
        response = {"code": 0, "data": {"status": [
            {"aaid": "fake-account", "org": {"org_id": "1"}, "account_channel": channel, "trade": True, "disable": False}
            for channel in ("9", "10")]}}
        client, _ = self.client(Response(response))
        with self.assertRaisesRegex(SessionError, "ambiguous"):
            client.account_context()
        self.assertEqual(load_session(self.path), self.original)


if __name__ == "__main__":
    unittest.main()
