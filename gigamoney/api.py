"""Read-only mobile API client with local signing and renewable sessions.

Runtime requests are limited to account reads, session refresh and trade
authentication. Order writes require the explicit TradingApiClient subclass.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import fcntl
import json
import logging
import os
from pathlib import Path
import stat
import tempfile
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener
import uuid

from .signing import encrypt_trade_password, request_timestamp, signature, token_expiry
from .diagnostics import command_context

LOG = logging.getLogger(__name__)
API_HOSTS = frozenset({"api-gl.lbkrs.com", "api.lbkrs.com"})
READ_PATHS = {
    "accounts": "/v3/account/list",
    "orders": "/v5/orders/today",
    "positions": "/v6/portfolio/detail",
    "cash": "/v5/portfolio/cash_info",
    "trade_check": "/v1/member/trade/check_token",
    "order_info": "/v5/orders/info",
    "order_detail": "/v3/orders/detail",
    "order_history": "/v4/orders/history",
    "quote": "/v3/quote/stock/detail",
    "market_status": "/v1/quote/market/status",
    "cards": "/v1/commssion/default/cards",
}
REFRESH_PATHS = {"main": "/auth/refresh_token", "trade": "/member/trade/refresh_token"}
TRADE_AUTH_PATH = "/v3/member/trade/auth"
GENERATED_HEADERS = frozenset({
    "authorization", "x-authorization", "x-api-signature", "x-timestamp",
    "x-request-id", "uber-trace-id", "host", "content-length", "connection",
    "accept-encoding", "content-type", "cookie", "proxy-authorization",
    "x-gigamoney-command-id",
})


class SessionError(RuntimeError):
    """A session must be renewed or bootstrapped again; messages contain no secrets."""


class AuthenticationRequired(SessionError):
    """Human verification or replacement account credentials are required."""


class TransportError(SessionError):
    """A read can be retried; a mutation must first be reconciled."""


@dataclass(frozen=True)
class ApiResult:
    path: str
    http_status: int
    api_code: int | None
    elapsed_ms: float
    request_id: str
    payload: dict[str, Any] = field(repr=False)

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and self.api_code == 0

    def summary(self) -> dict[str, Any]:
        return {"ok": self.ok, "path": self.path, "http_status": self.http_status,
                "api_code": self.api_code, "elapsed_ms": self.elapsed_ms,
                "request_id": self.request_id, "uses_captured_signature": False}


class ApiError(RuntimeError):
    def __init__(self, result: ApiResult):
        self.result = result
        super().__init__(f"API request failed: HTTP {result.http_status}, code {result.api_code}.")


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def save_session(path: Path, state: dict[str, Any]) -> None:
    """Atomically persist rotated credentials in an owner-only file and directory."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(path.parent.stat().st_mode) & 0o077:
        raise SessionError("Session directory must be private (mode 700).")
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".session-", delete=False) as stream:
            name = stream.name
            os.fchmod(stream.fileno(), 0o600)
            json.dump(state, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        name = None
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if name is not None:
            os.unlink(name)


def load_session(path: Path) -> dict[str, Any]:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, encoding="utf-8") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
                raise SessionError("Session file must be private (mode 600).")
            state = json.load(stream)
        if not isinstance(state, dict) or state.get("version") != 1 or state.get("host") not in API_HOSTS:
            raise ValueError
        for key in ("api_secret", "authorization", "refresh_token", "trade_token", "trade_refresh_token"):
            value = state.get(key)
            if not isinstance(value, str) or not value or "\r" in value or "\n" in value:
                raise ValueError
        headers = state.get("headers")
        if not isinstance(headers, dict) or not headers.get("x-api-key"):
            raise ValueError
        for key, value in headers.items():
            if (not isinstance(key, str) or key != key.lower() or key in GENERATED_HEADERS or
                    not isinstance(value, str) or any(c in key + value for c in "\r\n")):
                raise ValueError
        templates = state.get("templates")
        if not isinstance(templates, dict):
            raise ValueError
        for endpoint, template in templates.items():
            if (endpoint not in READ_PATHS or not isinstance(template, dict) or
                    not valid_read_path(endpoint, template.get("path"))):
                raise ValueError
        capture_origin(state)
        auth = state.get("trade_auth")
        if auth is not None:
            if (not isinstance(auth, dict) or set(auth) != {"android_device_tag", "password_md5", "type"}
                    or auth["type"] != "trade" or not isinstance(auth["android_device_tag"], str)
                    or not auth["android_device_tag"] or not isinstance(auth["password_md5"], str)
                    or len(auth["password_md5"]) != 32
                    or any(c not in "0123456789abcdef" for c in auth["password_md5"])):
                raise ValueError
        return state
    except (OSError, ValueError, TypeError):
        raise SessionError("Cannot load a valid private API session file.") from None


def valid_read_path(endpoint: str, path: Any) -> bool:
    if not isinstance(path, str) or any(c.isspace() or ord(c) < 32 for c in path):
        return False
    parts = urlsplit(path)
    return (endpoint in READ_PATHS and not parts.scheme and not parts.netloc and not parts.fragment
            and parts.path == READ_PATHS[endpoint])


def capture_origin(state: dict[str, Any]) -> str | None:
    """Optional, explicit loopback capture route; never honor ambient proxies."""
    capture = state.get("capture_proxy")
    if capture is None:
        return None
    if not isinstance(capture, dict) or set(capture) != {"url", "upstream"} or capture["upstream"] != state["host"]:
        raise ValueError("Invalid capture proxy configuration.")
    url = capture["url"]
    if not isinstance(url, str) or any(c.isspace() or ord(c) < 32 for c in url):
        raise ValueError("Invalid capture proxy URL.")
    parts = urlsplit(url)
    if (parts.scheme != "http" or parts.hostname != "127.0.0.1" or not parts.port
            or parts.username is not None or parts.password is not None or parts.path
            or parts.query or parts.fragment or parts.netloc != f"127.0.0.1:{parts.port}"):
        raise ValueError("Capture proxy must use an explicit 127.0.0.1 HTTP port.")
    return url


class ApiClient:
    def __init__(self, session_path: Path, *, opener=None, timeout: float = 15,
                 clock=time.time, refresh_margin: float = 60, lock_timeout: float = 30,
                 expected_account: str | None = None, expected_route: dict | None = None,
                 max_response_bytes: int = 4194304):
        self.session_path = Path(session_path)
        # No ambient proxy and no redirects: credentials stay on the selected API host.
        self.opener = opener or build_opener(ProxyHandler({}), HTTPSHandler(), NoRedirects())
        self.timeout = timeout
        self.clock = clock
        self.refresh_margin = refresh_margin
        self.lock_timeout = lock_timeout
        self.expected_account = expected_account
        self.expected_route = expected_route or {}
        self.max_response_bytes = max_response_bytes

    @classmethod
    def from_settings(cls, settings, **kwargs):
        return cls(settings.session_path,
                   timeout=float(settings.get("api.requestTimeoutSeconds", 15)),
                   refresh_margin=float(settings.get("api.refreshMarginSeconds", 60)),
                   lock_timeout=float(settings.get("api.sessionLockTimeoutSeconds", 30)),
                   max_response_bytes=int(settings.get("api.maxResponseBytes", 4194304)),
                   expected_account=str(settings.require("gigamoney.accountId")),
                   expected_route={header: str(settings.get(key)) for header, key in (
                       ("x-target-aaid", "api.accountAaid"), ("x-target-org-id", "api.orgId"),
                       ("account-channel", "api.accountChannel")) if settings.get(key)}, **kwargs)

    @contextmanager
    def _session(self):
        """Serialize refreshes across CLI processes and reload the latest rotation."""
        path = self.session_path
        if not path.parent.is_dir() or stat.S_IMODE(path.parent.stat().st_mode) & 0o077:
            if self.expected_account is not None:
                raise AuthenticationRequired("Restore this account's private API session directory (mode 700) before starting.")
            raise SessionError("Session directory must exist and be private (mode 700).")
        fd = os.open(path.with_suffix(path.suffix + ".lock"),
                     os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            deadline = time.monotonic() + self.lock_timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise SessionError("Timed out waiting for the API session lock.") from None
                    time.sleep(0.1)
            try:
                state = load_session(path)
            except SessionError:
                if self.expected_account is not None:
                    raise AuthenticationRequired("A valid private session for this account is required; import it from the verified emulator.") from None
                raise
            if self.expected_account is not None and state.get("account_id") != self.expected_account:
                raise AuthenticationRequired("API session is not bound to this configured account; import its verified session.")
            if any(state["headers"].get(key) != value for key, value in self.expected_route.items()):
                raise AuthenticationRequired("Configured broker account does not match the API session.")
            yield state
        finally:
            os.close(fd)

    def status(self) -> dict[str, Any]:
        with self._session() as state:
            return {"host": state["host"], "endpoints": sorted(state["templates"]),
                    "expires_in_seconds": {
                        key: round(token_expiry(state[key]) - self.clock())
                        for key in ("authorization", "refresh_token", "trade_token", "trade_refresh_token")
                    }}

    def _permitted(self, method: str, path: str, body: dict[str, Any] | None) -> bool:
        return ((method == "GET" and body is None and
                      any(valid_read_path(name, path) for name in READ_PATHS)) or
                     (method == "POST" and path in REFRESH_PATHS.values() and
                      isinstance(body, dict) and set(body) == {"refresh_token"}) or
                     (method == "POST" and path == TRADE_AUTH_PATH and isinstance(body, dict) and
                      set(body) == {"android_device_tag", "password_md5", "type"} and body["type"] == "trade"))

    def _send(self, state: dict[str, Any], method: str, path: str,
              body: dict[str, Any] | None = None) -> ApiResult:
        if not self._permitted(method, path, body) or state["host"] not in API_HOSTS:
            raise ValueError("Only account reads and session refreshes are supported.")
        timestamp, request_id = request_timestamp(), uuid.uuid4().hex
        if path == TRADE_AUTH_PATH:
            body = {"type": "trade", "android_device_tag": body["android_device_tag"],
                    "trade_password": encrypt_trade_password(body["password_md5"], timestamp)}
        headers = {**state["headers"], "authorization": state["authorization"],
                   "x-authorization": state["trade_token"], "x-timestamp": timestamp,
                   "x-request-id": request_id, "accept-encoding": "identity"}
        headers["x-api-signature"] = signature(state["api_secret"], method, path, timestamp,
                                              state["authorization"], body)
        content = None
        if body is not None:
            headers["content-type"] = "application/json"
            content = json.dumps(body, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        origin = capture_origin(state)
        if origin:
            # The reverse proxy forwards the unchanged path/body over verified
            # upstream TLS. This cleartext hop never leaves the loopback interface.
            headers["host"] = state["host"]
            if command_context.get():
                headers["x-gigamoney-command-id"] = quote(command_context.get(), safe="")
        request = Request((origin or "https://" + state["host"]) + path, method=method, data=content, headers=headers)
        public_path = path.split("?", 1)[0]
        started = time.perf_counter()
        LOG.info("api_started method=%s path=%s request_id=%s command_id=%s capture_proxy=%s",
                 method, public_path, request_id, command_context.get(), bool(origin))
        try:
            try:
                response = self.opener.open(request, timeout=self.timeout)
            except HTTPError as exc:
                response = exc
            with response:
                raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise ValueError("Response exceeds size limit.")
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("Response must be an object.")
                code = payload.get("code")
                code = code if type(code) is int else None
                result = ApiResult(public_path, response.code, code,
                                   round((time.perf_counter() - started) * 1000, 2), request_id, payload)
        except (OSError, URLError, ValueError) as exc:
            LOG.warning("api_failed method=%s path=%s request_id=%s elapsed_ms=%.2f error_type=%s",
                        method, public_path, request_id, (time.perf_counter() - started) * 1000,
                        type(exc).__name__)
            raise TransportError("API transport or response failure; no automatic network retry was made.") from None
        LOG.info("api_finished method=%s path=%s request_id=%s http_status=%s api_code=%s elapsed_ms=%.2f ok=%s",
                 method, public_path, request_id, result.http_status, result.api_code, result.elapsed_ms, result.ok)
        return result

    def _refresh(self, state: dict[str, Any], kind: str) -> ApiResult:
        refresh_key = "refresh_token" if kind == "main" else "trade_refresh_token"
        token_key = "authorization" if kind == "main" else "trade_token"
        if token_expiry(state[refresh_key]) <= self.clock():
            if kind == "trade" and state.get("trade_auth"):
                return self._authenticate_trade(state)
            raise AuthenticationRequired(f"The {kind} refresh token has expired; sign-in is required.")
        result = self._send(state, "POST", REFRESH_PATHS[kind], {"refresh_token": state[refresh_key]})
        if not result.ok:
            if kind == "trade" and (result.http_status == 401 or result.api_code == 402003) and state.get("trade_auth"):
                return self._authenticate_trade(state)
            if result.http_status < 500:
                raise AuthenticationRequired(f"The {kind} session cannot be refreshed; verification in the app is required.")
            raise ApiError(result)
        return self._save_tokens(state, kind, result)

    def _authenticate_trade(self, state: dict[str, Any]) -> ApiResult:
        if not state.get("trade_auth"):
            raise AuthenticationRequired("Trading credentials are missing; import the verified account session.")
        body = state["trade_auth"]
        result = self._send(state, "POST", TRADE_AUTH_PATH, body)
        if not result.ok:
            if result.http_status < 500:
                raise AuthenticationRequired("Trade authentication was rejected; verify credentials or complete verification in the app.")
            raise ApiError(result)
        data = result.payload.get("data", {})
        if not isinstance(data, dict) or any(value for key, value in data.items() if key.startswith("need_")):
            raise AuthenticationRequired("Trade authentication requires verification in the app.")
        return self._save_tokens(state, "trade", result)

    def _save_tokens(self, state: dict[str, Any], kind: str, result: ApiResult) -> ApiResult:
        data = result.payload.get("data", {})
        if isinstance(data, dict) and any(value for key, value in data.items() if key.startswith("need_")):
            raise AuthenticationRequired(f"The {kind} session requires verification in the app.")
        refresh_key = "refresh_token" if kind == "main" else "trade_refresh_token"
        token_key = "authorization" if kind == "main" else "trade_token"
        try:
            tokens = result.payload["data"]["credentials" if kind == "main" else "token"]
            access, refresh = tokens["token"], tokens["refresh_token"]
            if (not isinstance(access, str) or not isinstance(refresh, str) or
                    token_expiry(access) <= self.clock() or token_expiry(refresh) <= self.clock()):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise SessionError("Refresh returned an unusable token pair; session was not overwritten.") from None
        state[token_key], state[refresh_key] = access, refresh
        state["updated_at"] = self.clock()
        save_session(self.session_path, state)
        LOG.info("api_session_refreshed kind=%s expires_in_s=%d", kind, token_expiry(access) - self.clock())
        return result

    def _ensure_main(self, state: dict[str, Any]) -> None:
        if token_expiry(state["authorization"]) <= self.clock() + self.refresh_margin:
            self._refresh(state, "main")

    def refresh(self, kind: str = "trade") -> ApiResult:
        if kind not in REFRESH_PATHS:
            raise ValueError("Unknown session kind.")
        with self._session() as state:
            if kind == "trade":
                self._ensure_main(state)
            return self._refresh(state, kind)

    def ensure_session(self, state: dict[str, Any]) -> None:
        self._ensure_main(state)
        if token_expiry(state["trade_token"]) <= self.clock() + self.refresh_margin:
            self._refresh(state, "trade")

    def account_context(self) -> dict[str, str]:
        with self._session() as state:
            headers = state["headers"]
            result = {"aaid": headers.get("x-target-aaid", ""),
                      "org_id": headers.get("x-target-org-id", ""),
                      "account_channel": headers.get("account-channel", "")}
            if not all(result.values()):
                # Older bootstrap files omitted org_id on account-wide GETs.
                # Fill it only from one active account matching every saved route.
                self.ensure_session(state)
                response = self._send(state, "GET", READ_PATHS["accounts"])
                if not response.ok:
                    raise ApiError(response)
                matches = []
                for row in response.payload.get("data", {}).get("status", []):
                    route = {"aaid": str(row.get("aaid", "")),
                             "org_id": str(row.get("org", {}).get("org_id", "")),
                             "account_channel": str(row.get("account_channel", ""))}
                    if (all(route.values()) and row.get("trade") is True and not row.get("disable")
                            and all(not v or route[k] == v for k, v in result.items())):
                        matches.append(route)
                if len(matches) != 1:
                    raise SessionError("API account routing is ambiguous; bootstrap the selected account again.")
                result = matches[0]
                headers.update({"x-target-aaid": result["aaid"], "x-target-org-id": result["org_id"],
                                "account-channel": result["account_channel"]})
                save_session(self.session_path, state)
            return result

    def read(self, endpoint: str = "orders", params: dict[str, Any] | None = None) -> ApiResult:
        if endpoint not in READ_PATHS:
            raise ValueError("Unknown read-only endpoint.")
        with self._session() as state:
            template = state["templates"].get(endpoint)
            if template is None and params is None:
                raise SessionError("This read-only endpoint has no bootstrap template.")
            path = template["path"] if template else READ_PATHS[endpoint]
            if params is not None:
                parts = urlsplit(path)
                # Automated order reads specify a complete account-wide query.
                # Captured app tabs can carry status/symbol filters that hide
                # completed or unrelated orders from reporting and reconciliation.
                query = {} if endpoint == "orders" else dict(parse_qsl(parts.query, keep_blank_values=True))
                query.update({str(k): str(v) for k, v in params.items()})
                path = parts.path + ("?" + urlencode(sorted(query.items())) if query else "")
            self.ensure_session(state)
            result = self._send(state, "GET", path)
            # Authentication rejection gets one recovery attempt. Network failures
            # and other errors are surfaced immediately, without replay loops.
            if result.http_status == 401:
                self._refresh(state, "main")
                self._refresh(state, "trade")
                result = self._send(state, "GET", path)
            elif result.http_status == 402 and result.api_code == 402001:
                # This observed response revokes trade authorization before JWT
                # expiry. Reauthenticate now; only replay this read, never an order.
                self._authenticate_trade(state)
                result = self._send(state, "GET", path)
            if result.http_status == 401 or (result.http_status == 402 and result.api_code == 402001):
                raise AuthenticationRequired("Authentication recovery failed; verification in the app is required.")
            if not result.ok:
                raise ApiError(result)
            return result
