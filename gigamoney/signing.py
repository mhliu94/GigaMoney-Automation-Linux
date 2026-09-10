"""GigaMoney mobile HTTP signatures, implemented without the Android runtime.

This protocol was checked against app 4.72.1 captures. It is distinct from
Longbridge's public OpenAPI signature scheme. Credentials are supplied locally.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import time
from typing import Any


def canonical_body(body: dict[str, Any] | None) -> str:
    """Flatten JSON, sort the complete bracketed keys, and join without escaping.

    Empty containers and nulls contribute no pairs. Array indices sort as text:
    ``items[10]`` comes before ``items[2]``. Values retain their original case.
    Float formatting has not been verified; use decimal strings for quantities
    and prices, as the app does.
    """
    if body is None:
        return ""
    if not isinstance(body, dict):
        raise ValueError("The mobile API body must be a JSON object.")
    pairs: list[tuple[str, str]] = []

    def visit(key: str, value: Any) -> None:
        if isinstance(value, dict):
            for name, child in value.items():
                if not isinstance(name, str):
                    raise ValueError("JSON object keys must be strings.")
                visit(f"{key}[{name}]" if key else name, child)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(f"{key}[{index}]", child)
        elif value is None:
            return
        elif isinstance(value, str):
            pairs.append((key, value))
        elif isinstance(value, (bool, int)):
            pairs.append((key, json.dumps(value)))
        else:
            raise ValueError("Use strings for decimal values; this JSON type is not supported.")

    visit("", body)
    return "&".join(f"{key}={value}" for key, value in sorted(pairs))


def signature(secret: str, method: str, path: str, timestamp: str,
              authorization: str, body: dict[str, Any] | None = None) -> str:
    """Sign the exact path/query and authorization header, with fresh time supplied.

    The secret is used as UTF-8 text, including when it resembles hexadecimal.
    Only the HTTP method is lowercased. Never log the constructed signing input.
    """
    if not secret:
        raise ValueError("A signing secret is required.")
    value = f"{method.lower()}|{path}|{timestamp}|token={authorization}"
    flattened = canonical_body(body)
    if flattened:
        value += "|" + base64.b64encode(flattened.encode("utf-8")).decode("ascii")
    digest = hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def request_timestamp() -> str:
    """Unix milliseconds with three fractional digits, matching the mobile app."""
    now = time.time_ns()
    return f"{now // 1_000_000}.{now // 1_000 % 1_000:03d}"


def encrypt_trade_password(password_md5: str, timestamp: str) -> str:
    """Match the app's ParamEncrypt: AES-CBC of the password digest, keyed by time.

    This is protocol compatibility, not a recommended password-storage scheme.
    The password digest is a credential and must stay in the private bootstrap.
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.padding import PKCS7

    if len(password_md5) != 32 or any(c not in "0123456789abcdef" for c in password_md5):
        raise ValueError("Trade password digest is invalid.")
    key = hashlib.md5(timestamp.encode("utf-8")).hexdigest().encode("ascii")
    padder = PKCS7(128).padder()
    padded = padder.update(password_md5.encode("ascii")) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode("ascii")


def token_expiry(token: str) -> float:
    """Read JWT expiry only as a scheduling hint; this does not authenticate a JWT."""
    try:
        segment = token.removeprefix("Bearer ").split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
        expiry = claims["exp"]
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry):
            raise ValueError
        return float(expiry)
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        raise ValueError("Session token has no usable expiry.") from None
