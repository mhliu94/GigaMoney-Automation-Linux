#!/usr/bin/env python3
"""Use the emulator-free, read-only client, or verify signatures offline."""
from __future__ import annotations

import argparse
from collections import Counter
import hmac
import json
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gigamoney.api import ApiClient, ApiError, SessionError, load_session, READ_PATHS
from gigamoney.signing import signature
from gigamoney.config import load_settings


def verify_capture(session_path: Path, capture: Path) -> dict:
    # mitmproxy is only needed for offline capture verification, never runtime HTTP.
    from scripts.proxy_inspect import flows

    secret = load_session(session_path)["api_secret"]
    matched, failed, unsupported = Counter(), Counter(), Counter()
    for flow in flows(capture):
        if flow.type != "http":
            continue
        request = flow.request
        recorded = request.headers.get("x-api-signature")
        if not recorded:
            continue
        label = request.method + " " + request.path.split("?", 1)[0]
        try:
            body = json.loads(request.content) if request.content else None
            computed = signature(secret, request.method, request.path, request.headers["x-timestamp"],
                                 request.headers.get("authorization", ""), body)
        except (ValueError, TypeError, KeyError):
            unsupported[label] += 1
            continue
        (matched if hmac.compare_digest(computed, recorded) else failed)[label] += 1
    return {"ok": bool(matched) and not failed and not unsupported,
            "matched": sum(matched.values()), "failed": sum(failed.values()),
            "unsupported": sum(unsupported.values()), "matched_workflows": dict(sorted(matched.items())),
            "failed_workflows": dict(sorted(failed.items())), "unsupported_workflows": dict(sorted(unsupported.items())),
            "network_requests_sent": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("--session", type=Path)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("status", help="Show expiry metadata without sending a request")
    read = sub.add_parser("read", help="Read account data with a fresh signature")
    read.add_argument("endpoint", choices=READ_PATHS, default="orders", nargs="?")
    read.add_argument("--data", action="store_true", help="Also print private account response data")
    refresh = sub.add_parser("refresh", help="Explicitly renew a session and save the rotated tokens")
    refresh.add_argument("kind", choices=("main", "trade", "both"), default="both", nargs="?")
    verify = sub.add_parser("verify-capture", help="Compare captured signatures offline; never replay requests")
    verify.add_argument("capture", type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = load_settings(args.config)
        logging.getLogger().setLevel(settings.get("logging.level", "INFO"))
        client = ApiClient.from_settings(settings)
        if args.session:
            client.session_path = args.session.expanduser().resolve()
        if args.action == "status":
            output = client.status()
        elif args.action == "read":
            result = client.read(args.endpoint)
            output = result.summary()
            if args.data:
                output["data"] = result.payload.get("data")
        elif args.action == "refresh":
            kinds = ("main", "trade") if args.kind == "both" else (args.kind,)
            output = {"ok": True, "refreshes": [client.refresh(kind).summary() for kind in kinds]}
        else:
            output = verify_capture(client.session_path, args.capture)
        print(json.dumps(output, indent=2))
        return 0 if output.get("ok", True) else 1
    except ApiError as exc:
        print(json.dumps(exc.result.summary(), indent=2))
        return 1
    except (SessionError, OSError, ValueError, RuntimeError, ImportError) as exc:
        # Avoid printing raw exception strings from parsers, files or networking.
        output = {"ok": False, "error_type": type(exc).__name__}
        if isinstance(exc, SessionError):
            output["message"] = str(exc)
        print(json.dumps(output, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
