from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

from .adb import Adb
from .cancel_orders import CancelAutomation
from .config import execution_backend, load_settings
from .errors import GigaMoneyError
from .holdings import HoldingsAutomation
from .lock import EmulatorLock
from .monitor import main as monitor_main
from .monitor import monitor_status, start_monitor, stop_monitor
from .orders import OrderAutomation
from .runner import TradingRunner, command_plan, holdings_snapshot, request_runner_stop, runner_status
from .vision import OcrEngine, classify_png


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gigamoney", description="GigaMoney trading automation for Linux")
    parser.add_argument("--config", help="configuration JSON (or set GIGAMONEY_CONFIG)")
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("init-config", "validate-config", "render-service", "build-android"):
        sub.add_parser(command, help="local setup: " + command.replace("-", " "))
    bootstrap = sub.add_parser("api-bootstrap", help="inspect/import a verified account session or export its app profile")
    bootstrap.add_argument("action", choices=("inspect", "import", "export-profile"))
    bootstrap.add_argument("--from-emulator", action="store_true")

    sub.add_parser("device-check", help="validate ADB device, package, resolution, and density")
    sub.add_parser("snapshot-check", help="capture screenshot, XML, and OCR without changing the UI")
    sub.add_parser("holdings", help="query cash and positions as JSON")

    market = sub.add_parser("market-order", help="place or safely stage a market order")
    market.add_argument("--symbol", required=True)
    market.add_argument("--quantity", required=True)
    market.add_argument("--side", required=True, choices=("Buy", "Sell", "BUY", "SELL", "buy", "sell"))
    market.add_argument("--dry-run", action="store_true", help="navigate only; enter no values")
    market.add_argument("--no-submit", action="store_true", help="enter values but do not submit")

    limit = sub.add_parser("limit-order", help="place or safely stage a limit order")
    limit.add_argument("--symbol", required=True)
    limit.add_argument("--price", required=True)
    limit.add_argument("--quantity", required=True)
    limit.add_argument("--side", required=True, choices=("Buy", "Sell", "BUY", "SELL", "buy", "sell"))
    limit.add_argument("--kill", action="store_true", help="cancel any unfilled remainder after result")
    limit.add_argument("--dry-run", action="store_true", help="navigate only; enter no values")
    limit.add_argument("--no-submit", action="store_true", help="enter values but do not submit")

    cancel = sub.add_parser("cancel-all", help="cancel all detected in-progress orders")
    cancel.add_argument("--dry-run", action="store_true", help="report count without canceling")

    sub.add_parser("runner", help="run Kafka consumer and periodic holdings publisher in foreground")
    sub.add_parser("runner-status", help="report runner PID state")
    stop = sub.add_parser("runner-stop", help="request a graceful runner stop")
    stop.add_argument("--force", action="store_true", help="also send SIGTERM after validating the PID")

    monitor = sub.add_parser("monitor", help="run the local command center in the foreground")
    monitor.add_argument("--host")
    monitor.add_argument("--port", type=int)
    monitor.add_argument("--redirect-http-port", type=int)
    monitor.add_argument("--interval", type=int)
    monitor.add_argument("--cert-file")
    monitor.add_argument("--key-file")
    monitor_start = sub.add_parser("monitor-start", help="start the local command center in the background")
    monitor_start.add_argument("--host")
    monitor_start.add_argument("--port", type=int)
    monitor_start.add_argument("--redirect-http-port", type=int)
    monitor_start.add_argument("--interval", type=int)
    monitor_start.add_argument("--cert-file")
    monitor_start.add_argument("--key-file")
    monitor_start.add_argument("--open-browser", action="store_true")
    sub.add_parser("monitor-status", help="report command-center PID state")
    sub.add_parser("monitor-stop", help="stop the local command center")
    sub.add_parser("self-test", help="run dependency-free routing and conversion checks")
    return parser


def _json(value) -> None:
    print(json.dumps(value, indent=2, sort_keys=False))


def _locked(settings, *, device=False):
    if not device and execution_backend(settings) == "api":
        return nullcontext()
    return EmulatorLock(
        settings.work_dir / "emulator.lock",
        float(settings.get("automation.lockTimeoutSeconds", 300)),
    )


def _automation(settings, kind):
    if execution_backend(settings) == "api":
        from .api_trading import ApiTrading
        return ApiTrading(settings)
    return {"orders": OrderAutomation, "holdings": HoldingsAutomation, "cancel": CancelAutomation}[kind](settings)


def _self_test(settings) -> dict:
    market = command_plan({"type": "MARKET_ORDER", "symbol": "bili", "side": "BUY", "qty_shares": 2})
    limit = command_plan({
        "type": "LIMIT_ORDER", "symbol": "BILI", "side": "SELL", "qty_shares": 3,
        "limit_price": 17.38,
    })
    fok = command_plan({
        "type": "LIMIT_ORDER_FOK", "symbol": "BILI", "side": "BUY", "qty_shares": 4,
        "limit_price": 17.4,
    })
    try:
        command_plan({"type": "MARKET_ORDER", "symbol": "BILI", "side": "BUY", "notional_usd": 100})
        notional_guard = False
    except ValueError:
        notional_guard = True
    old_account = settings.data["gigamoney"].get("accountId")
    old_num = settings.data["gigamoney"].get("accountNumId")
    settings.data["gigamoney"]["accountId"] = "GIGA-A"
    settings.data["gigamoney"]["accountNumId"] = 7
    try:
        snapshot = holdings_snapshot({
            "queriedAt": "2026-07-10T12:00:00Z",
            "cash": {"balances": [{"currency": "USD", "balance": "$1,234.50"}]},
            "positions": [{
                "symbol": "bili", "quantity": "12.5", "cost": "$17.25",
                "marketPrice": "$18.50", "marketValue": "$231.25",
                "dailyPL": "$15.00", "dailyPLPercent": "6.94%",
            }],
        }, settings, True)
    finally:
        settings.data["gigamoney"]["accountId"] = old_account
        settings.data["gigamoney"]["accountNumId"] = old_num
    checks = {
        "marketRouting": market.kind == "market" and market.arguments["quantity"] == "2",
        "limitRouting": limit.kind == "limit" and not limit.arguments["kill"],
        "fokRouting": fok.kind == "limit" and fok.arguments["kill"],
        "notionalGuard": notional_guard,
        "snapshot": snapshot["cash"] == 1234.5 and snapshot["positions"][0]["dailyPLPercent"] == 6.94,
    }
    if not all(checks.values()):
        raise RuntimeError(f"Self-test failed: {checks}")
    return {"ok": True, "checks": checks}


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if arguments.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    try:
        if arguments.command in {"init-config", "validate-config", "render-service", "build-android"}:
            from .setup import main as setup_main
            return setup_main([*(["--config", arguments.config] if arguments.config else []), arguments.command])
        allow_missing = arguments.command in {
            "self-test", "device-check", "snapshot-check", "runner-status", "runner-stop",
            "monitor-status", "monitor-stop",
        }
        settings = load_settings(arguments.config, allow_missing=allow_missing)
        logging.getLogger().setLevel(logging.DEBUG if arguments.verbose else settings.get("logging.level", "INFO"))
        if arguments.command == "api-bootstrap":
            from .bootstrap import main as bootstrap_main
            params = ["--config", str(settings.path), arguments.action]
            if arguments.from_emulator:
                params.append("--from-emulator")
            if arguments.action == "export-profile":
                return bootstrap_main(params)
            import subprocess
            if not settings.proxy_python.is_file():
                raise RuntimeError("Install the proxy/bootstrap Python environment and set proxy.pythonPath; see docs/deployment.md.")
            return subprocess.run([str(settings.proxy_python), "-m", "gigamoney.bootstrap", *params],
                                  cwd=settings.root, check=False).returncode
        elif arguments.command == "self-test":
            _json(_self_test(settings))
        elif arguments.command == "device-check":
            with _locked(settings, device=True):
                _json(asdict(Adb(settings).validate(strict_layout=True)))
        elif arguments.command == "snapshot-check":
            with _locked(settings, device=True):
                adb = Adb(settings)
                info = adb.validate(strict_layout=True)
                screenshot = adb.screenshot()
                xml = adb.dump_xml()
                ocr = OcrEngine(settings).png(screenshot)
                _json({
                    "device": asdict(info),
                    "screenshotBytes": len(screenshot),
                    "uiNodeCount": sum(1 for _ in xml.iter("node")),
                    "ocrWordCount": len(ocr.words),
                    "ocrPreview": ocr.text[:500],
                    "screenClassification": classify_png(screenshot),
                })
        elif arguments.command == "holdings":
            with _locked(settings):
                _json(_automation(settings, "holdings").query())
        elif arguments.command == "market-order":
            if arguments.dry_run and arguments.no_submit:
                raise ValueError("--dry-run and --no-submit are mutually exclusive")
            with _locked(settings):
                _json(_automation(settings, "orders").market_order(
                    arguments.symbol, arguments.quantity, arguments.side,
                    dry_run=arguments.dry_run, no_submit=arguments.no_submit,
                ))
        elif arguments.command == "limit-order":
            if arguments.dry_run and arguments.no_submit:
                raise ValueError("--dry-run and --no-submit are mutually exclusive")
            with _locked(settings):
                _json(_automation(settings, "orders").limit_order(
                    arguments.symbol, arguments.price, arguments.quantity, arguments.side,
                    dry_run=arguments.dry_run, no_submit=arguments.no_submit, kill=arguments.kill,
                ))
        elif arguments.command == "cancel-all":
            with _locked(settings):
                _json(_automation(settings, "cancel").cancel_all(dry_run=arguments.dry_run))
        elif arguments.command == "runner":
            TradingRunner(settings).run()
        elif arguments.command == "runner-status":
            _json(runner_status(settings))
        elif arguments.command == "runner-stop":
            _json(request_runner_stop(settings, force=arguments.force))
        elif arguments.command == "monitor":
            forwarded = ["--config", str(settings.path)]
            for option, value in (
                ("--host", arguments.host), ("--port", arguments.port),
                ("--redirect-http-port", arguments.redirect_http_port),
                ("--interval", arguments.interval), ("--cert-file", arguments.cert_file),
                ("--key-file", arguments.key_file),
            ):
                if value is not None:
                    forwarded.extend((option, str(value)))
            return monitor_main(forwarded)
        elif arguments.command == "monitor-start":
            _json(start_monitor(
                settings, host=arguments.host, port=arguments.port,
                redirect_http_port=arguments.redirect_http_port,
                interval=arguments.interval, cert_file=arguments.cert_file,
                key_file=arguments.key_file, open_browser=arguments.open_browser,
            ))
        elif arguments.command == "monitor-status":
            _json(monitor_status(settings))
        elif arguments.command == "monitor-stop":
            _json(stop_monitor(settings))
        return 0
    except (GigaMoneyError, ValueError, RuntimeError) as exc:
        logging.getLogger("gigamoney").error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
