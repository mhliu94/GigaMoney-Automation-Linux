from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigurationError

PACKAGE = "lb.whale.hkwinner.android"


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


DEFAULTS: dict[str, Any] = {
    "gigamoney": {
        "package": PACKAGE,
        "tradePassword": "",
        "accountId": "",
        "accountNumId": None,
        "tradingEnabledDefault": True,
        "foregroundLaunchTimeoutSeconds": 15,
    },
    "adb": {
        "path": "",
        "serial": "",
        "expectedResolution": "1080x2424",
        "expectedDensity": 420,
        "commandTimeoutSeconds": 30,
    },
    "ocr": {"command": "tesseract", "language": "eng"},
    "automation": {
        "lockTimeoutSeconds": 300,
        "maxHomeBacks": 3,
        "maxWatchlistScrolls": 8,
        "maxPositionScrolls": 8,
        "maxCancelCount": 50,
    },
    "kafka": {
        "bootstrapServers": "",
        "commandTopic": "trading-commands",
        "accountDetailsTopic": "account-details",
        "consumerGroupId": "gigamoney-order-runner",
        "autoOffsetReset": "latest",
        "pollTimeoutSeconds": 1,
        "maxPollIntervalMilliseconds": 900000,
        "maxCommandAgeSeconds": 300,
        "commitFailedCommands": True,
        "holdingsIntervalSeconds": 120,
        "clientVersion": "2.15.0",
    },
    "monitor": {
        "serverId": "",
        "host": "127.0.0.1",
        "port": 8787,
        "redirectHttpPort": 0,
        "publicHost": "",
        "intervalSeconds": 60,
        "certFile": "",
        "keyFile": "",
        "adminPassword": "",
        "avd": "",
        "emulatorPath": "",
        "emulatorArgs": ["-netdelay", "none", "-netspeed", "full", "-no-snapshot-load"],
        "emulatorBootTimeoutSeconds": 300,
        "display": "",
        "runnerLog": "logs/runner.log",
    },
}


@dataclass(frozen=True)
class Settings:
    data: dict[str, Any]
    path: Path
    root: Path

    def get(self, dotted: str, default: Any = None) -> Any:
        current: Any = self.data
        for part in dotted.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def require(self, dotted: str) -> Any:
        value = self.get(dotted)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ConfigurationError(f"Required setting is empty: {dotted}")
        return value

    @property
    def work_dir(self) -> Path:
        path = self.root / "work"
        path.mkdir(parents=True, exist_ok=True)
        return path


def default_config_path() -> Path:
    env_path = os.environ.get("GIGAMONEY_CONFIG")
    return Path(env_path).expanduser() if env_path else project_root() / "config" / "gigamoney.config.json"


def load_settings(path: str | Path | None = None, *, allow_missing: bool = False) -> Settings:
    config_path = Path(path).expanduser().resolve() if path else default_config_path().resolve()
    raw: dict[str, Any] = {}
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigurationError(f"Cannot read {config_path}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise ConfigurationError(f"Configuration root must be an object: {config_path}")
        raw = loaded
    elif not allow_missing:
        raise ConfigurationError(
            f"Configuration not found: {config_path}. Copy config/gigamoney.config.example.json first."
        )
    return Settings(_merge(DEFAULTS, raw), config_path, project_root())


def find_adb(configured: str = "") -> str:
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())
    for variable in ("ANDROID_SDK_ROOT", "ANDROID_HOME"):
        value = os.environ.get(variable)
        if value:
            candidates.append(Path(value) / "platform-tools" / "adb")
    candidates.append(Path.home() / "Android" / "Sdk" / "platform-tools" / "adb")
    found = shutil.which("adb")
    if found:
        candidates.append(Path(found))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
    raise ConfigurationError(
        "adb was not found. Set adb.path or ANDROID_SDK_ROOT, or add platform-tools to PATH."
    )
