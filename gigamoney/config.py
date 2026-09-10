from __future__ import annotations

import json
import copy
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
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


DEFAULTS: dict[str, Any] = {
    "paths": {"workDir": "work", "logDir": "logs"},
    "logging": {"level": "INFO"},
    "gigamoney": {
        "backend": "android",
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
    "api": {
        "sessionPath": "",
        "accountAaid": "",
        "orgId": "",
        "accountChannel": "",
        "allowedSymbols": ["NVDA", "BIYA", "AEHL", "TANH"],
        "maxOrderQuantity": None,
        "maxOrders": None,
        "maxOrderPages": 100,
        "fokWaitSeconds": 2,
        "positionCost": "diluted",
        "extendedHoursMarketOrders": True,
        "marketStatusMaxAgeSeconds": 30,
        "requestTimeoutSeconds": 15,
        "maxResponseBytes": 4194304,
        "refreshMarginSeconds": 60,
        "sessionLockTimeoutSeconds": 30,
        "readAttempts": 2,
        "readRetryDelaySeconds": 0.25,
        "cancelPollDelaysSeconds": [0, 1, 1, 2],
        "bootstrap": {"capturePath": "", "appProfilePath": "config/api-app-profile.json",
                      "maxCaptureAgeSeconds": 1800},
    },
    "proxy": {"stateDir": "", "pythonPath": "", "apiPort": 18080,
              "startupTimeoutSeconds": 15, "evidenceMaxAgeSeconds": 300,
              "boot": {"sourceImage": "", "magiskApk": "", "outputDirectory": ""}},
    "androidBuild": {"sdkRoot": "", "platform": "", "buildToolsVersion": "",
                     "javaHome": "", "junitJar": "", "outputJar": "android/gigamoney-zero-idle-dump.jar"},
    "service": {"name": "gigamoney-runner", "restartSeconds": 10},
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
        "consumerGroupId": "",
        "priceBooksTopic": "price-books",
        "priceBookMaxAgeSeconds": 300,
        "priceBookPollIntervalSeconds": 30,
        "priceBookLookbackSeconds": 3600,
        "priceBookTimeoutSeconds": 30,
        "priceBookBatchSize": 500,
        "autoOffsetReset": "latest",
        "pollTimeoutSeconds": 1,
        "maxPollIntervalMilliseconds": 900000,
        "maxCommandAgeSeconds": 300,
        "commitFailedCommands": True,
        "holdingsIntervalSeconds": 30,
        "clientVersion": "2.15.0",
        "sessionTimeoutMilliseconds": 10000,
        "deliveryTimeoutSeconds": 10,
        "producerLingerMilliseconds": 5,
        "clientConfig": {},
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
        "adminUsername": "admin",
        "sessionLifetimeSeconds": 28800,
        "loginWindowSeconds": 300,
        "loginLockoutSeconds": 900,
        "maxLoginFailures": 5,
        "avd": "",
        "emulatorPath": "",
        "emulatorArgs": ["-netdelay", "none", "-netspeed", "full", "-no-snapshot-load"],
        "emulatorBootTimeoutSeconds": 300,
        "display": "",
        "runnerLog": "",
        "restart": {"enabled": True, "timezone": "America/New_York", "start": "20:01", "end": "21:00",
                    "checkIntervalSeconds": 30, "minIntervalSeconds": 3600,
                    "runnerStopTimeoutSeconds": 360, "runnerPollSeconds": 0.5,
                    "emulatorStopSettleSeconds": 60, "emulatorBootPollSeconds": 5,
                    "foregroundMaxTries": 5, "foregroundRetrySeconds": 5},
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
        path = self.resolve_path(self.get("paths.workDir", "work"))
        path.mkdir(parents=True, exist_ok=True)
        return path

    def resolve_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        return (path if path.is_absolute() else self.root / path).resolve()

    @property
    def log_dir(self) -> Path:
        return self.resolve_path(self.get("paths.logDir", "logs"))

    @property
    def session_path(self) -> Path:
        value = self.get("api.sessionPath")
        return self.resolve_path(value) if value else self.work_dir / "api/session.json"

    @property
    def proxy_dir(self) -> Path:
        value = self.get("proxy.stateDir")
        return self.resolve_path(value) if value else self.work_dir / "proxy"

    @property
    def proxy_python(self) -> Path:
        value = self.get("proxy.pythonPath")
        return self.resolve_path(value) if value else self.proxy_dir / "venv/bin/python"

    @property
    def kafka_client_config(self) -> dict:
        result = dict(self.get("kafka.clientConfig", {}))
        for key in ("ssl.ca.location", "ssl.certificate.location", "ssl.key.location", "sasl.kerberos.keytab"):
            if result.get(key):
                result[key] = str(self.resolve_path(result[key]))
        return result


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
    validate_config(raw)
    return Settings(_merge(DEFAULTS, raw), config_path, project_root())


def validate_config(raw: dict, defaults: dict = DEFAULTS, prefix: str = "") -> None:
    """Catch misspelled/invalid settings without including secret values in errors."""
    for key, value in raw.items():
        name = f"{prefix}.{key}".lstrip(".")
        if key not in defaults:
            raise ConfigurationError(f"Unknown configuration field: {name}")
        expected = defaults[key]
        if name == "kafka.priceBookMaxAgeSeconds" and value is None:
            # Older configs used null for no age cutoff; the cache now treats
            # it as the five-minute default instead of disabling freshness.
            continue
        if isinstance(expected, dict):
            if not isinstance(value, dict):
                raise ConfigurationError(f"{name} must be an object.")
            if name != "kafka.clientConfig":
                validate_config(value, expected, name)
            elif any(k in value for k in ("group.id", "bootstrap.servers", "enable.auto.commit", "enable.auto.offset.store")):
                raise ConfigurationError("Configure Kafka routing through kafka fields; automatic commits are not supported.")
        elif isinstance(expected, bool):
            if not isinstance(value, bool):
                raise ConfigurationError(f"{name} must be boolean.")
        elif isinstance(expected, (int, float)):
            import math
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ConfigurationError(f"{name} must be a finite nonnegative number.")
            if (name.endswith(("Attempts", "MaxTries", "maxOrderPages", "maxLoginFailures")) or
                    key in {"holdingsIntervalSeconds", "pollTimeoutSeconds", "intervalSeconds", "checkIntervalSeconds",
                            "priceBookLookbackSeconds", "priceBookTimeoutSeconds", "priceBookBatchSize", "priceBookPollIntervalSeconds",
                            "marketStatusMaxAgeSeconds"}) and value <= 0:
                raise ConfigurationError(f"{name} must be positive.")
        elif isinstance(expected, str) and not isinstance(value, str):
            raise ConfigurationError(f"{name} must be a string.")
        elif isinstance(expected, list):
            if not isinstance(value, list):
                raise ConfigurationError(f"{name} must be an array.")
            if name == "api.cancelPollDelaysSeconds" and (not value or any(type(x) not in {int, float} or x < 0 for x in value)):
                raise ConfigurationError(f"{name} must contain nonnegative delays.")
            if name in {"api.allowedSymbols", "monitor.emulatorArgs"} and any(not isinstance(x, str) for x in value):
                raise ConfigurationError(f"{name} must contain strings.")
        elif expected is None and value is not None:
            import math
            if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                raise ConfigurationError(f"{name} must be null or a nonnegative number.")
        if name in {"api.readAttempts", "api.maxOrderPages", "api.maxOrders", "api.maxResponseBytes",
                    "monitor.maxLoginFailures", "monitor.restart.foregroundMaxTries", "service.restartSeconds",
                    "monitor.port", "monitor.redirectHttpPort", "proxy.apiPort",
                    "kafka.priceBookBatchSize"} and value is not None and type(value) is not int:
            raise ConfigurationError(f"{name} must be an integer.")
        if name == "kafka.priceBookMaxAgeSeconds" and value is not None and value <= 0:
            raise ConfigurationError(f"{name} must be null or positive.")
    if not prefix:
        if raw.get("logging", {}).get("level", "INFO") not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError("logging.level must be DEBUG, INFO, WARNING, ERROR or CRITICAL.")
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        restart = raw.get("monitor", {}).get("restart", {})
        try:
            ZoneInfo(restart.get("timezone", "America/New_York"))
            for key in ("start", "end"):
                hour, minute = restart.get(key, DEFAULTS["monitor"]["restart"][key]).split(":")
                if not (0 <= int(hour) <= 23 and 0 <= int(minute) <= 59):
                    raise ValueError
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            raise ConfigurationError("monitor.restart requires a valid timezone and HH:MM times.") from None


def find_adb(configured: str = "", *, root: Path | None = None) -> str:
    candidates: list[Path] = []
    if configured:
        configured_path = Path(configured).expanduser()
        candidates.append(root / configured_path if root is not None and not configured_path.is_absolute() else configured_path)
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


def execution_backend(settings: Settings) -> str:
    backend = str(settings.get("gigamoney.backend", "android")).strip().lower()
    if backend not in {"android", "api"}:
        raise ConfigurationError("gigamoney.backend must be android or api.")
    return backend
