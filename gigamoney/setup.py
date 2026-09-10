"""Portable local setup helpers. These commands never start trading."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from .config import DEFAULTS, default_config_path, execution_backend, load_settings
from .errors import ConfigurationError


def init_config(path=None):
    target = Path(path).expanduser().resolve() if path else default_config_path().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as handle:
        json.dump(DEFAULTS, handle, indent=2)
        handle.write("\n")
    return {"ok": True, "config": str(target)}


def validate(settings):
    execution_backend(settings)
    required = ["gigamoney.accountId", "kafka.bootstrapServers", "kafka.consumerGroupId"]
    if execution_backend(settings) == "api":
        required += ["api.accountAaid", "api.orgId", "api.accountChannel"]
    missing = [key for key in required if not settings.get(key)]
    result = {"ok": not missing, "missingRequired": missing, "config": str(settings.path),
              "backend": execution_backend(settings), "workDir": str(settings.work_dir),
              "logDir": str(settings.log_dir), "sessionPath": str(settings.session_path),
              "proxyDir": str(settings.proxy_dir)}
    if execution_backend(settings) == "api" and settings.session_path.exists():
        from .api import ApiClient, SessionError
        try:
            with ApiClient.from_settings(settings)._session():
                result["sessionMatchesAccount"] = True
        except SessionError:
            result.update(ok=False, sessionMatchesAccount=False)
    elif execution_backend(settings) == "api":
        result.update(ok=False, sessionMatchesAccount=False, sessionMissing=True)
    return result


def unit_quote(value, *, expand_dollars=True):
    # systemd performs specifier/environment expansion independently of a shell.
    text = str(value)
    if any(c in text for c in "\r\n\0"):
        raise ConfigurationError("Service paths may not contain control characters.")
    escaped = text.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%')
    return '"' + (escaped.replace('$', '$$') if expand_dollars else escaped) + '"'


def unit_path(value):
    text = str(value)
    if not text.startswith("/") or any(c in text for c in "\r\n\0"):
        raise ConfigurationError("Service paths must be absolute and contain no control characters.")
    return text.replace("%", "%%")


def render_service(settings):
    name = settings.get("service.name", "gigamoney-runner")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ConfigurationError("service.name must contain letters, digits, dots, underscores or hyphens.")
    executable = Path(sys.executable).absolute()
    log = settings.resolve_path(settings.get("monitor.runnerLog")) if settings.get("monitor.runnerLog") else settings.log_dir / "runner.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    text = f"""[Unit]
Description=GigaMoney Kafka runner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={unit_path(settings.root)}
ExecStart={unit_quote(executable)} -m gigamoney.cli --config {unit_quote(settings.path)} runner
Restart=on-failure
RestartSec={settings.get('service.restartSeconds', 10)}
Environment=PYTHONUNBUFFERED=1
Environment={unit_quote('GIGAMONEY_RUN_LOG=' + str(log), expand_dollars=False)}
StandardOutput=append:{unit_path(log)}
StandardError=append:{unit_path(log)}
UMask=0077

[Install]
WantedBy=default.target
"""
    output = settings.work_dir / "systemd" / (name + ".service")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text)
    return {"ok": True, "unit": str(output), "installed": False, "started": False}


def _latest(directory):
    items = [item for item in directory.iterdir() if item.is_dir()] if directory.is_dir() else []
    if not items:
        raise ConfigurationError(f"No installed Android SDK components under {directory}.")
    return max(items, key=lambda item: tuple(int(part) for part in re.findall(r"\d+", item.name)))


def build_android(settings, *, run=subprocess.run):
    sdk = (settings.resolve_path(settings.get("androidBuild.sdkRoot")) if settings.get("androidBuild.sdkRoot") else
           Path(os.environ.get("ANDROID_SDK_ROOT") or os.environ.get("ANDROID_HOME") or Path.home() / "Android/Sdk").expanduser())
    platform = (sdk / "platforms" / settings.get("androidBuild.platform") if settings.get("androidBuild.platform")
                else _latest(sdk / "platforms"))
    build = (sdk / "build-tools" / settings.get("androidBuild.buildToolsVersion") if settings.get("androidBuild.buildToolsVersion")
             else _latest(sdk / "build-tools"))
    java_home = settings.resolve_path(settings.get("androidBuild.javaHome")) if settings.get("androidBuild.javaHome") else os.environ.get("JAVA_HOME")
    if not java_home:
        javac = shutil.which("javac")
        java_home = Path(javac).resolve().parent.parent if javac else Path.home() / ".local/opt/android-studio/jbr"
    java_home = Path(java_home).expanduser()
    junit_candidates = [settings.resolve_path(settings.get("androidBuild.junitJar")) if settings.get("androidBuild.junitJar") else Path(os.environ.get("JUNIT_JAR") or "/usr/share/java/junit4.jar"),
                        Path.home() / ".local/opt/android-studio/lib/junit4.jar"]
    junit = next((path.expanduser() for path in junit_candidates if path.expanduser().is_file()), junit_candidates[0])
    libs = [platform / "android.jar", platform / "uiautomator.jar", platform / "optional/android.test.runner.jar", junit]
    programs = [java_home / "bin/javac", java_home / "bin/jar", build / "d8"]
    for path in [*libs, *programs]:
        if not path.is_file():
            raise ConfigurationError(f"Missing build prerequisite: {path}. Set androidBuild fields for the installed SDK/JDK.")
    output = settings.resolve_path(settings.get("androidBuild.outputJar", "android/gigamoney-zero-idle-dump.jar"))
    output.parent.mkdir(parents=True, exist_ok=True)
    environment = {**os.environ, "JAVA_HOME": str(java_home), "PATH": str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")}
    with tempfile.TemporaryDirectory() as directory:
        temp = Path(directory)
        (temp / "classes").mkdir()
        (temp / "dex").mkdir()
        commands = [
            [programs[0], "--release", "8", "-classpath", os.pathsep.join(str(x) for x in libs), "-d", temp / "classes",
             settings.root / "android/src/gigamoney/ZeroIdleDump.java", settings.root / "android/src/android/test/RepetitiveTest.java"],
            [programs[1], "--create", "--file", temp / "classes.jar", "-C", temp / "classes", "."],
            [programs[2], *[arg for lib in libs for arg in ("--lib", lib)], "--output", temp / "dex", temp / "classes.jar"],
            [programs[1], "--create", "--file", output, "-C", temp / "dex", "classes.dex"],
        ]
        for command in commands:
            run([str(part) for part in command], env=environment, check=True)
    return {"ok": True, "jar": str(output)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("action", choices=("init-config", "validate-config", "render-service", "build-android"))
    args = parser.parse_args(argv)
    try:
        if args.action == "init-config":
            result = init_config(args.config)
        else:
            settings = load_settings(args.config)
            result = {"validate-config": validate, "render-service": render_service, "build-android": build_android}[args.action](settings)
        print(json.dumps(result, indent=2))
        return 0 if result["ok"] else 1
    except (OSError, ConfigurationError) as error:
        print(json.dumps({"ok": False, "message": str(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
