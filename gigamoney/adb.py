from __future__ import annotations

import re
import subprocess
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .config import Settings, find_adb
from .errors import DeviceError, UiError
from .ui import parse_xml


@dataclass(frozen=True)
class DeviceInfo:
    serial: str
    resolution: str
    density: int
    package_installed: bool
    foreground: bool


class Adb:
    _zero_idle_ready_devices: set[tuple[str, str]] = set()

    def __init__(self, settings: Settings):
        self.settings = settings
        self.executable = find_adb(str(settings.get("adb.path", "")), root=settings.root)
        self.timeout = float(settings.get("adb.commandTimeoutSeconds", 30))
        configured = str(settings.get("adb.serial", "")).strip()
        self.serial = configured or self._select_single_device()
        self.package = str(settings.get("gigamoney.package"))

    def _base(self) -> list[str]:
        return [self.executable, "-s", self.serial]

    def _run(
        self,
        arguments: Sequence[str],
        *,
        binary: bool = False,
        check: bool = True,
        timeout: float | None = None,
    ) -> str | bytes:
        try:
            result = subprocess.run(
                [*self._base(), *arguments],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=timeout or self.timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DeviceError(f"ADB command failed: {exc}") from exc
        if check and result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise DeviceError(f"ADB {' '.join(arguments)} failed ({result.returncode}): {detail}")
        return result.stdout if binary else result.stdout.decode("utf-8", errors="replace").strip()

    def _select_single_device(self) -> str:
        try:
            result = subprocess.run(
                [self.executable, "devices"], capture_output=True, text=True, timeout=self.timeout
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DeviceError(f"Cannot list ADB devices: {exc}") from exc
        attached = []
        for line in result.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 2 and parts[1] == "device":
                attached.append(parts[0])
        if len(attached) != 1:
            detail = ", ".join(attached) if attached else "none"
            raise DeviceError(
                f"Set adb.serial because exactly one ready device is required (found: {detail})."
            )
        return attached[0]

    def shell(self, *arguments: str, check: bool = True) -> str:
        return str(self._run(["shell", *arguments], check=check))

    def validate(self, *, strict_layout: bool = True) -> DeviceInfo:
        state = str(self._run(["get-state"]))
        if state != "device":
            raise DeviceError(f"ADB device {self.serial!r} is not ready: {state}")
        size_output = self.shell("wm", "size")
        size_match = re.findall(r"(?:Physical|Override) size:\s*(\d+x\d+)", size_output)
        resolution = size_match[-1] if size_match else ""
        density_output = self.shell("wm", "density")
        density_match = re.findall(r"(?:Physical|Override) density:\s*(\d+)", density_output)
        density = int(density_match[-1]) if density_match else 0
        expected_resolution = str(self.settings.get("adb.expectedResolution", "")).strip()
        expected_density = int(self.settings.get("adb.expectedDensity", 0) or 0)
        if strict_layout and expected_resolution and resolution != expected_resolution:
            raise DeviceError(
                f"Unsafe emulator resolution {resolution or 'unknown'}; expected {expected_resolution}."
            )
        if strict_layout and expected_density and density != expected_density:
            raise DeviceError(f"Unsafe emulator density {density or 'unknown'}; expected {expected_density}.")
        packages = self.shell("pm", "list", "packages", self.package)
        installed = f"package:{self.package}" in packages
        if not installed:
            raise DeviceError(f"GigaMoney package is not installed: {self.package}")
        return DeviceInfo(self.serial, resolution, density, installed, self.is_foreground())

    def screenshot(self) -> bytes:
        return bytes(self._run(["exec-out", "screencap", "-p"], binary=True, timeout=45))

    def _ensure_zero_idle_dumper(self) -> str:
        device_key = (self.executable, self.serial)
        remote_path = "/data/local/tmp/gigamoney-zero-idle-dump.jar"
        if device_key in self._zero_idle_ready_devices:
            return remote_path
        local_path = self.settings.resolve_path(self.settings.get("androidBuild.outputJar", "android/gigamoney-zero-idle-dump.jar"))
        if not local_path.is_file():
            raise DeviceError(
                f"Zero-idle UIAutomator helper is missing: {local_path}. "
                "Run android/build-zero-idle-dumper.sh."
            )
        self._run(["push", str(local_path), remote_path], timeout=45)
        self.shell("mkdir", "-p", "/data/local/tmp/local/tmp")
        self._zero_idle_ready_devices.add(device_key)
        return remote_path

    def dump_xml(self, *, attempts: int = 2, wait_for_idle_timeout_ms: int | None = None):
        if wait_for_idle_timeout_ms not in {None, 0}:
            raise ValueError("Only the legacy default or a zero idle timeout is supported.")
        error: Exception | None = None
        for _ in range(attempts):
            try:
                if wait_for_idle_timeout_ms == 0:
                    helper = self._ensure_zero_idle_dumper()
                    output_name = "gigamoney-window-zero-idle.xml"
                    output = f"/data/local/tmp/local/tmp/{output_name}"
                    self.shell("rm", "-f", output)
                    self.shell(
                        "uiautomator", "runtest", helper,
                        "-c", "gigamoney.ZeroIdleDump",
                        "-e", "output", output_name,
                    )
                else:
                    output = "/sdcard/gigamoney-window.xml"
                    self.shell("uiautomator", "dump", "--compressed", output)
                raw = self._run(["exec-out", "cat", output], binary=True)
                return parse_xml(bytes(raw))
            except (DeviceError, ValueError, ET.ParseError) as exc:
                error = exc
                time.sleep(0.25)
        raise UiError(f"Could not obtain a valid UI hierarchy: {error}")

    def tap(self, x: int, y: int) -> None:
        self.shell("input", "tap", str(int(x)), str(int(y)))

    def swipe(self, x1: int, y1: int, x2: int, y2: int, duration_ms: int) -> None:
        self.shell("input", "swipe", str(x1), str(y1), str(x2), str(y2), str(duration_ms))

    def keyevent(self, key: str | int) -> None:
        self.shell("input", "keyevent", str(key))

    def input_text(self, value: str) -> None:
        escaped = value.replace("\\", "\\\\").replace(" ", "%s")
        for character in "&<>();":
            escaped = escaped.replace(character, f"\\{character}")
        self.shell("input", "text", escaped)

    def set_text_at(self, x: int, y: int, value: str) -> None:
        self.tap(x, y)
        time.sleep(0.08)
        self.keyevent("KEYCODE_MOVE_END")
        for _ in range(12):
            self.keyevent("KEYCODE_DEL")
        self.input_text(value)
        time.sleep(0.15)

    def is_foreground(self) -> bool:
        windows = self.shell("dumpsys", "window", "windows", check=False)
        activities = self.shell("dumpsys", "activity", "activities", check=False)
        focus_lines = [
            line for line in windows.splitlines()
            if "mCurrentFocus" in line or "mFocusedApp" in line
        ]
        resumed_lines = [
            line for line in activities.splitlines()
            if "mResumedActivity" in line or "topResumedActivity" in line
        ]
        return self.package in "\n".join([*focus_lines, *resumed_lines])

    def launch(self) -> None:
        self.shell(
            "monkey", "-p", self.package, "-c", "android.intent.category.LAUNCHER", "1"
        )

    def ensure_foreground(self) -> None:
        if self.is_foreground():
            return
        self.launch()
        deadline = time.monotonic() + float(
            self.settings.get("gigamoney.foregroundLaunchTimeoutSeconds", 15)
        )
        while time.monotonic() < deadline:
            if self.is_foreground():
                return
            time.sleep(0.5)
        raise UiError(f"{self.package} did not enter the foreground")

    def restart(self) -> None:
        self.shell("am", "force-stop", self.package)
        time.sleep(0.4)
        self.launch()
        time.sleep(1.2)
        self.ensure_foreground()
