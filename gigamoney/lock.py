from __future__ import annotations

import fcntl
import os
import time
from pathlib import Path

from .errors import SafetyError


class EmulatorLock:
    """Advisory process lock preventing concurrent emulator interaction."""

    def __init__(self, path: Path, timeout: float = 300):
        self.path = path
        self.timeout = timeout
        self._file = None

    def __enter__(self) -> "EmulatorLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+", encoding="utf-8")
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self._file.close()
                    self._file = None
                    raise SafetyError(f"Timed out waiting for emulator lock: {self.path}")
                time.sleep(0.2)
        self._file.seek(0)
        self._file.truncate()
        self._file.write(f"pid={os.getpid()} acquired={time.time():.3f}\n")
        self._file.flush()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._file is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None
