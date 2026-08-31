import tempfile
import unittest
from pathlib import Path

from gigamoney.errors import SafetyError
from gigamoney.lock import EmulatorLock


class LockTests(unittest.TestCase):
    def test_second_controller_times_out(self):
        path = Path(tempfile.mkdtemp()) / "emulator.lock"
        with EmulatorLock(path, 0.1):
            with self.assertRaises(SafetyError):
                with EmulatorLock(path, 0.05):
                    pass


if __name__ == "__main__":
    unittest.main()
