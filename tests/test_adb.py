import unittest

from gigamoney.adb import Adb


class AdbTests(unittest.TestCase):
    def test_foreground_uses_only_focus_and_resumed_lines(self):
        adb = Adb.__new__(Adb)
        adb.package = "lb.whale.hkwinner.android"

        def background(command, *args, **kwargs):
            if command == "dumpsys" and args[0] == "window":
                return "Window #2 lb.whale.hkwinner.android\nmCurrentFocus=com.android.settings"
            return "mResumedActivity=com.android.settings"

        adb.shell = background
        self.assertFalse(adb.is_foreground())

        def foreground(command, *args, **kwargs):
            if command == "dumpsys" and args[0] == "window":
                return "mCurrentFocus=lb.whale.hkwinner.android/.MainActivity"
            return ""

        adb.shell = foreground
        self.assertTrue(adb.is_foreground())


if __name__ == "__main__":
    unittest.main()
