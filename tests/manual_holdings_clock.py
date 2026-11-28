"""Drive the independent reporting clock explicitly in offline consumer tests."""
import math
import time


class ManualTimer:
    def __init__(self, read, interval=30, on_error=None):
        self.read = read
        self.interval = interval
        self.on_error = on_error
        self.due = 0

    def start(self, *, wait_initial=False):
        self.due = time.monotonic()
        self.advance()

    def advance(self):
        now = time.monotonic()
        if now >= self.due:
            try:
                self.read()
            except Exception as error:
                if self.on_error:
                    self.on_error(error)
            self.due += self.interval * max(1, math.floor((time.monotonic()-self.due)/self.interval)+1)

    def close(self):
        pass
