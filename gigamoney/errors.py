class GigaMoneyError(RuntimeError):
    """Base error for expected automation failures."""


class ConfigurationError(GigaMoneyError):
    """Configuration is missing or invalid."""


class DeviceError(GigaMoneyError):
    """ADB or emulator validation failed."""


class UiError(GigaMoneyError):
    """The expected application UI could not be reached."""


class SafetyError(GigaMoneyError):
    """A fail-closed safety check prevented an action."""
