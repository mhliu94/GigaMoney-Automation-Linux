# GigaMoney Automation for Linux

This is a separate Linux counterpart to `/home/ubuntu/GigaMoney-Automation`. The Windows PowerShell repository is not modified. The Linux port uses Python, ADB, UIAutomator XML, Tesseract OCR, Pillow, `fcntl` locking, and the native Linux `confluent-kafka` client.

The recorded controls are valid only for an emulator at **1080x2424, density 420**. Every holdings, order, and cancel workflow checks the selected device, package, resolution, and density before touching the UI. A process lock prevents the CLI and Kafka runner from controlling the emulator concurrently.

## Setup

```bash
cd /home/ubuntu/GigaMoney-Automation-Linux
sudo apt-get update
sudo apt-get install -y python3-venv tesseract-ocr
python3 -m venv .venv
.venv/bin/pip install -e '.[kafka]'
android/build-zero-idle-dumper.sh
cp config/gigamoney.config.example.json config/gigamoney.config.json
```

Edit `config/gigamoney.config.json`. Set the ADB serial and
`monitor.adminPassword` explicitly. Set `tradePassword` only if the app prompts
for it, and lock down the file:

```bash
chmod 600 config/gigamoney.config.json
```

The live config is ignored by Git. Passwords are never printed by the program.

Position rows are read from UIAutomator resource IDs, not OCR. The small
`gigamoney-zero-idle-dump.jar` helper sets the UIAutomator idle timeout to zero
and dumps the hierarchy directly, so rapidly changing market values cannot keep
the hierarchy reader waiting for a quiet screen. OCR remains in use for visual
navigation where the app does not expose a reliable resource identifier.

## Commands

All examples assume the default config path. Add `--config /path/to/file.json` before the subcommand to select another file.

```bash
# Read-only environment and OCR checks
.venv/bin/gigamoney device-check
.venv/bin/gigamoney snapshot-check

# Query holdings (prints JSON)
.venv/bin/gigamoney holdings

# Navigate to a ticket without entering values
.venv/bin/gigamoney market-order --symbol BILI --quantity 2 --side Buy --dry-run
.venv/bin/gigamoney limit-order --symbol BILI --price 17.38 --quantity 2 --side Buy --dry-run

# Enter values but do not submit
.venv/bin/gigamoney limit-order --symbol BILI --price 17.38 --quantity 2 --side Buy --no-submit

# LIVE: submit orders
.venv/bin/gigamoney market-order --symbol BILI --quantity 2 --side Buy
.venv/bin/gigamoney limit-order --symbol BILI --price 17.38 --quantity 2 --side Sell
.venv/bin/gigamoney limit-order --symbol BILI --price 17.38 --quantity 2 --side Buy --kill

# Inspect/cancel open orders
.venv/bin/gigamoney cancel-all --dry-run
.venv/bin/gigamoney cancel-all
```

`--dry-run` enters no values and never submits. `--no-submit` enters ticket values but never presses Submit. A live unconfirmed order follows the Windows behavior and retries the entire flow once after restarting the app; check the broker manually if an invocation exits without a confirmed result.

The fill-or-kill option is client-side emulation: it submits a limit order and then cancels any visible unfilled remainder. It is not exchange-atomic FOK.

## Kafka runner

The runner accepts `MARKET_ORDER`, `LIMIT_ORDER`, `LIMIT_ORDER_FOK`, `CANCEL_OPEN_ORDERS`, and `SET_TRADING_ENABLED`. It filters `account_id`, rejects stale or invalid timestamps, commits offsets manually, publishes holdings periodically, and preserves the original fail-safe default of committing failed commands to prevent replay.

Run it interactively first:

```bash
.venv/bin/gigamoney runner
```

Then install the user service:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/gigamoney-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gigamoney-runner.service
journalctl --user -u gigamoney-runner.service -f
```

Runner helpers:

```bash
.venv/bin/gigamoney runner-status
.venv/bin/gigamoney runner-stop
```

## Local command center

The Linux command center provides the same operational controls as the Zhuorui
monitor: start/stop the GigaMoney runner, start/stop the configured Android
emulator, monitor five machine/emulator health signals, and display average,
fastest, and slowest holdings-query times for the latest ten successful queries
in the current runner session.

Start it in the background and open it in Chrome:

```bash
cd /home/ubuntu/GigaMoney-Automation-Linux
.venv/bin/gigamoney monitor-start --open-browser
```

For a remotely accessible installation, configure `monitor.host`,
`monitor.port`, `monitor.redirectHttpPort`, and `monitor.publicHost`. Using HTTPS
on port 443 with a redirect from port 80 lets operators browse directly to the
configured public host. Sign-in is required before the dashboard or status API
can be viewed. A self-signed TLS certificate causes a browser warning until the
certificate is explicitly trusted or replaced with one issued by a public CA.

The process continues to run as `ubuntu`; `authbind` grants that account access
only to privileged ports 80 and 443. Certificate and key paths, bind address,
HTTPS port, redirect port, public host, and AVD are configured under `monitor`
in the ignored live config. Useful
management commands are:

```bash
.venv/bin/gigamoney monitor-status
.venv/bin/gigamoney monitor-stop

# Foreground mode, if preferred
.venv/bin/gigamoney monitor
```

The dashboard uses an eight-hour secure session cookie, CSRF tokens for control
actions, and a 15-minute per-client lockout after five failed logins. Its runner
start button uses the same virtual environment and live
config as the monitor. Its emulator start button auto-detects the only installed
AVD; set `monitor.avd` when more than one AVD exists. Start/stop requests are
serialized, and only same-origin JSON requests with the command-center action
header are accepted.

While the command center is running, it checks every 30 seconds for a scheduled
restart window from 8:01 PM through 9:00 PM America/New_York time. If the last
automatic or Web UI emulator-start attempt was more than one hour ago, it asks
the trading runner to stop after its current operation, verifies that it has
exited, stops the emulator, waits one minute, and cold-boots the emulator. It
polls Android readiness every five seconds for up to the configured
`monitor.emulatorBootTimeoutSeconds` (five minutes by default), then makes up to
five attempts to foreground GigaMoney before restarting the trading runner. The
emulator lock excludes other CLI automation throughout the emulator restart. A
boot timeout, exited emulator process, or failed foreground check stops the
emulator again, and every restart attempt is persisted even when it fails.

Updated runners emit exact holdings timings. For a runner that was already active
when timing instrumentation was installed, the dashboard immediately derives
backward-compatible estimates from the known publication schedule and labels
them as estimated. Existing trading and emulator processes do not need to be
restarted for the fallback statistics.

## Tests

```bash
.venv/bin/python -m unittest discover -v
.venv/bin/gigamoney self-test
python3 -m compileall -q gigamoney tests
```

Tests do not place or cancel orders. `device-check` and `snapshot-check` are read-only. Holdings navigates the app but does not trade.

## Safety boundaries

- Always keep `adb.serial` set; the code otherwise requires exactly one ready device.
- A resolution or density mismatch fails closed before all operational workflows.
- Ambiguous dark overlays fail closed instead of guessing whether Back is safe.
- Holdings refuses to return a partial portfolio if it cannot prove it reached the bottom.
- The Kafka runner validates its recorded PID before sending a stop signal.
- Passwords are stored locally in JSON; use file mode `600` and do not commit it.
