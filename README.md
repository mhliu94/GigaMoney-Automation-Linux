# GigaMoney Automation for Linux

This project supports direct API trading and the original Android workflows through the same Python CLI, Kafka runner, and command center.

For the Android backend, the recorded controls require an emulator at **1080x2424, density 420**. Each UI workflow validates the device and uses a process lock. The API backend uses Android for initial account enrollment, then runs independently.

For HTTPS/WebSocket captures and API investigation, see [the proxy setup and operation guide](docs/proxy.md).

For emulator-free trading with fresh signing, session renewal, and the existing
Kafka workflows, see [the direct API backend guide](docs/api.md). Set
`gigamoney.backend` to `api` in the private configuration after installing
`.[api,kafka]` and importing the verified session. For a new Linux machine or another account, start with [the deployment guide](docs/deployment.md). The setup below also supports Android.

## Repository layout

```text
GigaMoney-Automation-Linux/
├── README.md           # Start here: setup, commands, and operations
├── pyproject.toml      # Python dependencies and the gigamoney CLI entry point
├── gigamoney/          # Shared runner, API and Android backends, command center
│   └── monitor_web/   # Command-center HTML, JavaScript, and CSS
├── config/             # Tracked examples; private machine/account JSON lives here
├── docs/               # Deployment, configuration, backend, and capture guides
├── scripts/            # Capture managers, diagnostics, and their support files
├── android/            # Java hierarchy helper source and build wrapper
├── systemd/            # Service-generation instructions
└── tests/              # Offline regression tests with fake brokers and devices
```

Local setup also creates `.venv/` (Python packages), `logs/` (operational logs),
`work/` (persistent account state, API sessions, order journals, captures, and
generated services), and optionally `certs/` (dashboard TLS files). These local
artifacts are excluded from Git. **`work/` contains persistent state, including
duplicate-order protection; preserve it across runner restarts.** Paths can be
changed in the configuration.

Use the [documentation index and source map](docs/README.md) to find a guide or
implementation, the [configuration directory guide](config/README.md) to identify
private versus shareable files, and the [script directory guide](scripts/README.md)
to distinguish runnable tools from support files.

## Setup

```bash
# Run from your checkout directory.
sudo apt-get update
sudo apt-get install -y python3-venv tesseract-ocr default-jdk junit4
python3 -m venv .venv
.venv/bin/pip install -e '.[api,kafka]'
.venv/bin/gigamoney init-config
# Edit the config, including Android SDK/JDK settings when needed.
.venv/bin/gigamoney build-android
```

Edit `config/gigamoney.config.json`. Set the ADB serial and
`monitor.adminPassword` explicitly when using the command center. Set
`gigamoney.tradePassword` for API session import, or for Android password prompts,
and lock down the file:

```bash
chmod 600 config/gigamoney.config.json
```

The live config is ignored by Git. Passwords are never printed by the program.

In Android mode, position rows are read from UIAutomator resource IDs. The small
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

In API mode, both `--dry-run` and `--no-submit` read account/ticket data and return
an unsubmitted preview. See [API order flow and retries](docs/api.md#order-flow-retries-and-timing).

The following navigation and retry behavior applies to the Android backend.
`--dry-run` enters no values and never submits. `--no-submit` enters ticket values but never presses Submit. A live unconfirmed order follows the Windows behavior and retries the entire flow once after restarting the app; check the broker manually if an invocation exits without a confirmed result.

Market, limit, and fill-or-kill order flows skip advertisement detection on their
first attempt. On the second attempt, ad detection and dismissal run at home,
before submission, and during FOK cancellation. A UI error while navigating to
the order ticket also restarts the app and retries once before submitting. Trade
password handling remains active on both attempts. Dry runs and staged orders
make only one attempt.

Retry coverage differs by action: holdings queries retry once after a failure;
cancel-all has no retry of the whole action. Their advertisement checks remain
active. A cancellation failure after a confirmed FOK submission does not restart
and resubmit that order.

The fill-or-kill option is client-side emulation: it submits a limit order and then cancels any visible unfilled remainder. It is not exchange-atomic FOK.

## Kafka runner

The runner accepts `MARKET_ORDER`, `LIMIT_ORDER`, `LIMIT_ORDER_FOK`, `CANCEL_OPEN_ORDERS`, and `SET_TRADING_ENABLED`. It filters `account_id`, rejects stale or invalid timestamps, commits offsets manually, publishes holdings periodically, and preserves the original fail-safe default of committing failed commands to prevent replay.

After each confirmed market, limit, or fill-or-kill order, the runner commits the
command and immediately queries and publishes fresh holdings to the account-details
topic before polling for another command. Every update, including periodic and
post-order updates, resets the next periodic query to 30 seconds (configurable via
`kafka.holdingsIntervalSeconds`) after Kafka delivery completes. A holdings-query or
Kafka-publication failure is logged without replaying the completed order; the next
attempt waits the same interval after the failure. A verification challenge is different: the API runner records `authentication_required` and stops intake. A command blocked before submission remains uncommitted. Complete verification/import and manually restart; the normal stale-command guard remains active.

Order prices with more than two decimal places round to cents: up for buys, down
for sells (for example, 2.3625 becomes 2.37 Buy or 2.36 Sell). This applies to
limit/FOK prices on both backends and API market trigger prices. Prices with at
most two decimal places retain their value; a sell price rounding to zero is rejected.

Run it interactively first:

```bash
.venv/bin/gigamoney runner
```

Then install the user service:

```bash
.venv/bin/gigamoney render-service
mkdir -p ~/.config/systemd/user
cp work/systemd/gigamoney-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gigamoney-runner.service
journalctl --user -u gigamoney-runner.service -f
```

Runner helpers:

```bash
.venv/bin/gigamoney runner-status
.venv/bin/gigamoney runner-stop
```

## Workflow logging

The runner writes timestamped logs to `logs/runner.log` when started through the
command center. Interactive CLI commands write the same workflow records to stderr.
Market, limit, FOK, cancel-all, and holdings workflows emit a start, an attempt
record, and exactly one `Workflow finished` JSON summary. Retries emit a warning;
only terminal failures emit an error summary. Routine navigation is at DEBUG level
(`--verbose`); failures include the last stage and error, with a traceback at DEBUG.
Configured trade and dashboard passwords are redacted from workflow error records.

The final summary contains the workflow name, command ID when available, mode,
attempt count, total `elapsed_s`, and the last attempt's `attempt_elapsed_s`.
Order summaries include symbol, side, quantity, confirmation status and reported
fills. A FOK cancellation failure records that submission was already confirmed.
Cancel-all records cancellation requests and the last detected remaining count;
the request count is not proof that each individual cancellation was confirmed.

To calculate action success rates, group `Workflow finished` records by `workflow`
with `mode=live` and count `outcome=success` versus `outcome=failure`. An attempt
count of two identifies actions that needed a retry. `dry_run` and `staged`
outcomes are separate. Success means the automation completed; it does not mean
an order was fully filled. `Command finished` records separately identify ignored
commands, failures, and offset commits, with elapsed time including lock waits
and the commit. Do not count both record types as separate actions.

Position updates identify `startup`, `periodic`, or `post_order` as their trigger.
Post-order updates carry the order's command ID. Successful publications report
query-plus-delivery time, delivery time alone, and `order_to_update_s` from command
processing start through publication. Failures distinguish query, snapshot, and
Kafka delivery stages. A publication failure does not change a completed order's
success outcome. Existing holdings timing records remain compatible with the dashboard.
Command-center controls and scheduled restarts also log outcomes and elapsed time.

```bash
rg 'Workflow finished|Workflow retry|Command finished|Published holdings|Holdings publication failed' logs/runner.log
```

## Local command center

The Linux command center provides the same operational controls as the Zhuorui
monitor: start/stop the GigaMoney runner, start/stop the configured Android
emulator, monitor five machine/emulator health signals, and display average,
fastest, and slowest holdings-query times for the latest ten successful queries
in the current runner session.

Start it in the background and open it in Chrome:

```bash
# Run from your checkout directory.
.venv/bin/gigamoney monitor-start --open-browser
```

For a remotely accessible installation, configure `monitor.host`,
`monitor.port`, `monitor.redirectHttpPort`, and `monitor.publicHost`. Using HTTPS
on port 443 with a redirect from port 80 lets operators browse directly to the
configured public host. Sign-in is required before the dashboard or status API
can be viewed. A self-signed TLS certificate causes a browser warning until the
certificate is explicitly trusted or replaced with one issued by a public CA.

The process runs as the deployment user. For privileged ports, configure `authbind` access for that user; unprivileged HTTPS ports need no such setup. Certificate and key paths, bind address,
HTTPS port, redirect port, public host, and AVD are configured under `monitor`
in the ignored live config. Useful
management commands are:

```bash
.venv/bin/gigamoney monitor-status
.venv/bin/gigamoney monitor-stop

# Foreground mode, if preferred
.venv/bin/gigamoney monitor
```

By default, the dashboard uses an eight-hour secure session cookie, CSRF tokens for control
actions, and a 15-minute per-client lockout after five failed logins. Username, session lifetime, and lockout settings are configurable under `monitor`. Its runner
start button uses the same virtual environment and live
config as the monitor. Its emulator start button auto-detects the only installed
AVD; set `monitor.avd` when more than one AVD exists. Start/stop requests are
serialized, and only same-origin JSON requests with the command-center action
header are accepted.

In API mode, the dashboard shows Linux health and scheduled restarts restart only
the runner. No emulator start, shutdown delay, or app launch is required.

The configurable `monitor.restart` settings default to checking every 30 seconds for a scheduled
restart window from 8:01 PM through 9:00 PM America/New_York time. If the last
automatic or Web UI emulator-start attempt was more than one hour ago, it asks
the trading runner to stop after its current operation, verifies that it has
exited. In Android mode it stops the emulator, waits one minute, and cold-boots the emulator. It
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
.venv/bin/python -m compileall -q gigamoney scripts tests
```

Tests do not place or cancel live orders. `device-check` and `snapshot-check` are read-only. Holdings reads the API or navigates the app, according to the selected backend.

## Safety boundaries

- For Android, keep `adb.serial` set; the code otherwise requires exactly one ready device.
- A resolution or density mismatch fails closed before all operational workflows.
- Ambiguous dark overlays fail closed instead of guessing whether Back is safe.
- Holdings refuses to return a partial portfolio if it cannot prove it reached the bottom.
- The Kafka runner validates its recorded PID before sending a stop signal.
- Passwords are stored locally in JSON; use file mode `600` and do not commit it.
