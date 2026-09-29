# Documentation and source map

## Find a guide

| Task | Read |
| --- | --- |
| Set up another Linux machine or account, enroll a session, start/stop services | [Deployment](deployment.md) |
| Look up a configurable field or default | [Configuration reference](configuration.md) |
| Understand API order flows, signing, renewal, retries, and duplicate protection | [API backend](api.md) |
| Connect KTrader Order UI and inspect durable order publications | [Order-status reporting](order-status.md) |
| Prepare the emulator for enrollment or record API/Android traffic | [Capture setup and operation](proxy.md) |
| Review the observations behind the protocol implementation | [Investigation notes](investigation.md) |
| Run daily commands, use the command center, or interpret logs | [Project README](../README.md) |
| Identify private configuration files | [Configuration directory](../config/README.md) |
| Choose a diagnostic/capture script | [Script directory](../scripts/README.md) |
| Generate a service for this installation | [Systemd directory](../systemd/README.md) |

Investigation notes describe dated observations. Use the deployment and backend
guides for current behavior.

## Find the implementation

All Python application modules live in [`gigamoney/`](../gigamoney/). Both backends
enter through the same CLI and Kafka runner; capture tools live separately in
[`scripts/`](../scripts/).

| Area | Files |
| --- | --- |
| CLI, settings, local setup | `cli.py`, `config.py`, `setup.py` |
| Kafka intake, holdings publication, runner state | `runner.py` |
| API authentication, HTTP transport, request signing | `api.py`, `signing.py` |
| API order workflows, reconciliation, persistent journal | `api_trading.py` |
| KTrader order snapshots, change detection, durable publication sequences | `order_status.py` |
| Background Kafka book cache and extended-hours limit price selection | `price_books.py` |
| Verified emulator session import | `bootstrap.py` |
| Android workflows | `automation.py`, `orders.py`, `cancel_orders.py`, `holdings.py` |
| Android device access, hierarchy, OCR | `adb.py`, `ui.py`, `vision.py` |
| Shared logging, locking, errors | `diagnostics.py`, `lock.py`, `errors.py` |
| Command-center server and web assets | `monitor.py`, `monitor_web/` |

The Android hierarchy helper's Java source and build wrapper live in
[`android/`](../android/). Its generated JAR is local. Regression tests in
[`tests/`](../tests/) use fake devices, Kafka clients, and brokers; test commands
are in the [project README](../README.md#tests).

## Local files and persistence

`config/*.example.json` and source/documentation are shareable. Private account
configuration and app profiles stay in `config/`; API credentials, journals,
capture artifacts, and process state go in `work/`; operational logs go in
`logs/`. These are default locations and can be configured.

`work/api/orders/` is the persistent duplicate-protection journal. Keep it with
the same account across restarts and migrations. Also retain `work/api/order-status/`,
which records pending and confirmed Kafka snapshots and their sequence numbers.
`work/api/session.json` belongs
to its verified account/device. Raw captures beneath `work/proxy/` also contain
credentials and account data. These local files are ignored by Git and must not
be used to enroll a different account.
