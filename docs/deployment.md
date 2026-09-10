# Install on another Linux machine and account

Use one checkout/configuration and verified session per account. The direct API
runner works without an emulator after setup. Android is used for the account's
initial login, phone verification, and any later verification the broker requires.
The optional Android trading backend still requires the recorded 1080x2424,
density-420 layout. Session extraction requires the same reviewed GigaMoney app
build and the instrumentation setup described in [proxy.md](proxy.md).

## Files to configure

| File | Purpose |
|---|---|
| `config/gigamoney.config.json` | Private machine/account configuration: Kafka, account mapping, API limits/timing, emulator, paths, monitor, proxy and service settings. |
| `config/gigamoney.config.example.json` | Complete shareable defaults; contains every supported operator setting. |
| `config/api-app-profile.json` | Private app-build signing profile. Contains no account tokens, account routing, device ID, or trading password. |
| `config/api-app-profile.example.json` | Shape of the app profile; its empty secret is a placeholder, not a working profile. |
| `work/api/session.json` | Generated account-bound credentials and the verified emulator's device identity. Automatically updated during renewal. Do not hand-edit or copy between accounts. |
| `work/api/orders/` | Persistent submission/cancellation journal. Preserve it when restarting or moving the same account. |
| `work/api/authentication.json` | Generated status when human verification is required; shown by runner status and the command center. |

Paths above are defaults. `paths.workDir`, `paths.logDir`, `api.sessionPath` and
`proxy.stateDir` can select other locations. Empty session/proxy paths derive from
`paths.workDir`. Empty `monitor.runnerLog` derives from `paths.logDir`. Relative
paths resolve against the installed project root, regardless of shell working
directory; absolute paths are recommended for external state directories.

All entry points accept `--config /absolute/path/account.json`. You can instead
set `GIGAMONEY_CONFIG`; an explicit `--config` wins. Put the option before the
subcommand. Private JSON files in `config/` are ignored by Git; example files are
tracked. Protect private files with mode 600 and session directories with 700.

See [configuration reference](configuration.md) for every field and its default.
Protocol routes, signature encoding, the no-short-selling rule, and recorded UI
geometry are implementation contracts rather than operator settings. One-off
order parameters remain command arguments or Kafka message fields.

## 1. Install the code and dependencies

Clone the repository anywhere under your Linux user, then enter that checkout.
The main environment requires Python 3.10+; the pinned capture environment needs
Python 3.12+. Install the Android SDK/emulator and the same GigaMoney APK used for
the reviewed profile. Complete emulator/CA/instrumentation setup using
[the proxy guide](proxy.md); those private boot/CA artifacts are not in Git.

For a Debian/Ubuntu host:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv tesseract-ocr default-jdk junit4
python3 -m venv .venv
.venv/bin/pip install -e '.[api,kafka]'
.venv/bin/gigamoney init-config
```

`init-config` creates a private config and refuses to overwrite an existing one.
Configure `adb.path`/`adb.serial`, or let SDK/PATH discovery find ADB and set the
serial explicitly from `adb devices`. Configure `monitor.emulatorPath` and
`monitor.avd` if discovery cannot uniquely identify them. No username or SDK
installation directory is hardcoded in the example.

If using the Android trading backend, build its hierarchy helper:

```bash
.venv/bin/gigamoney build-android
```

`androidBuild` can select SDK root/platform/build-tools, Java home, JUnit jar and
output jar. Empty fields discover the installed tools. API trading and session
import do not need this helper.

For capture/bootstrap, using a Python 3.12+ interpreter:

```bash
python3 -m venv work/proxy/venv
work/proxy/venv/bin/pip install -r scripts/proxy-requirements.txt -e '.[api]'
```

If these paths differ, configure `proxy.pythonPath` and `proxy.stateDir`. The
proxy guide's boot builder also accepts `--config`; its image/APK/output paths
can be set under `proxy.boot`. Its currently reviewed image is Android API 36
x86_64 with the pinned Magisk build. Different app/Android builds need validation.

## 2. Set the account and Kafka configuration

Edit the generated JSON:

- Set `gigamoney.backend` to `api`.
- Set `gigamoney.accountId` to the exact logical `account_id` used by your control
  UI, and `gigamoney.accountNumId` if that integration uses it. These are distinct
  from the broker's opaque account routing identifiers.
- Set `kafka.bootstrapServers`, topics, and **a consumerGroupId unique to this
  account**, for example `gigamoney-account-b`. Different accounts must not share
  one group: Kafka would distribute their commands between consumers, and the
  account filter could consume/discard commands intended for another account.
- Set `api.allowedSymbols`, `api.maxOrderQuantity`, and `api.maxOrders` for this
  account. Null caps mean unlimited. The total order cap counts dispatches across
  the persistent journal, including rejected submissions; deleting the journal
  defeats both this cap and duplicate protection.
- Set `gigamoney.tradePassword` to this account's trading password. The importer
  saves its digest for automatic trade authentication. It is not the login/SMS
  verification code, and changing this config field alone does not update an
  already imported session; import the verified session again after a change.
- Optionally set `kafka.clientConfig` for SSL/SASL, e.g. `security.protocol`,
  `sasl.mechanism`, `sasl.username`, and `sasl.password`. Certificate/key paths are
  resolved against the project root. Routing and automatic-commit options cannot
  override the runner's explicit settings.

Keep the defaults of 30 seconds for holdings and directional cent rounding unless
the account's operational requirements change. Every update resets the holdings
clock from completion. Set `monitor.restart.enabled` to false to disable daily
restarts, or configure its timezone/window. API-mode restarts do not boot Android.

## 3. Import this account's verified emulator session

Obtain the private app profile from an existing installation of the same app
build. On that installation, export it once:

```bash
.venv/bin/gigamoney api-bootstrap export-profile
```

Transfer only the resulting `config/api-app-profile.json` through your private
deployment channel. Do not transfer another account's session, trading password,
or journal. The profile is checked against captured signatures during import.

On the new machine, leave the runner stopped. Start Android capture, sign into
the intended account, complete its phone verification, and perform trade
authentication if prompted. Open/refresh Orders or Portfolio so a successful
account read is captured. No test order is needed for enrollment.

```bash
.venv/bin/python scripts/proxy.py --config config/gigamoney.config.json start
.venv/bin/python scripts/proxy.py --config config/gigamoney.config.json status
```

Set `api.bootstrap.capturePath` to that capture directory's `flows.mitm`. Inspect
the routes and copy the intended account's three values into `api.accountAaid`,
`api.orgId`, and `api.accountChannel`:

```bash
.venv/bin/gigamoney api-bootstrap inspect
.venv/bin/gigamoney api-bootstrap import --from-emulator
.venv/bin/gigamoney validate-config
.venv/bin/python scripts/api.py read positions
.venv/bin/gigamoney cancel-all --dry-run
```

The importer reads credentials from the already verified app through local
instrumentation; it does not log in, submit orders, or bypass phone verification.
It requires a recent successful account read with a matching signature and
credentials, plus successful trade authentication identifying exactly one device.
If the app refreshed between capture and extraction, refresh its Orders screen
and repeat the import. `api.bootstrap.maxCaptureAgeSeconds` bounds capture age.
A capture containing both main/trade refresh pairs can be imported without
`--from-emulator`. Never use a capture/session belonging to a different account.

Import refuses to overwrite a session bound to another account. Existing legacy
unbound session files require migration or a separate destination. This
workstation's existing session is migrated as part of this change.

The diagnostic read must succeed for the intended account before enabling command
consumption. `cancel-all --dry-run` only reports open orders. Stop Android capture
and shut down the emulator after enrollment if using API trading independently.
Closing Android is sufficient; logging out is unnecessary.

## 4. Start and stop

From the checkout, start the foreground Kafka runner:

```bash
.venv/bin/gigamoney --config config/gigamoney.config.json runner
```

In another terminal:

```bash
.venv/bin/gigamoney --config config/gigamoney.config.json runner-status
.venv/bin/gigamoney --config config/gigamoney.config.json runner-stop
```

For a service, generate a unit using the actual checkout, Python and config paths:

```bash
.venv/bin/gigamoney render-service
mkdir -p ~/.config/systemd/user
cp work/systemd/gigamoney-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gigamoney-runner.service
```

Adjust those last paths/names when `paths.workDir` or `service.name` differs. The
generator does not install/start the unit. For a user service to survive logout
and start at boot, an administrator can enable lingering for the deployment user
with `sudo loginctl enable-linger "$USER"`. Logs go to the configured runner log;
the unit also preserves its location in runner status for the command center.
Use `systemctl --user stop gigamoney-runner.service` to stop a managed instance.

## Optional command center and API traffic recording

Set `monitor.adminUsername`, `monitor.adminPassword`, TLS certificate/key paths,
bind address/port and public hostname. Use unprivileged HTTPS ports such as 8787
unless the deployment user has been granted the required port-binding access.
For local testing, a certificate can be generated with OpenSSL and its paths set
in `monitor.certFile` and `monitor.keyFile`; production certificate provisioning
depends on the host/domain. The monitor requires HTTPS.

```bash
.venv/bin/gigamoney monitor-start
.venv/bin/gigamoney monitor-status
.venv/bin/gigamoney monitor-stop
```

The monitor's Start script button uses its configured account and Python
environment. Avoid competing process managers; choose the monitor or systemd to
manage the runner. Restart the monitor after changing its loaded configuration.

Optional API capture uses `proxy.apiPort`, binds only to loopback and selects the
session belonging to the same `--config`:

```bash
.venv/bin/python scripts/api_proxy.py --config config/gigamoney.config.json start
.venv/bin/python scripts/api_proxy.py --config config/gigamoney.config.json status
.venv/bin/python scripts/api_proxy.py --config config/gigamoney.config.json stop
```

Capture is not required for API runtime. When enabled, its process must remain
available; failures do not silently bypass recording. Use separate ports/state
directories for separate deployments on one machine.

## Verification and long-running behavior

Normal main/trade refreshes persist credentials without Android. HTTP 402 /
402001 on a read now triggers immediate trade authentication and one read retry.
Financial submissions are never blindly replayed. If human verification is
required, the runner records `authentication_required`, stops intake cleanly and
shows Attention in the command center. A command blocked before submission is
not committed. After verification/import, manually start the runner; ordinary
timestamp checks still discard stale commands. Check statuses before sending
fresh commands after an outage. Scheduled restarts do not clear this pause.

Validation uses offline broker/credential doubles for two accounts, separate
config/state roots, account-mismatch guards, cent rounding, 30-second scheduling,
authentication recovery and challenge handling. The generated systemd unit is
checked locally. This change does not claim a completed 15-day endurance test or
deployment to a second physical host. The emulator credential getter is tied to
the reviewed app build; the import and subsequent signed read are the required
account-specific enrollment checks on a new machine.
