# GigaMoney traffic capture

All manager commands accept `--config /path/account.json` (or `GIGAMONEY_CONFIG`).
`proxy.stateDir`, `proxy.pythonPath`, `proxy.apiPort` and `proxy.boot` select deployment-specific paths and capture settings. See [deployment.md](deployment.md) for a new account and [configuration.md](configuration.md) for every setting.

The [direct API trading backend](api.md) is integrated with the Kafka runner.
Use the API proxy below to record its traffic; the older Android capture only
records emulator processes.

## API runner capture

```bash
.venv/bin/python scripts/api_proxy.py start
.venv/bin/python scripts/api.py read orders
.venv/bin/python scripts/api_proxy.py status
.venv/bin/python scripts/api_proxy.py report
```

The reverse proxy listens on `127.0.0.1` (port `18080` by default) and forwards to the API
session's fixed upstream host using verified HTTPS. No system CA or global
proxy setting is changed. The client signs the original path/body, sends them
over local HTTP to the proxy with the original Host header, and the proxy
forwards them unchanged. This records the Linux API runner's requests;
Kafka, SSH, and command-center traffic are outside this capture.

`start` launches a background capture and stores an explicit `capture_proxy`
route in the private API session. Once the updated client code is loaded, each
request reloads that route; subsequent capture start/stop operations do not need
a runner restart. A runner started before this feature was installed must first
be stopped and started from the command center. `capture_ready` requires a real,
successful account/order response within `proxy.evidenceMaxAgeSeconds` (300 seconds by default), not just a
listening proxy process. The recorder continues running when this freshness
indicator expires during an idle period.

Each start creates `work/proxy/captures/<UTC-time>-api/`. `flows.mitm` contains
complete requests/responses; `events.jsonl` contains UTC timestamps, route,
request ID, Kafka command ID when present, HTTP/API codes, and elapsed time.
Command IDs are carried in a local-only header which the proxy removes before
forwarding. The report summarizes response counts/timing and transport errors.
Bodies and credentials remain only in the owner-only raw captures, ignored by
Git. No capture command submits, cancels, or replays an order. Your Kafka tests
use the existing runner and trading limits.

```bash
# Restore direct HTTPS, then stop and flush the capture.
.venv/bin/python scripts/api_proxy.py stop
```

If the capture process exits unexpectedly while the route is enabled, requests
fail instead of silently bypassing recording. Restart the proxy or run `stop`
to restore direct routing. Do not resend a financial command solely because a
proxy/network response was lost; the existing order journal handles uncertainty.

Validation on 2026-09-09: actual GET order/cash reads and POST trade-token
renewals returned HTTP 200 / API 0 through this proxy. Captured signatures
matched fresh signing inputs, the local command header was removed upstream,
and request IDs/command IDs were retained in capture metadata. No financial
order was submitted for this validation.

## Android capture

The capture setup records GigaMoney's HTTPS requests/responses and WebSocket
messages on this Linux host. It uses mitmproxy's local process interception,
because the app bypasses Android's HTTP proxy setting. No HTTP proxy setting is
needed in Android. Only the Android SDK emulator/network processes are selected;
loopback connections are passed through. Background traffic from other apps in
this emulator can also appear. Host Kafka, SSH, and dashboard processes are not
selected. The setup does not expose an HTTP proxy or capture UI on a public port.

The reviewed setup uses `lb.whale.hkwinner.android` version 4.72.1 on Android 16,
API 36, x86_64. Its Rust HTTP client pins server certificates. A Frida hook adds
the exact local `*.lbkrs.com` server certificate to the existing certificate
list, preserving the original certificates, flags, and proxy argument. TLS
validation remains enabled. The app APK is unchanged.

Android capture requires a separate Magisk 30.7 boot image containing the
capture CA, plus local instrumentation. Follow [artifact and instrumentation
setup](#prepare-the-boot-artifact-and-instrumentation) before starting capture on
a new machine. The default artifact directory is `work/proxy/image/`; the stock
SDK image stays unchanged. Back up the stopped AVD, including its original
`config.ini`, under `work/proxy/backup/<AVD-name>.avd/` before activation.

### Operate the capture

Run these commands from the repository root:

```bash
.venv/bin/python scripts/proxy.py status
.venv/bin/python scripts/proxy.py report
```

`status` distinguishes `transport_ready` (processes and hook are running) from
`capture_ready` (a successful account/order API response was actually captured
within `proxy.evidenceMaxAgeSeconds`, after the latest app/process change or network
reconnect). It includes the endpoint and time of that evidence. An idle app can
expire this check without a fault; open Orders or refresh account data to verify
it again. Pings, quote streams, and diagnostic uploads do not qualify.
`report` lists response counts, HTTP status codes,
mean response times, and errors without printing authorization headers or bodies.

To change capture connections, first stop the trading runner and wait until
`runner-status` reports `running: false`:

```bash
.venv/bin/gigamoney runner-stop --force
.venv/bin/gigamoney runner-status
.venv/bin/python scripts/proxy.py stop
.venv/bin/python scripts/proxy.py start
```

If the hook is running but a read-only account refresh produces no captured API
response, reconnect existing sockets with the runner stopped:

```bash
.venv/bin/python scripts/proxy.py reconnect
```

Then open Orders or refresh account data and check `capture_ready` again.
`reconnect` briefly cycles emulator Wi-Fi, preserves the capture files, and does
not restart the app or enter a trading password. `start` can return with
`transport_ready: true` while this account/API verification is still pending.

`start` restarts GigaMoney with the certificate hook, waits for the Rust client
to accept the capture certificate, starts interception, and reconnects emulator
Wi-Fi to replace sockets opened before capture. It leaves the trading runner
stopped. Resume it through the existing command center, or:

```bash
.venv/bin/python -c 'from gigamoney.config import load_settings; from gigamoney.monitor import GigaMoneyController; print(GigaMoneyController(load_settings()).start_script())'
```

`stop` stops interception and detaches the certificate hook. Android has no
persistent HTTP proxy route to clear. The rooted boot image and certificate
remain installed until rolled back. This is a manually managed capture session,
not a new host service enabled at boot.

### Capture files

Every start creates a private directory under `work/proxy/captures/`:

- `flows.mitm`: full HTTP headers and bodies, responses, and completed flows.
- `messages.jsonl`: each WebSocket/TCP/UDP message as it arrives, including
  timestamp, direction, connection ID, and lossless base64 bytes. This also
  preserves messages from connections that are still open.
- `events.jsonl`: request/response timing, status, TLS failures, and capture gaps.
- `proxy.log`: mitmproxy connection diagnostics.
- `targets.json`: the currently selected emulator process IDs.
- `api-health.json`: evidence of the latest successful account/order response,
  tied to the current app/process generation, without headers or response data.

`work/proxy/trust.log` records certificate-hook application and reconnects.
The supervisor follows app restarts, and the proxy refreshes emulator process
IDs every second. Interception pauses when the hook is unavailable, allowing
normal app connections; these intervals appear as `capture_targets` events with
`capture_ready: false`. Startup/reconnect intervals are therefore **coverage
gaps**, not evidence that every request was captured. Existing sockets opened
during such a gap may remain outside capture until reconnected. Use a fresh
capture start for a controlled workflow recording.

Raw captures contain account data, credentials, and request signatures. Runtime
files live beneath an owner-only `work/proxy/` directory and are ignored by Git.
Keep that directory local. Captures have no automatic retention limit; stop a
session when it is no longer needed. Long-lived quote streams also consume
memory while mitmproxy retains their flows.

To inspect a completed capture with a local web UI:

```bash
work/proxy/venv/bin/mitmweb --no-server --web-host 127.0.0.1 --web-port 8081 --set web_open_browser=false -r /absolute/path/to/flows.mitm
```

### Prepare the boot artifact and instrumentation

The builder prepares a separate image and does not activate it. Keep the source
SDK image stock. It validates Android API 36/x86_64 and the official Magisk APK
hash. A matching running emulator is used to execute Magisk's Android binaries.

```bash
umask 077
python3 -m venv work/proxy/venv
work/proxy/venv/bin/pip install -r scripts/proxy-requirements.txt -e '.[api]'
curl -fL https://github.com/topjohnwu/Magisk/releases/download/v30.7/Magisk-v30.7.apk -o work/proxy/Magisk-v30.7.apk
work/proxy/venv/bin/python scripts/proxy_boot.py --source-image /absolute/path/to/Android/Sdk/system-images/android-36.1/google_apis_playstore/x86_64 --magisk-apk work/proxy/Magisk-v30.7.apk
```

The script creates the CA and fixed server certificate if absent, merges the
SDK's concatenated cpio archives, adds the CA boot hook, and patches a copy of
the ramdisk using the official Magisk binaries. It preserves verity/encryption
flags. The fixed server certificate expires after one year; renew the public
certificate and private-key bundle together, then restart capture so its pin
matches. Do not regenerate the CA without rebuilding its emulator trust setup.

Before activation, stop the trading runner and emulator, back up the entire AVD,
and change its `config.ini` `image.sysdir.1` to the absolute output directory.
Cold-boot without loading a snapshot. Install the verified Magisk APK, complete
its additional setup/reboot, and allow the Shell entry in Magisk's Superuser
page. Confirm `/apex/com.android.conscrypt/cacerts/<certificate-hash>.0` matches
`work/proxy/ca/mitmproxy-ca-cert.pem`.

The matching instrumentation binary is the official
`frida-server-17.17.0-android-x86_64.xz` from the
[Frida 17.17.0 release](https://github.com/frida/frida/releases/tag/17.17.0).
Its compressed SHA256 is
`5f0671fbeaee0b1ee66b1536c3eae966d58765b7e3954ea49932e5931e43bd6c`.
Verify it before decompressing. Install the decompressed binary as
`/data/adb/gigamoney-proxy/gigamoney-frida-server` (root-owned, executable;
parent directory mode 700). The supervisor starts it bound to emulator loopback
and reconnects after an emulator reboot. Frida and frida-server versions must
match. The CA setup is based on
[mitmproxy's certificate guidance](https://docs.mitmproxy.org/stable/concepts/certificates/)
and [Magisk's boot overlay mechanism](https://topjohnwu.github.io/Magisk/guides.html).

### Rollback

Stop the trading runner, stop capture, and stop the emulator. Restore the original
`image.sysdir.1` entry from `work/proxy/backup/<AVD-name>.avd/config.ini` into the
current AVD configuration, then cold-boot. This restores the original boot image
and system certificate store while retaining current app/account state. The
Frida service will no longer start without the rooted image. The unused Magisk
app can be uninstalled. The complete AVD backup is for recovery; restoring it
would also rewind local app data, so it is not needed for a normal rollback.

For dated protocol observations and validation evidence, see the
[investigation notes](investigation.md).
