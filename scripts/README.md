# Capture and diagnostic tools

Normal trading starts through `.venv/bin/gigamoney runner`. The tools here support
session enrollment, traffic recording, diagnostics, and emulator preparation.
Shared trading and session code lives in [`gigamoney/`](../gigamoney/).

## Commands to run

Run from the checkout root. Each Python command below accepts `--config` before
its action/options; `GIGAMONEY_CONFIG` also selects the main configuration.

| Script | Purpose | Python environment |
| --- | --- | --- |
| `api.py` | Account reads, session status/refresh, and offline signature checks. | `.venv`; use the proxy environment for `verify-capture`. |
| `api_proxy.py` | Start/stop/status/report for the optional API runner reverse proxy. | `.venv`; launches the configured proxy environment. |
| `proxy.py` | Start/stop/status/report/reconnect for Android traffic capture. | `.venv`; launches the configured proxy environment. |
| `proxy_inspect.py` | Inspect Android captures or probe an allowlisted account GET with captured credentials. | Proxy environment. |
| `proxy_boot.py` | Build a separate CA-enabled Magisk boot artifact for the reviewed Android image. | Proxy environment. |

Examples:

```bash
.venv/bin/python scripts/api.py --config config/gigamoney.config.json status
.venv/bin/python scripts/api_proxy.py --config config/gigamoney.config.json status
.venv/bin/python scripts/proxy.py --config config/gigamoney.config.json status
```

The proxy environment defaults to `work/proxy/venv/`; dependencies are pinned in
`proxy-requirements.txt`. `proxy.pythonPath` and `proxy.stateDir` select alternative
locations. Full setup and operation are in the [capture guide](../docs/proxy.md).

## Support files

These files are loaded or installed by the managers/builders:

| File | Loaded by | Role |
| --- | --- | --- |
| `proxy_capture.py` | mitmproxy, launched by either capture manager | Record request/response metadata and stream messages. |
| `proxy_trust.py` | `proxy.py` | Supervise local app instrumentation across process restarts. |
| `proxy_trust.js` | `proxy_trust.py` | Add the local capture certificate to the app's existing trust list. |
| `proxy_ca.rc`, `proxy_ca.sh` | `proxy_boot.py` | Install the capture CA in the separate emulator boot image. |

Generated certificates, boot images, recordings, and process state belong under
the private proxy state directory, not alongside these source files. API trading
does not need these capture processes once its verified session is imported.
