# Protocol investigation and validation notes

These notes preserve observations from September 8–9, 2026. Descriptions of
missing features or test limitations refer to the implementation at that point
in the investigation. Current setup and behavior are documented in the
[deployment](deployment.md), [API backend](api.md), and [capture](proxy.md) guides.
Raw captures, account credentials, and actual order IDs remain outside Git.

## Android capture investigation

### Validation and API findings

Read-only validation captured successful responses from:

- `GET /v3/account/list`
- `GET /v7/watchlist/groups/all`
- `GET /v6/portfolio/detail` (positions in `data.stockhold`)
- `GET /v5/portfolio/cash_info` (cash in `data.cash_balance`)

Cash and positions are readable JSON. Both the app and complete emulator
restart/reconnect paths were exercised. No order was submitted or replayed for
these tests. Capturing order-submission traffic requires a separately authorized
order from the user; the capture scripts never create one.

The API uses `authorization`, `x-authorization`, `x-api-signature`, and device
headers. Replacing emulator trading requires a separate client that implements
session handling, request signing, order confirmation, and duplicate prevention.
This change supplies captures; it does not change the trading runner to use the
API or prove that captured requests can be replayed successfully.

### Manual-order investigation (2026-09-08 UTC)

The capture session beginning `20260907T234322.049937Z` missed the main API
connections after the scheduled emulator restart. Background HTTPS and some
WebSocket traffic continued, so the original process-only `capture_ready` check
was misleading. The app's diagnostic uploads nevertheless retained these
request summaries, each reporting HTTP 200 and application code 0:

| UTC time | Reported request | Evidence |
| --- | --- | --- |
| 00:20:52.272 | `POST /v3/orders/submit`, `order_type=MO` | Market submission request fields; no full response |
| 00:21:45.967 | `POST /v3/orders/submit`, `order_type=LO` | Limit submission request fields; no full response |
| 00:22:26.022 | `POST /v1/tube/replace` | Replace request fields; no full response; recovered after correcting the report filter |
| 00:22:40.546, 00:22:47.547, 00:22:53.454 | `POST /v2/orders/withdraw` | Three cancel request summaries; no full responses |

The original report missed the replacement because it searched only `/orders/`
paths. The later verified replacement identified `/v1/tube/replace`, allowing
the earlier diagnostic summary to be recovered as well. The report now handles
both paths. The logged result codes describe API acceptance, not proof of
execution or a final order state.

Observed submission fields include `action`, `counter_id`, `order_type`,
`quantity`, `time_in_force`, `settlement_currency`, `client_request_id`, and
commission/card options. The market sample also has `trigger_price`; the limit
sample has `price` and `force_only_rth`. Cancel includes `order_id`,
`order_id_str`, `aaid`, `account_channel`, and `org_id`. Their enum meanings,
optional-field rules, sell behavior, native FOK support, and duplicate handling
must be established before implementing writes. Do not reuse the captured
client request ID or infer server idempotency from that field's name.

A Wi-Fi reconnect restored full capture; opening Orders at 00:29:12 captured
successful `/v5/orders/today` and `/v4/orders/history` responses. Replaying the
read-only today-orders request directly from Linux also returned HTTP 200 and
application code 0. This uses an existing captured signature and session; it
does **not** demonstrate fresh request signing, session renewal, or independent
trading with the emulator stopped. Unsigned probe variants returned 403.

The runner log records password entry at 00:17:42.659 for an order retry and
00:18:48.521 for a holdings query. It stopped at 00:19:48.139, before the manual
order summaries above. The certificate hook does not type passwords.

Reproduce the evidence report and read-only connectivity probe with:

```bash
work/proxy/venv/bin/python scripts/proxy_inspect.py orders
work/proxy/venv/bin/python scripts/proxy_inspect.py probe --endpoint orders
```

Use `--capture /absolute/path/to/flows.mitm` for an earlier session. The evidence
report labels full HTTP captures separately from app telemetry. The probe only
allows GET requests to account list, today orders, positions, or cash; it blocks
redirects and prints status/timing rather than credentials or account bodies.
It neither refreshes expired credentials nor generates signatures. All live
requests made during this investigation were read-only; no order was submitted,
replaced, or canceled by the diagnostic scripts.

The capture status now requires actual account/order evidence and invalidates
it on an app/process change, explicit reconnect, or five minutes of inactivity.
That check improves visibility; it cannot prove that every concurrent API
socket is intercepted. Confirm a full HTTP record for each manual workflow
before considering its capture complete. Local interception and socket
selection follow [mitmproxy's local capture mode](https://docs.mitmproxy.org/stable/concepts/modes/#local-capture).

### Verified market submission (2026-09-08 01:22 UTC)

The user submitted a staged BIYA market Buy ticket for one share. Full HTTP
capture recorded `POST /v3/orders/submit` at 01:22:12.971 UTC, returning HTTP 200
and application code 0 in 677.62 ms. The request contains `action: "1"`,
`counter_id: "ST/US/BIYA"`, `order_type: "MO"`, `quantity: "1"`,
`time_in_force: "0"`, and `trigger_price: "2.29"`, alongside the commission/card
options and client request ID described above.

The submit response provides an order ID but leaves several other fields,
including status and quantity, empty. A subsequent `GET /v3/orders/detail`
returned the matching order under `data.order`: status `NotReported`, quantity
`"1"`, executed quantity `"0"`, action `1`, and order type `MO`. The UI displays
this as "To submit". API submission acceptance must therefore be followed by a
correlated status query; it does not demonstrate a fill. Preserve order IDs as
strings and treat this status as pending, not as a reason to resubmit.

Full capture also recorded `POST /member/trade/refresh_token` immediately before
submission (HTTP 200, code 0, 220.5 ms). The response contains
`data.token.token`, `data.token.refresh_token`, and
`data.token.refresh_token_expires`, plus `data.aaid`. Tokens and actual order
IDs remain in the private capture directory. This records app-driven renewal;
standalone renewal and fresh request signing were not yet implemented at this
stage. Their subsequent verification is recorded in [api.md](api.md).

No automated order submission was performed.

### Verified limit submission (2026-09-08 01:30 UTC)

The user submitted a BIYA limit Buy for one share at USD 2.29. Full HTTP capture
recorded `POST /v3/orders/submit` at 01:30:26.615 UTC, returning HTTP 200 and
application code 0 in 304.90 ms. The request has `action: "1"`,
`counter_id: "ST/US/BIYA"`, `order_type: "LO"`, `price: "2.29"`,
`quantity: "1"`, `time_in_force: "0"`, and `force_only_rth: "2"`.

The correlated `GET /v3/orders/detail` reports status `NotReported`, price
`"2.29"`, quantity `"1"`, and executed quantity `"0"`. Its `button_control`
contains `withdraw: 2` and `replace: 2`; the UI exposes Amend and Cancel.
The amendment form displays "Valid for the day" and "RTH+Pre/Post-Mkt" for
this particular order. No general enum mapping is inferred beyond this sample.
Opening Amend leads to editable price/quantity fields and an **Edit Confirm**
button; opening the form alone does not submit the replacement.

### Verified replacement (2026-09-08 01:34 UTC)

The user confirmed changing the BIYA limit Buy from one share at USD 2.29 to
two shares at USD 2.28. Full HTTP capture recorded `POST /v1/tube/replace` at
01:34:42.449 UTC, returning HTTP 200 and application code 0 in 303.61 ms.
The request fields are `order_id`, `order_id_str`, `price`, and `quantity`;
the new price and quantity are strings. The response data includes `order_id`,
`order_id_str`, and `approval` (false in this sample).

The correlated `GET /v3/orders/detail` confirms `ReplacedNotReported`, price
`"2.28"`, quantity `"2"`, and executed quantity `"0"` on the original order.
This remains a pending order; the UI still displays "To submit". The boolean
`approval` alone must not be treated as the replacement's success result.
`proxy_inspect.py orders` now includes this route in both full HTTP and
diagnostic summaries. Its network probe remains read-only and refuses this
replacement endpoint.

### Verified cancellation (2026-09-08 01:41 UTC)

The user canceled the amended two-share BIYA limit Buy. Full HTTP capture
recorded `POST /v2/orders/withdraw` at 01:41:25.495 UTC, returning HTTP 200,
application code 0, and `data.ok: true` in 294.56 ms. The request fields
`aaid`, `account_channel`, `order_id`, `order_id_str`, and `org_id` are all
strings in this sample.

The matching `GET /v3/orders/detail` and the subsequent today-orders response
confirm `CanceledStatus`, price `"2.28"`, quantity `"2"`, and executed quantity
`"0"`; the UI displays "Canceled". The separate one-share market Buy remained
`NotReported` with zero filled at this check. These captures establish the
buy-side market, limit, replace, and cancel message formats for the observed
pending-order states. They do not validate standalone signing/renewal, sell
orders, native FOK, fills, partial fills, or timeout/idempotency behavior.

## Direct API validation, 2026-09-08 UTC

Live read/staging checks on 2026-09-08 succeeded for BIYA buy/sell market and
limit orders, FOK staging, cancel-all inspection, and normalized holdings.
Every checked holdings field matched the app for the four rows its UI scraper
returned; the API returned all five positions. A holdings read took about 0.5
seconds. Actual order submission/cancellation in this integration was tested
offline, using the request shapes previously captured from user-submitted
orders; no new live financial orders were sent by these checks.

With the emulator fully stopped, all seven CLI read/staging checks also passed
(0.3–1.1 seconds including process startup). Main/trade renewal and all account
read routes returned HTTP 200 / API 0. Offline signature verification
matched 1,819/1,819 captured signatures across 116 method/path combinations,
including market/limit submissions, replace, cancel, and authentication:

```bash
work/proxy/venv/bin/python scripts/api.py verify-capture \
  /absolute/path/to/flows.mitm
```

Only offline capture reading requires the proxy virtual environment. Replacement
is not an existing Kafka command and is not added to the runner by this change.

### User-generated Kafka tests, 2026-09-09 UTC

The reverse proxy captured the following live requests from commands submitted
by the user through the control UI. Each command produced exactly one submit
request, with verified signing, account routing, and current commission-card IDs.

- At 01:28:50, a BIYA market buy for 200 shares used `order_type: MO`, action
  Buy, DAY, USD settlement. The broker returned HTTP 200 / API 0 and an exact
  order ID. Detail confirmed `NotReported`, 0 filled. The workflow took 1.590 s.
  The immediate holdings snapshot published 2.451 s after command receipt, and
  the next periodic query began 120.071 s after that publication completed.
- At 01:30:59, a BIYA limit buy for 100 shares at 2.3625 used `order_type: LO`,
  the exact supplied price, DAY, and regular-plus-pre/post-market settings. The
  broker rejected it with HTTP 400 / API 602035: invalid price increment. Its
  order-info response specified a 0.01 price step in that range. The workflow
  took 0.541 s and correctly persisted a rejected journal entry, committed the
  failed command, and made no duplicate submission or success-triggered update.

At the time of this test, the implementation sent the supplied limit price
unchanged. The subsequent change rounds order prices directionally to cents and
shortens the holdings interval to 30 seconds. This test proves rejection handling; it does
not establish that a valid-priced limit submission will be accepted. Market
submission acceptance is also distinct from execution/fill. The corresponding recordings remain private local artifacts.
