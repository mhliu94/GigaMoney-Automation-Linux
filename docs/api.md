# Emulator-free trading backend

The existing CLI, Kafka runner, and command center now support `gigamoney.backend:
"api"`. Market orders, limit orders, synthetic FOK, cancel-all, and holdings use
freshly signed HTTPS requests. Runtime trading needs no emulator, ADB, Frida,
proxy, OCR, or recorded signature. The Android backend remains selectable.

For ongoing request/response recording of Kafka tests, enable the optional
[API reverse proxy](proxy.md#api-runner-capture) with
`.venv/bin/python scripts/api_proxy.py start`. Capture persists in the background;
the normal API runtime does not require it.

For another Linux machine/account and a repeatable verified-emulator session import, see [deployment.md](deployment.md). All supported operator settings are listed in [configuration.md](configuration.md).

## Select and run

Install the API and Kafka dependencies in the existing environment:

```bash
.venv/bin/pip install -e '.[api,kafka]'
```

In the private `config/gigamoney.config.json`, merge these settings into the
existing configuration; retain the existing Kafka topics, consumer group,
account IDs, and monitor credentials:

```json
{
  "gigamoney": {"backend": "api"},
  "api": {
    "sessionPath": "work/api/session.json",
    "allowedSymbols": ["NVDA", "BIYA", "AEHL", "TANH"],
    "maxOrderPages": 100,
    "fokWaitSeconds": 2,
    "positionCost": "diluted"
  }
}
```

Set the symbol allowlist for the intended account before starting the runner.
The API backend imposes no local share-quantity or lifetime order-count cap.
Legacy `api.maxOrderQuantity` and `api.maxOrders` settings are accepted but ignored,
including numeric values in existing configs. Broker trading permissions and lot
sizes apply, and sells require sufficient available long holdings. Preserve the
order journal for duplicate protection and reconciliation.

The same commands now use the selected backend:

```bash
# Read/stage only: these commands do not submit or cancel orders.
.venv/bin/gigamoney holdings
.venv/bin/gigamoney market-order --symbol BIYA --quantity 1 --side Buy --no-submit
.venv/bin/gigamoney limit-order --symbol BIYA --price 2.28 --quantity 1 --side Sell --no-submit
.venv/bin/gigamoney limit-order --symbol BIYA --price 2.28 --quantity 1 --side Buy --kill --no-submit
.venv/bin/gigamoney cancel-all --dry-run

# Start the Kafka service when ready to execute commands.
.venv/bin/gigamoney runner
.venv/bin/gigamoney runner-status
.venv/bin/gigamoney runner-stop
```

Removing `--no-submit` from an order command submits it. Removing `--dry-run`
from `cancel-all` cancels open orders. In API mode both order preview flags read
current account/ticket data and return an unsubmitted ticket; they never write
an order. Direct CLI commands are independent of the Kafka trading-enabled
switch, as with the Android CLI.

The command center's **Start script** button starts the selected backend. In API
mode its health checks require only Linux, and its scheduled restart restarts
the runner without starting, stopping, or waiting for Android. Reload the monitor
after changing backend settings. To switch back, stop the runner, set the backend
to `android`, and restart the monitor/runner with the emulator ready.

## Preserved Kafka behavior

- The existing message schemas and topics are unchanged: `MARKET_ORDER`,
  `LIMIT_ORDER`, `LIMIT_ORDER_FOK`, `CANCEL_OPEN_ORDERS`, and
  `SET_TRADING_ENABLED`. `time_in_force: "FOK"` and `cancel_unfilled: true`
  still select the synthetic FOK workflow. Notional market orders remain rejected.
- Account filtering, timestamp/staleness checks, manual offset commits,
  persistent trading-enabled state, and the existing failed-command policy remain.
- Holdings publish on startup, immediately after a successful new order, and every
  30 seconds. **Every update resets that interval from completion of Kafka
  delivery**, including the immediate update. A failed update also waits a fresh
  interval. Order commits precede publication; publication failure cannot replay
  the order. An acknowledged order with failed FOK/confirmation still triggers
  a position update.
- The holdings payload retains `account_id`, `account_num_id`, `cash`,
  `cash_by_currency`, `positions`, `ts`, and `trading_enabled`. Positions retain
  quantity, cost, market price/value, and daily P/L fields. `api.positionCost` selects diluted or average cost to match the app preference. API holdings include the complete
  stock list, independently of watchlist filters or what fits on screen.
- One process lock prevents two runners; another serializes API workflows across
  runner and CLI processes. Keep one session/journal directory for the account.

## Order flow, retries, and timing

A market order validates symbol/quantity and reads current security permissions,
lot size, supported order settings, and fee-card IDs. Sells also read holdings
and open orders, and require enough available long shares after outstanding
sell commitments. It reads the broker's non-delayed US market status. A
regular-hours market order has neither a client-selected price nor a price
lookup. Extended-hours conversion selects its limit from the background cache,
saves the command intent, signs and sends one submit request, saves the exact
string order ID, and reads order detail to confirm the matching ticket and status.

Limit orders follow the same flow with the limit price and the app's
DAY / regular-plus-pre/post-market setting. During regular hours, market commands
remain DAY market orders. During premarket and after-hours, they become DAY limit
orders with regular-plus-pre/post-market execution enabled. Price and quantity
travel as decimal strings. No cached fee IDs,
order IDs, timestamps, or signatures are reused for a new order.

### Kafka pricing and extended hours

The command schema is unchanged. `MARKET_ORDER` messages do not need to contain a
price. During regular hours the broker determines the execution price; no
`price` or `trigger_price` is sent. During extended hours, `kafka.priceBooksTopic`
(default `price-books`) supplies cached order-book snapshots for limit conversion.
The selector uses `offer_side.levels` for buys and `bid_side.levels` for sells, sorting
by `prc` and accumulating `total_qty` (or `qty`) until the requested quantity is
covered. The last required level supplies the price. If visible depth is smaller
than the requested quantity, it uses the outermost available level; partial fills
and an unfilled remainder are possible. It adds no percentage allowance, price
ceiling, price floor, or comparison against the broker quote.

During premarket/after-hours the cached book supplies the actual limit price,
with `order_type=LO` and `force_only_rth=2`. An explicit `LIMIT_ORDER` or
`LIMIT_ORDER_FOK` keeps its command's `limit_price`; it does not read or compare
against a Kafka or broker quote. Positive finite values and the existing
directional cent rounding still apply. Broker price-band validation remains with
the broker.

The market-status endpoint supplies `trade_status`, separately from
`delay_trade_status`: 201 is premarket, 202 regular trading, and 203 after-hours
in the reviewed app protocol. Closed, overnight, and unknown sessions fail before
submission. `api.extendedHoursMarketOrders` can disable automatic conversion;
market commands outside regular hours then fail instead of being queued.
`api.marketStatusMaxAgeSeconds` checks the broker's session-status timestamp.

The runner starts a dedicated background thread that refreshes all allowed
symbols every `kafka.priceBookPollIntervalSeconds` (30 seconds). It directly
assigns partitions under a separate consumer identity, never commits, and never
joins the trading command consumer group. The initial forward scan starts at
the five-minute freshness window; subsequent refreshes retain the consumer and
advance from the last scanned offsets. `kafka.priceBookTimeoutSeconds` bounds
each background refresh, and `kafka.priceBookBatchSize` controls batch size.

Completed refreshes atomically publish `work/api/price-books.json`. Both the
runner and CLI read that cache at order time without network access or waiting
for Kafka. `kafka.priceBookMaxAgeSeconds` defaults to 300 and can be tightened;
the cache never accepts source prices older than 300 seconds, including legacy
null settings. Both the snapshot and the selected side's original `update_time`
must satisfy this limit. Refreshing the cache does not reset source timestamps.
A feed outage leaves the last completed cache available until it expires.
Missing, invalid, future-dated, or expired books fail before submission; there
is no fallback to an older book or delayed broker quote. An extended-hours
command arriving before the first usable cache is ready fails immediately.
Explicit limit orders and regular-hours market orders work without the cache.

The old `kafka.priceBookLookbackSeconds` setting remains accepted for config
compatibility; the background reader's history is bounded by source freshness.

`api_order_pricing`, the workflow summary, and the persistent journal record the
requested/effective order type, session, Kafka topic/partition/offset, source
price and timestamp, age, depth used, and submitted price. Accepted submissions
still report actual status and filled quantity; they are not a promise of an
immediate fill. Reconciliation reuses the recorded request even if prices or
sessions have since changed. Existing queued orders are not converted or replaced.

Preview the selected ticket and price without submitting:

```bash
.venv/bin/gigamoney market-order --symbol BIYA --quantity 100 --side Buy --no-submit
```

### Rounding, cancellation, and recovery

Before submission, prices with more than two decimal places round to cents: up
for Buy and down for Sell. This covers limit/FOK prices and market trigger prices.
For example, 2.3625 becomes 2.37 for Buy or 2.36 for Sell; exact-cent values do not
move. A sell price that rounds to zero is rejected locally. Logs include the
command ID, requested price, and effective price whenever formatting or rounding
changes it. The journal retains the original command intent and stores the
effective request separately, so existing commands remain deduplicated.

FOK preserves the existing **limit-then-cancel** behavior: after acknowledgment,
wait `api.fokWaitSeconds` (default 2 seconds), check the order, and cancel any
remaining open quantity. This is not an exchange-native atomic FOK; partial fills
can occur. A fully filled order needs no cancel. Cancel-all uses the app's order
list route with pagination and exact account/order IDs. Cancellation must reach
a terminal status; HTTP success alone is insufficient. Status checks wait
0, 1, 1, then 2 seconds, with a read between each wait. A fill racing a cancel
is reported as filled, never as a successful cancellation.

Reads get at most two transport/server-error attempts, separated by 250 ms.
Authentication recovery is bounded separately. Financial writes have no blind
transport retry: after a lost reply the workflow reconciles the existing request
instead. This intentionally differs from repeating UI taps after failure.

Each Kafka `command_id` has an atomic, private record under `work/api/orders/`.
Commands missing an ID use their Kafka topic/partition/offset as a stable key.
Re-delivery returns a completed result or reconciles an interrupted one; a changed
payload with the same ID is rejected. The send marker is persisted **before**
the HTTP mutation, so a crash cannot turn a possibly accepted request into a
fresh submission. The order-count cap counts these sends, including uncertain
ones, across restarts. Do not delete the journal to retry an order.

When the broker order ID was saved, confirmation can resume on restart. Startup
also completes an interrupted FOK's cancellation if its cancel was never sent.
When the submit reply was lost before an ID could be saved, automatic recovery
requires the broker to echo `client_request_id`. The observed order-list responses
do not echo it; in that case the result stays unresolved and new submissions
are refused. Inspect the journal and broker orders, and reconcile the exact ID
before continuing. Matching only symbol, quantity, or time is deliberately
insufficient. A pending cancel is also retained and checked without sending a
duplicate cancel. These situations are explicit failures in the workflow logs.

Each workflow logs start, attempt/recovery, result, order ID, and elapsed time.
HTTP logs contain the method, route, request ID, command ID, HTTP/API codes, and
milliseconds. Holdings logs link publication to its triggering order and report
the next interval. Tokens, signing material, request bodies, and raw backend
error messages are excluded. Use the existing `logs/runner.log` when started
through the command center.

## Signing and sessions

`gigamoney/signing.py` implements the mobile app's HMAC-SHA256 protocol, which
is distinct from public Longbridge OpenAPI. The key is the UTF-8 signing secret.
The base input is:

```text
lowercase_method|exact_path_and_query|timestamp|token=authorization_header
```

For a JSON body, recursively flatten to bracketed keys, sort complete keys
lexicographically, join raw `key=value` pairs with `&`, then append `|` and the
Base64 encoding of that UTF-8 string. Empty containers and nulls contribute no
pairs. Null handling is not needed by the captured order/authentication bodies.
Floating-point JSON values are unsupported; use decimal strings. The signature
is Base64 of the HMAC digest. Each request gets a new microsecond-resolution
millisecond timestamp and a new request ID.

Main and trade tokens renew when fewer than 60 seconds remain. Refreshes are
serialized across processes, and rotated credentials are atomically persisted
before the next operation. Main refresh uses `POST /auth/refresh_token`; trade
refresh uses `POST /member/trade/refresh_token`. Observed lifetimes are six hours /
30 days for main access/refresh and five minutes / 30 minutes for trade
access/refresh. JWT expiry is a scheduling hint; server success and returned
credentials are validated.

An expired or rejected trade refresh can now authenticate through
`POST /v3/member/trade/auth`. The saved private password digest is encrypted
freshly for each timestamp using the app's AES-256-CBC/PKCS7 convention: key =
ASCII MD5 hex of the request timestamp, IV = first 16 key bytes, plaintext =
ASCII MD5 hex of the configured trade password. The encrypted body is then
signed. Two successful captured authentication requests validated this encoding;
an actual rejected refresh followed by fresh authentication returned HTTP 200,
API code 0 on 2026-09-08.

Main-session expiry/revocation that cannot be refreshed, wrong passwords, or
server-requested MFA still require authentication in the app and a new bootstrap.
No unattended initial login or MFA flow is implemented. The repeatable `api-bootstrap import --from-emulator` command reads the already verified app session, binds it to the logical account and broker route, and preserves that device identity. Verification challenges record `work/api/authentication.json` and stop Kafka intake without committing an order blocked before submission. The command center shows Attention; restore/import the session and manually restart. Scheduled restarts preserve this state. A changed trade password
also requires updating `gigamoney.tradePassword` and reimporting the session. HTTP 402 / 402001 on a read immediately reauthenticates the trade session and retries that read once; repeated rejection requires human verification. Financial requests are never blindly replayed. There is no
background keepalive; scheduled holdings reads keep active sessions renewed.

The private `work/api/session.json` contains the signing secret, API key,
device/account headers, main/trade access and refresh pairs, allowlisted read
query templates, and optional `trade_auth` credentials. Its file must be mode
600 and directory mode 700. Bootstrap creation is a maintenance step using the
logged-in app and captures through the documented `api-bootstrap` command; phone verification stays in the app. All such files
are ignored by Git. Missing account routing is filled only from a unique active
account matching the saved account identifiers.

The diagnostic client remains read-only for financial operations:

```bash
.venv/bin/python scripts/api.py status
.venv/bin/python scripts/api.py read orders
.venv/bin/python scripts/api.py read trade_check
.venv/bin/python scripts/api.py refresh both
```

Use `--session /private/session.json` before the subcommand for another session.
Diagnostic `read` fetches one response using its bootstrap query; use the main
`holdings` / `cancel-all --dry-run` workflows for normalized holdings or complete
order pagination. Add `--data` only when you want to print private response data.
Upstream TLS verification stays enabled, redirects/ambient proxies are disabled,
and routes/hosts are allowlisted. The optional capture route is restricted to an
explicit local HTTP listener at `127.0.0.1`; it cannot select an external proxy.

## Validation

Offline regression coverage includes the original Android/Kafka behavior plus API order
construction, real signed HTTP serialization against an offline server double,
long-share checks, pagination, partial FOK, pending cancellation, fill/cancel
races, lost replies, durable deduplication, session renewal/authentication,
account routing, and API-only command-center restarts. Kafka integration tests
use fake consumer/producer and broker objects; they cannot reach live trading.

```bash
.venv/bin/python -m unittest discover -v
.venv/bin/gigamoney self-test
```

Enrollment tests also cover separate accounts/configuration roots, credential
and device matching, expired capture evidence, and verification challenges.
The emulator getter remains specific to the reviewed app build; each new host
needs its own verified import and signed account-read check.

For dated capture comparisons and user-driven validation, see the
[investigation notes](investigation.md). Order replacement was investigated but
is not a supported Kafka command.
