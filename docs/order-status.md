# API order-status reporting

The API runner publishes full order snapshots matching
[KTrader's v1 contract](https://github.com/mhliu94/KTrader/blob/2d6c9d3bdd0235c1a8801fa16a143588f4a1fe68/order_ui/README.md#kafka-publisher-contract).
The checked-in test schema is copied verbatim from that commit's
`order_ui/schema/order-status.v1.schema.json`.

## Refresh and change detection

Every API holdings query reads `/v5/orders/today?status=1` using the selected
account route and complete pagination. The explicit open-order filter replaces
the captured app's filters, so saved symbol tabs cannot hide other securities.
A diagnostic read without explicit parameters still uses the captured template.
Startup, periodic, and post-order/post-cancel runner refreshes
publish new orders and changed statuses, cumulative fills, average fill prices,
prices, quantities, or other mapped fields. Orders placed manually or by another
client are discovered while open. Only orders originally created today in the
configured trading timezone are tracked. `work/api/order-polling.json` persists
discovered nonterminal IDs and their latest snapshots. If a tracked order is
absent from the open list, an exact-ID detail read confirms its status; absence
alone never implies cancellation. Once a terminal status is confirmed, that ID
is removed from polling, including across restarts. Delivery receipts seed this
state during upgrades so previously completed orders are not re-queried.

Local submit and cancel confirmations retain their already-read snapshots, so
immediate fills and terminal failures can be reported without an additional
terminal-order lookup. Open-only discovery cannot find an external app/API order
that opens and finishes between polls, or later corrections to a retired order;
those require a broker event feed or an explicit history reconciliation.
Mere changes in the polling time do not republish orders.

Valid API order commands refused before submission also publish a terminal
`REJECTED` snapshot immediately, with zero fills and the refusal reason. The
runner saves this evidence in the execution journal before committing the Kafka
command. A definite HTTP client-error refusal of the submission is reported the
same way when the broker supplied no order ID. These source records use a stable
`local-rejection:<account-and-command hash>` ID and the original `command_id`;
their message distinguishes a local refusal from a broker submission refusal.
They do not invent a broker order ID or alter any existing broker order.

An acknowledgment loss, uncertain confirmation, or authentication challenge is
never converted into a rejection. A new command blocked by a previous unresolved
submission can report its own definite non-submission without closing the
original uncertain order. Rejected command IDs remain rejected on redelivery,
even if inventory later changes. Use a new command ID for a new order intent.

The default topic is `order-status`, configurable through `kafka.orderStatusTopic`.
It uses the same Kafka cluster, connection settings, idempotent producer, and
delivery timeout as holdings. Each message has the compact JSON array key
`[account_id,order_id]`, with string IDs and plain UTF-8 JSON values. Holdings
continue on `account-details` with their existing payload and key.

Only orders whose original creation date is today in `api.orderTimezone`
(`America/New_York` by default) are published. Align this with the Order UI's
timezone, including when using a non-default calendar date boundary.

## Broker mapping

| Broker field/state | KTrader field/state |
| --- | --- |
| Exact string `order_id_str`, or string `id` | `order_id`; never converted to a float |
| `submitted_at` epoch seconds | Immutable UTC `created_at` |
| Latest `updated_at`, `done_at`, or `order_histories[].time`, when present | UTC `updated_at`; otherwise the time this source observed the state |
| `counter_id` in `ST/market/symbol` form | `symbol` |
| `action` 1 / 2 | `BUY` / `SELL` |
| `order_type` MO / LO / other | `MARKET` / `LIMIT` / `OTHER` |
| `quantity`, `executed_qty`, `executed_price` | `quantity`, cumulative `filled_quantity`, `average_fill_price` |
| Unknown/zero quantity with positive `submitted_amount` | `quantity: null`, `notional` |
| Limit `price` | `limit_price` |
| `NotReported`, `ReplacedNotReported`, `ProtectedNotReported`, `VarietiesNotReported`, `WaitToNew` | `PENDING_NEW` |
| `NewStatus`, `WaitToReplace`, `PendingReplaceStatus`, `ReplacedStatus` | `NEW` |
| `PartialFilledStatus` | `PARTIALLY_FILLED` |
| `WaitToCancel`, `PendingCancelStatus` | `PENDING_CANCEL` |
| `FilledStatus` | `FILLED` |
| `CanceledStatus`, `PartialWithdrawal` | `CANCELED`, preserving any partial fills |
| `RejectedStatus` / `ExpiredStatus` / unmapped status | `REJECTED` / `EXPIRED` / `UNKNOWN` |

The broker status and available rejection/cancellation messages are included in
`message`. Known local submissions include their original journal `command_id`;
external orders do not inherit the command that triggered a holdings refresh.
The optional `account_num_id` comes from configuration and must match KTrader.
Missing creation times, invalid quantities/prices, unsupported securities/sides,
and inconsistent fills are logged and skipped without inventing order data.

## Publication records and recovery

`work/api/order-status/` contains private, atomic records indexed by a hash of
the account and exact order ID. Each keeps the last Kafka-confirmed full snapshot,
its sequence, acknowledgment time/partition/offset, and any pending snapshot.
This is the latest publication state for each order, not an append-only history;
Kafka retains the event history according to the topic policy.

New orders begin at sequence 0. Changed snapshots increment that order's
sequence. Pending snapshots are persisted before sending and marked published
only after a successful delivery callback and flush. A timeout, restart, or
crash after delivery retries the identical payload and sequence, then sends any
newer state with a higher sequence. `created_at` cannot change; `updated_at`
cannot decrease. Corrupt state is reported instead of resetting a sequence.

After restart, the runner compares orders with the last Kafka-acknowledged
snapshots on disk and skips unchanged orders, including completed orders.
Only a new order or a changed status, fill, or other mapped field is published;
the polling timestamp alone never triggers delivery. Pending messages for today
are retried even if an order is absent from the next broker response.
Prior-day/future-day messages are not replayed. Keep the publication directory
across restarts and account migrations; do not run a second publisher for the
same account with an independent directory. Dashboard reconstruction uses its
retained Kafka history rather than a publisher-wide replay on restart.

Refusals are retried from the execution journal on account refresh and restart,
using the same creation time, source ID, and per-order sequence. The runner also
retries these reports and its pending outbox when a holdings query fails, so
reporting a known refusal does not depend on broker/session availability.
App-entered and other external API orders continue to use their exact broker IDs
and are reported independently, without guessing a command correlation from
their symbol, quantity, or time.

An orders read or mapping failure is logged and counted in `orderStatusErrors`
in the CLI query result. Other valid orders and holdings can still be reported.
A Kafka order-delivery failure leaves the message pending and does not prevent
holdings delivery or replay a financial command. Authentication challenges retain
the existing behavior of stopping runner intake.

## KTrader server requirements

No KTrader code change is required for the current v1 consumer. Its schema and
sequence replacement rules already support these messages. Deployment must have:

- A provisioned topic matching `kafka.orderStatusTopic` and
  `order_ui.kafka.topic`, on the same Kafka cluster. The Order UI does not create
  topics; it disables automatic topic creation.
- At least one full trading day plus restart margin retained; 72 hours is the
  recommended minimum. A compacted topic is also compatible with the stable keys.
  Do not tombstone today's completed orders.
- Kafka write permission for this runner and read/group permission for Order UI.
- A visible shared account whose `string_id` equals `gigamoney.accountId` and
  whose numeric ID matches `gigamoney.accountNumId` when supplied.
- Matching `api.orderTimezone` and `order_ui.timezone`.

Order UI restarts reconstruct its view by replaying retained Kafka messages.
Its default port is 8030; it operates separately from this project's command
center and from KTrader's trading UI.

## Verification

Install development dependencies with `.venv/bin/pip install -e '.[dev]'`, then
run `.venv/bin/python -m pytest -q`. Tests use fake brokers and Kafka clients and
cover pagination, external/completed orders, fills, duplicate suppression,
KTrader schema validation, delivery failures, durable crash/restart replay,
creation-date/DST boundaries, immutable timestamps, and command correlation.

Operational logs include `Order status publication completed delivered=...`,
`Today's open orders query failed`, `Order snapshot mapping failed`, and
`Order publication failed`. An empty broker order list is valid and does not
require placing a live order to verify query connectivity.
