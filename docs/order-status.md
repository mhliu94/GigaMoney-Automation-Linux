# API order-status reporting

The API runner publishes full order snapshots matching
[KTrader's v1 contract](https://github.com/mhliu94/KTrader/blob/2d6c9d3bdd0235c1a8801fa16a143588f4a1fe68/order_ui/README.md#kafka-publisher-contract).
The checked-in test schema is copied verbatim from that commit's
`order_ui/schema/order-status.v1.schema.json`.

## Refresh and change detection

Every API holdings query reads all pages of `/v5/orders/today` using the selected
account route. These explicit queries replace the captured app's query parameters
so saved status or symbol filters cannot hide orders. A diagnostic read without
explicit parameters still uses the captured template.
Startup, periodic, and post-order/post-cancel runner refreshes
publish new orders and changed statuses, cumulative fills, average fill prices,
prices, quantities, or other mapped fields. Orders placed manually or by another
client are included, regardless of the local submission allowlist. Completed
orders remain eligible. Mere changes in the polling time do not republish orders.
Disappearing orders are not interpreted as cancellations.

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

After restart, the runner republishes each unchanged order once when observed,
including completed orders, using the existing sequence and payload. Pending
messages for today are retried even if an order is absent from the next broker
response. Prior-day/future-day messages are not replayed. Keep the publication
directory across restarts and account migrations; do not run a second publisher
for the same account with an independent directory.

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
`Today's orders query failed`, `Order snapshot mapping failed`, and
`Order publication failed`. An empty broker order list is valid and does not
require placing a live order to verify query connectivity.
