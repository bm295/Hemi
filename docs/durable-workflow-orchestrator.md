# Durable Workflow Orchestrator

Order fulfillment is handled by the durable workflow orchestrator, not by the
legacy saga runner. The public fulfillment route keeps the historical
`/orders/{orderId}/fulfillment-saga` name for API compatibility, but the POST
path enqueues the `order-fulfillment` workflow and the worker executes the
durable workflow definition.

## Runtime Path

1. `POST /orders/{orderId}/fulfillment-saga` builds a `StartWorkflowCommand`.
   The generic `POST /workflows/` endpoint uses the same command path.
2. `WorkflowCommandQueue` validates the workflow registry, hashes the request,
   and persists or returns a `WorkflowInstance` through `IWorkflowInstanceStore`.
3. `WorkflowWorkerService` claims due workflow instances and dispatches them to
   `WorkflowDispatcher`.
4. `WorkflowEngine` runs `OrderFulfillmentWorkflow` steps:
   - `SendOrderToKitchenStep`
   - `CaptureOrderPaymentStep`
   - `DeductOrderInventoryStep`
   - `CloseOrderStep`
5. `SqlServerWorkflowJournal` persists state changes, step attempts, payload
   updates, and workflow events in one durable transition when SQL-backed
   persistence is active.
6. `OutboxWorkflowEventPublisher` stores workflow events in the workflow outbox;
   `WorkflowOutboxPublisher` claims and publishes those messages asynchronously.

`GET /orders/{orderId}/fulfillment-saga` first returns durable workflow status.
It falls back to `LegacyOrderFulfillmentSagaQueryService` only when no workflow
instance exists for that order. That fallback is for migration and read
compatibility with historical saga rows.

## Current Implementation Boundary

The application host uses SQL Server for both workflow state and the core FnB
side effects by default. `SqlServerFnbRepository` implements the restaurant,
table, menu, order, reservation, payment, and inventory ports against
`ConnectionStrings:DefaultConnection`. The in-memory FnB adapters are retained
for the `Testing` environment and for local demos when `Fnb:UseInMemory=true`.

With both SQL schemas applied, workflow instances, step attempts, worker
leases, outbox events, orders, payments, reservations, inventory, and stock
movements survive process restart. The FnB repository uses SQL transactions,
row locks where mutation ordering matters, and natural uniqueness constraints
for retry safety:

- `UX_FnbPayment_Order_Settled` prevents duplicate settled payments for an
  order.
- `UX_FnbStockMovement_Order_Inventory_Reason` prevents duplicate inventory
  deduction or restoration movements for the same order/item/reason.

The outbox itself is SQL-backed. `WorkflowOutboxPublisher` is the
lease-fenced SQL outbox dispatcher, not the broker adapter. Its final delivery
dependency is `IWorkflowMessagePublisher`, and the application host currently
binds that boundary to `InMemoryWorkflowMessagePublisher` so local demos can
run without a real broker. A production deployment should replace only the
`IWorkflowMessagePublisher` binding with a broker adapter while keeping the SQL
outbox store, `OutboxWorkflowEventPublisher`, `WorkflowOutboxPublisher`, and
hosted outbox publisher service in place.

## SQL Schema

The durable workflow schema lives in
`src/Infrastructure/WorkflowPersistence/Sql/WorkflowTables.sql`.

It creates and upgrades these tables:

- `WorkflowInstance`: durable workflow state, payload, optimistic version,
  idempotency metadata, retry scheduling, and worker lease fields.
- `WorkflowStepExecution`: per-step execution attempt history. `Attempt` is the
  step execution attempt for a given step order, not the worker attempt.
- `WorkflowOutboxMessage`: pending/published/failed workflow event messages with
  retry metadata and outbox lease fields.

The schema script is idempotent and uses `COL_LENGTH` / `sys.indexes` checks so
it can be applied repeatedly during deployment. Runtime configuration uses
`ConnectionStrings:DefaultConnection`; a deployment must apply the workflow
schema before starting SQL-backed workers.

SQL Server integration tests are intentionally gated behind
`HEMI_TEST_SQLSERVER_CONNECTION_STRING`. Without that variable, the tests are
skipped rather than creating or mutating a local database implicitly.

## FnB SQL Schema

The core restaurant schema lives in
`src/Infrastructure/FnbPersistence/Sql/FnbTables.sql`.

It creates and idempotently seeds:

- `FnbRestaurantProfile`: Hemi Steak & Seafood Grill profile and 60-80 seat
  capacity target.
- `FnbDiningTable`: 12 deterministic table rows totaling 78 seats.
- `FnbMenuItem`: menu catalog and availability flags.
- `FnbServiceOrder` and `FnbOrderLine`: order header and normalized line items.
- `FnbPayment`: settled/refunded payment records with a unique settled-payment
  guard per order.
- `FnbReservation`: upcoming reservation records.
- `FnbInventoryItem` and `FnbStockMovement`: inventory balances and idempotent
  order-related stock movements.

## Idempotency and Correlation

Workflow starts are durable and idempotent:

- `IdempotencyKey` has a unique filtered index.
- `RequestHash` records the submitted workflow request shape.
- Repeating the same idempotency key with the same request returns the existing
  workflow instance.
- Reusing the same idempotency key with a different request returns an
  idempotency conflict.
- `WorkflowId + CorrelationId` is unique, so an order cannot start multiple
  conflicting fulfillment workflows.

## Lease Recovery

Workflow workers do not poll and then update rows separately. They claim due
instances atomically with update locks, `READPAST`, and lease expiry:

- due work is selected by state and `NextAttemptAtUtc`;
- active leases are skipped;
- expired leases can be reclaimed by another worker;
- terminal state updates clear workflow leases.

Workflow leases are fencing tokens, not only selection hints. In the normal
SQL-backed runtime, journaled state, payload, step, and event commits require
the current lease owner. A worker that no longer owns the row cannot complete a
journal commit after another worker has reclaimed the workflow.

The direct `IWorkflowInstanceStore` state/payload mutation methods remain as a
fallback path. They are optimistic-versioned and accept an expected lease owner;
the current runtime passes the active lease owner when it falls back to those
methods. Production workflow execution should still prefer `IWorkflowJournal`
because it commits state, payload, step attempt, and event changes in one
version- and lease-fenced transition.

The outbox uses the same model. `WorkflowOutboxPublisher` claims pending
messages with a lease owner and lease expiry, publishes only claimed messages,
and clears leases when messages are published or transition to retry/failed
state. Outbox message completion is fenced by `LeaseOwner`, so stale publishers
cannot mark messages published or failed after ownership changes.

## Compensation

The workflow engine records step attempts and builds the compensation stack from
durably succeeded steps. If a later step fails, compensable steps run in reverse
order:

- kitchen send can be reopened;
- payment capture can be refunded;
- inventory deduction can be restored.

Compensation outcomes are persisted as step terminal states and workflow states
so recovery and status queries can distinguish successful, failed, compensated,
and compensation-failed executions.

## Outbox Model

Workflow lifecycle and step events are written to `WorkflowOutboxMessage` inside
the durable workflow path. Outbox messages include:

- destination and message type;
- payload and headers JSON;
- retry count, last attempt, next attempt, and terminal error;
- `LeaseOwner` and `LeaseUntilUtc` for concurrent publisher safety;
- `PublishedAtUtc` for terminal success.

The publisher claims eligible messages atomically, skips active leases, retries
transient failures, and marks exhausted messages as failed only while it still
owns the outbox lease. Broker-specific delivery code should live behind
`IWorkflowMessagePublisher`; it should not bypass the SQL outbox claim, retry,
or lease-fencing path.

## Observability

Workflow instrumentation is centralized in `WorkflowMetrics` and
`WorkflowTracing`. Metrics are emitted through the `Hemi.Workflows` meter for
workflow events, command handling, and message publishing outcomes. Tracing uses
the `Hemi.Workflows` activity source for workflow command handling and workflow
event publication.

## Recoverable Worker Dispatch Failures

Worker-level dispatch failures that happen before the engine reaches a terminal
workflow state are retried. The default worker retry policy allows three
dispatch attempts with a two-second delay. When the journal is available, the
worker persists the updated payload, `Pending` state, `NextAttemptAtUtc`,
cleared lease, and `workflow.retry_scheduled` event in one transition. After
retry exhaustion, it journals terminal `Failed` state with `workflow.failed`.

Terminal workflows and compensation terminal states are not reset to `Pending`
and are not rescheduled.

## Legacy Saga Boundary

Legacy saga types under `src/Application/Sagas/Legacy` and the old
`src/Infrastructure/Persistence/SagaCoreTables.sql` schema are migration and
fallback-only artifacts. They are not the active fulfillment execution path and
should not be used for new order-fulfillment runtime work.
