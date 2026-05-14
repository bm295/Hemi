# Durable Workflow Orchestrator

Order fulfillment is handled by the durable workflow orchestrator, not by the
legacy saga runner. The public fulfillment route keeps the historical
`/orders/{orderId}/fulfillment-saga` name for API compatibility, but the POST
path enqueues the `order-fulfillment` workflow and the worker executes the
durable workflow definition.

## Runtime Path

1. `POST /orders/{orderId}/fulfillment-saga` builds a `StartWorkflowCommand`.
2. `WorkflowCommandQueue` persists or returns a `WorkflowInstance` through
   `IWorkflowInstanceStore`.
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

The outbox uses the same model. `WorkflowOutboxPublisher` claims pending
messages with a lease owner and lease expiry, publishes only claimed messages,
and clears leases when messages are published or transition to retry/failed
state.

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
transient failures, and marks exhausted messages as failed.

## Legacy Saga Boundary

Legacy saga types under `src/Application/Sagas/Legacy` and the old
`src/Infrastructure/Persistence/SagaCoreTables.sql` schema are migration and
fallback-only artifacts. They are not the active fulfillment execution path and
should not be used for new order-fulfillment runtime work.
