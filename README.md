# Hemi FnB Management Application

This repository now contains a layered C# application for **Hemi Steak & Seafood Grill**.

## Restaurant scope

- Venue: Hemi Steak & Seafood Grill
- Capacity target: approximately **60-80 seats**

## Tech stack

- **C# 14**
- **.NET 10** (`net10.0`)
- Layered architecture:
  - Presentation (Minimal API)
  - Application (use-case service + ports)
  - Infrastructure (SQL FnB adapters + in-memory test overrides + SQL workflow persistence)
  - Domain (entities and value objects)

## Features implemented

- Restaurant profile and seating capacity range
- Dining table management overview
- Menu management overview with availability filtering
- Full order lifecycle operations:
  - create order
  - add/remove items
  - send order to kitchen
  - process payment
  - deduct inventory on close
  - close order
- Durable workflow-based order fulfillment endpoint (kitchen -> payment -> inventory -> close) with compensation on failure
- SQL-backed workflow persistence for instances, step attempts, journaled transitions, and workflow outbox messages
- Idempotent workflow starts with correlation conflict protection
- Worker and outbox lease recovery for safe polling across multiple processes
- Reservation creation and upcoming reservation listing
- Inventory snapshot endpoint
- Basic sales report endpoint
- Food-app order integration endpoint
- SQL-backed restaurant profile, tables, menu, orders, payments, reservations, inventory, and stock movements

## Run

Apply the SQL schemas before running against SQL Server:

```bash
# Apply these SQL Server scripts to ConnectionStrings:DefaultConnection
src/Infrastructure/FnbPersistence/Sql/FnbTables.sql
src/Infrastructure/WorkflowPersistence/Sql/WorkflowTables.sql
```

```bash
dotnet run --project src/Presentation/Hemi.Presentation.csproj
```

FnB ports use `ConnectionStrings:DefaultConnection` by default. For local demos that should not touch SQL Server, set `Fnb:UseInMemory=true`; tests use the in-memory FnB adapters automatically.

The API starts locally and exposes endpoints such as:

- `GET /`
- `GET /tables`
- `GET /menu`
- `GET /orders/open`
- `POST /orders` (server resolves prices from menu catalog)
- `POST /orders/{orderId}/items`
- `DELETE /orders/{orderId}/items`
- `POST /orders/{orderId}/send-to-kitchen`
- `POST /orders/{orderId}/payments`
- `POST /orders/{orderId}/close`
- `POST /orders/{orderId}/fulfillment-saga` (routes to the `order-fulfillment` workflow engine)
- `GET /orders/{orderId}/fulfillment-saga` (returns durable workflow status, with legacy read fallback)
- `GET /inventory`
- `GET /reports/sales`
- `GET /reservations/upcoming`
- `POST /reservations`
- `POST /integrations/food-app/orders`

## Durable order fulfillment

The active fulfillment runtime is the durable workflow orchestrator:

- `POST /orders/{orderId}/fulfillment-saga` keeps the historical route name but enqueues the `order-fulfillment` workflow.
- `WorkflowWorkerService` atomically claims due `WorkflowInstance` rows using SQL leases, dispatches the workflow, and lets expired leases be recovered by another worker. Workflow leases are fencing tokens for the journaled runtime: state, payload, step, and event commits must match the current lease owner.
- `WorkflowEngine` records every step execution attempt, including internal retries, and persists compensation outcomes.
- `SqlServerWorkflowJournal` persists state, payload, step attempt, and outbox event changes together for durable workflow transitions.
- `OutboxWorkflowEventPublisher` writes lifecycle events to `WorkflowOutboxMessage`; `WorkflowOutboxPublisher` atomically claims outbox rows, publishes them, retries failures, and clears leases on terminal outcomes. Outbox leases are also fencing tokens, so stale publishers cannot complete messages after ownership changes.
- Non-terminal worker dispatch failures are recoverable: the worker schedules retries with `workflow.retry_scheduled`, clears the workflow lease, and marks the workflow failed through the journal after dispatch retry exhaustion.

The SQL schema for this path is `src/Infrastructure/WorkflowPersistence/Sql/WorkflowTables.sql`. It is idempotent and should be applied before running SQL-backed workers. SQL Server integration tests are gated by `HEMI_TEST_SQLSERVER_CONNECTION_STRING`.

Legacy saga code and `src/Infrastructure/Persistence/SagaCoreTables.sql` are migration/fallback-only. `GET /orders/{orderId}/fulfillment-saga` reads legacy saga state only when no durable workflow instance exists for the order.

Current durability boundary:

- Workflow state, step attempts, workflow event outbox messages, worker leases, and outbox leases are SQL-backed.
- Core restaurant ports for tables, menu, orders, payments, reservations, and inventory are SQL-backed in the application host. `src/Infrastructure/FnbPersistence/Sql/FnbTables.sql` creates and idempotently seeds the restaurant profile, tables, menu, inventory, and sample reservations with deterministic IDs.
- Payment creation uses the order's settled-payment natural key, enforced by `UX_FnbPayment_Order_Settled`. Inventory deduction/restoration uses order + inventory item + reason, enforced by `UX_FnbStockMovement_Order_Inventory_Reason`, so retries return existing side effects instead of duplicating them.
- `IWorkflowMessagePublisher` is currently bound to an in-memory publisher. The SQL outbox is durable, but final message delivery is a local/demo transport until a production broker adapter is supplied.

More detail: `docs/durable-workflow-orchestrator.md`.
