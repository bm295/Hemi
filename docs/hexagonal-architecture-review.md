# Hemi Restaurant Management System - Hexagonal Architecture Review

## 1. Repository Overview

- The repository is a .NET solution (`Hemi.sln`) with C# implementation under
  `src/`.
- Global build settings target `.NET 10` and `C# 14` through
  `Directory.Build.props`, with nullable reference types and warnings as errors.
- The project boundaries are:
  - `Hemi.Domain`: restaurant and workflow domain records/enums.
  - `Hemi.Application`: use-case orchestration, outbound ports, workflow
    abstractions, workflow runtime, definitions, policies, and registry.
  - `Hemi.Infrastructure`: SQL Server FnB persistence, SQL workflow
    persistence/journal/outbox, in-memory test adapters, messaging, and workflow
    monitoring.
  - `Hemi.Presentation`: Minimal API endpoints, DI composition, workflow worker,
    outbox publisher service, and command queue.
- The active order-fulfillment runtime is the durable workflow orchestrator
  documented in `docs/durable-workflow-orchestrator.md`.

## 2. Current Architecture Pattern

The codebase is a layered hexagonal architecture with pragmatic project names:

- Inbound adapters live in Presentation:
  Minimal API routes, `/workflows` endpoints, `/orders/{orderId}/fulfillment-saga`
  compatibility route, `WorkflowWorkerService`, and
  `WorkflowOutboxPublisherService`.
- Application owns business orchestration:
  `FnbManagementService`, outbound port contracts, workflow execution,
  workflow registry, retry policies, workflow DTOs, and workflow definitions.
- Infrastructure owns adapter implementations:
  `SqlServerFnbRepository`, SQL workflow repositories, `SqlServerWorkflowJournal`,
  outbox publishing, in-memory FnB test adapters, and workflow metrics/tracing.
- Domain remains framework-free and contains the restaurant/workflow data model.

Runtime composition in `Program.cs` now uses SQL Server by default for the FnB
ports. In-memory FnB adapters are selected only in the `Testing` environment or
when `Fnb:UseInMemory=true`.

## 3. Persistence Design

Core restaurant state is SQL-backed through
`src/Infrastructure/FnbPersistence/Sql/FnbTables.sql` and
`SqlServerFnbRepository`:

- restaurant profile, dining tables, menu items, orders, order lines, payments,
  reservations, inventory items, and stock movements are persisted;
- seeded data is deterministic, including 12 tables totaling 78 seats;
- order creation, line updates, payment, inventory deduction/restoration, and
  status updates use SQL transactions;
- settled payment and stock movement natural keys make retrying those side
  effects idempotent.

Workflow state is SQL-backed through
`src/Infrastructure/WorkflowPersistence/Sql/WorkflowTables.sql`:

- `WorkflowInstance` stores state, payload, idempotency metadata, retry
  scheduling, and worker leases;
- `WorkflowStepExecution` stores per-step execution attempts and compensation
  outcomes;
- `WorkflowOutboxMessage` stores lifecycle events with retry and outbox lease
  fields.

The remaining external integration boundary is final event delivery:
`IWorkflowMessagePublisher` is currently bound to
`InMemoryWorkflowMessagePublisher`. Production should replace that binding with
a broker adapter while retaining the SQL outbox and lease-fenced publisher.

## 4. Hexagonal Compliance

### Compliant

- Application defines technology-agnostic outbound ports for restaurant, table,
  menu, order, reservation, payment, inventory, saga query, workflow instance,
  workflow log, journal, outbox, event publishing, dispatching, registry, and
  retry policy concerns.
- Infrastructure implements the outbound adapters and depends inward on
  Application.
- Presentation depends on Application and Infrastructure for composition and
  keeps endpoint handlers thin.
- Domain has no dependency on outer layers.
- SQL workflow commits are lease-fenced through `IWorkflowJournal`; fallback
  instance-store state/payload updates are optimistic-versioned and can require
  the active lease owner.

### Partially Compliant

- The folder/project names do not use a strict `Adapters.Api` /
  `Adapters.Persistence` topology.
- `FnbManagementService` is still a broad orchestration service rather than a
  set of focused use-case handlers.
- `SqlServerFnbRepository` implements many ports in one class. This is
  acceptable at the current size, but it should be split by concern if it grows.
- Domain objects are still mostly data-centric records; many lifecycle rules
  live in the application service and workflow steps.
- Final workflow message delivery is not production-integrated until a broker
  adapter replaces the in-memory publisher.

## 5. Functionality Coverage

Required operational flow coverage:

1. Create order for table - implemented with server-side price resolution.
2. Add/remove items - implemented.
3. Send order to kitchen - implemented.
4. Process payment - implemented with retry-safe settled-payment persistence.
5. Deduct inventory - implemented at order closing and in the fulfillment
   workflow with retry-safe stock movements.
6. Close order - implemented with settled-payment guard and table release.
7. Basic reporting - implemented through the sales report endpoint.

Additional capabilities:

- reservation creation and upcoming reservation listing;
- inventory snapshot endpoint;
- food-app order intake endpoint;
- generic workflow list/start/status endpoints under `/workflows`;
- durable order fulfillment through the compatibility route
  `/orders/{orderId}/fulfillment-saga`.

## 6. Workflow Design

The order fulfillment workflow runs these steps:

1. `SendOrderToKitchenStep`
2. `CaptureOrderPaymentStep`
3. `DeductOrderInventoryStep`
4. `CloseOrderStep`

Current hardening includes:

- idempotent workflow starts with `IdempotencyKey`, `RequestHash`, and
  `WorkflowId + CorrelationId` uniqueness;
- worker lease recovery for due workflow instances;
- journaled state, payload, step attempt, and outbox event transitions;
- reverse-order compensation for completed kitchen, payment, and inventory
  steps;
- recoverable dispatch failures with retry scheduling and terminal failure
  after retry exhaustion;
- SQL outbox claiming, retry, failed/published terminal states, and lease
  fencing;
- workflow metrics and tracing through the `Hemi.Workflows` meter/activity
  source.

Legacy saga artifacts under `src/Application/Sagas/Legacy` and
`src/Infrastructure/Persistence/SagaCoreTables.sql` are migration/read-fallback
only. They are not the active execution runtime.

## 7. Dependency Direction

Observed project references:

- `Application -> Domain`
- `Infrastructure -> Application`
- `Presentation -> Application + Infrastructure`
- `Domain` has no project references.

This preserves inward dependency flow and keeps ASP.NET, SQL Server, messaging,
and hosting concerns outside the domain and application contracts.

## 8. Risks and Recommendations

1. Add a broker-backed `IWorkflowMessagePublisher` for production event delivery.
2. Split `FnbManagementService` into focused use-case handlers as the restaurant
   surface area grows.
3. Split `SqlServerFnbRepository` by adapter concern if persistence logic
   continues to expand.
4. Move high-value lifecycle invariants into domain behavior where that reduces
   duplication or ambiguity.
5. Decide whether to keep the disabled root discovery endpoint or remove the
   commented code entirely.
6. Continue running SQL Server integration tests with
   `HEMI_TEST_SQLSERVER_CONNECTION_STRING`, since those tests cover the
   production persistence paths that unit tests skip.
7. Keep legacy saga support read-only and remove it after migration needs expire.

## 9. Overall Verdict

**SUBSTANTIALLY ALIGNED** - The current codebase follows the intended
layered/hexagonal direction and now has SQL-backed durability for both workflow
state and core restaurant side effects. The main architecture gaps are no
broker-backed final event delivery, broad application/repository classes that
will need splitting as complexity grows, and a mostly data-centric domain model.
