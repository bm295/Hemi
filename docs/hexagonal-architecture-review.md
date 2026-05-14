# Hemi Restaurant Management System - Hexagonal Architecture Review

## 1. Repository Overview

- The repository includes a .NET solution (`Hemi.sln`); the FnB implementation is the C# code under `src/`.
- The C# projects remain separated as:
  - `Hemi.Domain`
  - `Hemi.Application`
  - `Hemi.Infrastructure`
  - `Hemi.Presentation`
- Hexagonal boundaries are explicit in the application layer with:
  - `Ports/` for outbound port contracts
  - `UseCases/` for orchestration/use-case services
  - `Contracts/` for application DTOs
- Global build settings target `.NET 10` and `C# 14` through `Directory.Build.props`.
- API endpoints support profile/tables/menu, full order lifecycle, durable order fulfillment workflow orchestration, reservation operations, inventory view, sales reporting, and external food-app order intake.
- The active order-fulfillment runtime is the durable workflow orchestrator documented in `docs/durable-workflow-orchestrator.md`.

## 2. Architecture Evaluation

### Positive Findings

- Clear layer separation exists at project level (Domain, Application, Infrastructure, Presentation).
- Application defines granular ports by concern (restaurant/table/menu/order/reservation/inventory/payment).
- Infrastructure uses dedicated in-memory adapters per restaurant concern with a shared store, instead of a single overloaded repository.
- Durable workflow infrastructure exists for SQL-backed workflow instances, step attempt history, workflow journaling, worker leases, and outbox publishing.
- Presentation depends on application ports/services via DI and keeps endpoint handlers thin.

### Remaining Gaps vs Expected Target Architecture

- The repository still does not use the explicitly requested package/folder topology (`/src/Adapters`, `/src/Api`, etc.).
- Use-case logic is still centralized in one orchestration service (`FnbManagementService`) rather than distinct use-case handlers.
- Core restaurant data adapters are still in-memory; workflow orchestration has SQL-backed durable persistence.

## 3. Hexagonal Architecture Compliance

### What Is Compliant

- **Ports in Application**: ports are defined in `Hemi.Application` and are technology-agnostic.
- **Adapters in Infrastructure**: distinct adapters implement outbound concerns for orders, reservations, payments, inventory, tables, menu, and profile.
- **Durable workflow persistence**: SQL Server adapters implement workflow instance storage, execution logs, journaling, worker leases, and the workflow event outbox.
- **Framework isolation**: ASP.NET and DI remain in Presentation.
- **Inward dependency direction**: Presentation/Infrastructure depend on Application; Application depends on Domain.

### What Is Partially Compliant

- Explicit adapter package naming (`Adapters.Api`, `Adapters.Persistence`) is still not represented as dedicated projects.
- Domain remains mostly data-centric records rather than behavior-rich aggregates.

## 4. Domain Model Evaluation

- Domain concepts present: restaurant profile, tables, menu, service orders/order lines, reservations, payments, inventory/stock movements, and sales report.
- Seat-capacity suitability is represented (`60..80`) and seeded tables total 78 seats.
- Domain behavior is still limited:
  - Most transition rules live in application service or workflow steps.
  - Entities are mostly immutable records with minimal domain methods.

## 5. FnB Functionality Coverage

Required operational flow coverage:

1. **Create order for table** - implemented.
2. **Add/remove items** - implemented.
3. **Send order to kitchen** - implemented.
4. **Process payment** - implemented.
5. **Deduct inventory** - implemented at order closing and in the fulfillment workflow.
6. **Close order** - implemented with settled-payment guard.

Additional required capability:

- **Basic reporting** - implemented via sales report query endpoint.

Fulfillment hardening:

- **Durable orchestrator path**: `POST /orders/{orderId}/fulfillment-saga` enqueues the `order-fulfillment` workflow despite the compatibility-oriented route name.
- **SQL workflow state**: `WorkflowInstance`, `WorkflowStepExecution`, and `WorkflowOutboxMessage` track workflow state, step attempt history, and events.
- **SQL schema gating**: the idempotent workflow schema is in `src/Infrastructure/WorkflowPersistence/Sql/WorkflowTables.sql`; SQL integration tests require `HEMI_TEST_SQLSERVER_CONNECTION_STRING`.
- **Lease recovery**: workflow workers and outbox publishers claim due rows with SQL leases and allow expired leases to be reclaimed.
- **Idempotency**: idempotency keys, request hashes, and workflow/correlation uniqueness prevent duplicate or conflicting starts.
- **Compensation**: failed workflows compensate completed kitchen, payment, and inventory steps in reverse order.
- **Outbox**: workflow events are stored durably and published asynchronously from claimed outbox rows.
- **Legacy saga boundary**: legacy saga code is retained for migration/read fallback only and is not the active fulfillment runtime.

Hardening completed:

- Order creation now resolves prices server-side from menu catalog (client no longer supplies `UnitPrice`), reducing pricing-tampering risk.

## 6. Dependency Direction Analysis

Observed project references:

- `Application -> Domain`
- `Infrastructure -> Application`
- `Presentation -> Application + Infrastructure`
- `Domain` has no reference to outer layers.

This preserves inward dependency flow and keeps framework/infrastructure concerns out of the domain layer.

## 7. Code Quality Review

### Strengths

- DI registration is explicit and clear.
- Async signatures are used across ports/services/adapters/endpoints.
- Validation/guard clauses are present for critical flows.
- Endpoint business logic remains thin and delegated to application service or workflow orchestration.
- Workflow tests cover idempotent starts, lease claiming, workflow status, step attempt history, compensation, and outbox behavior.

### Risks / Quality Issues

- SQL Server integration tests are gated by `HEMI_TEST_SQLSERVER_CONNECTION_STRING`, so they do not run unless a test database is explicitly supplied.
- Core restaurant data adapters are still in-memory and not production-grade for concurrent restaurant operations.
- The orchestration service remains large and could be split for maintainability.

## 8. Identified Architectural Violations

1. **Strict structure mismatch**: expected explicit `Adapters`/`Api` segmentation is not yet mirrored in project names/layout.
2. **Anemic domain**: core lifecycle rules are still primarily outside domain entities.
3. **Partial production persistence**: workflow orchestration has SQL durability, but core restaurant data still uses in-memory adapters.

## 9. Recommended Refactoring

1. **Restructure projects by hexagonal roles explicitly**
   - Introduce dedicated API adapter and persistence adapter projects/folders.

2. **Split orchestration into use-case handlers**
   - `CreateOrder`, `AddOrderItem`, `SendOrderToKitchen`, `ProcessPayment`, `CloseOrder`, `GetSalesReport`, etc.

3. **Move critical invariants into domain behavior**
   - Add aggregate methods for valid transitions and invariant enforcement.

4. **Introduce production persistence and integration adapters**
   - Database-backed restaurant repositories and robust external gateway adapters.

5. **Keep expanding test suites**
   - Broaden SQL-backed workflow tests and add production persistence tests as restaurant adapters move beyond memory.

## 10. Overall Verdict

**PARTIAL** - The repository now addresses the key review findings around coarse adapter boundaries and client-driven pricing, and it adds a durable SQL-backed workflow orchestrator for order fulfillment. It still falls short of full strict hexagonal compliance due to missing explicit adapter packaging, an anemic domain model, and in-memory core restaurant data adapters.
