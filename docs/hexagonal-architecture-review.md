# Hemi Restaurant Management System — Hexagonal Architecture Review

## 1. Repository overview

- The repository includes a .NET solution (`Hemi.sln`); the FnB implementation is the C# code under `src/`.
- The C# projects remain separated as:
  - `Hemi.Domain`
  - `Hemi.Application`
  - `Hemi.Infrastructure`
  - `Hemi.Presentation`
- Hexagonal boundaries are now explicit in the application layer with:
  - `Ports/` for outbound port contracts
  - `UseCases/` for orchestration/use-case services
  - `Contracts/` for application DTOs
- Global build settings target `.NET 10` and `C# 14` through `Directory.Build.props`.
- API endpoints support profile/tables/menu, full order lifecycle, reservation operations, inventory view, sales reporting, and external food-app order intake.

## 2. Architecture evaluation

### Positive findings

- Clear layer separation exists at project level (Domain, Application, Infrastructure, Presentation).
- Application defines granular ports by concern (restaurant/table/menu/order/reservation/inventory/payment).
- Infrastructure now uses dedicated in-memory adapters per concern with a shared store, instead of a single overloaded repository.
- Presentation depends on application ports/services via DI and keeps endpoint handlers thin.

### Remaining gaps vs expected target architecture

- The repository still does not use the explicitly requested package/folder topology (`/src/Adapters`, `/src/Api`, etc.).
- Use-case logic is still centralized in one orchestration service (`FnbManagementService`) rather than distinct use-case handlers.
- Infrastructure is still in-memory only (no database-backed adapters).

## 3. Hexagonal architecture compliance

### What is compliant

- **Ports in Application**: ports are defined in `Hemi.Application` and are technology-agnostic.
- **Adapters in Infrastructure**: distinct adapters implement outbound concerns for orders, reservations, payments, inventory, tables, menu, and profile.
- **Framework isolation**: ASP.NET and DI remain in Presentation.
- **Inward dependency direction**: Presentation/Infrastructure depend on Application; Application depends on Domain.

### What is partially compliant

- Explicit adapter package naming (`Adapters.Api`, `Adapters.Persistence`) is still not represented as dedicated projects.
- Domain remains mostly data-centric records rather than behavior-rich aggregates.

## 4. Domain model evaluation

- Domain concepts present: restaurant profile, tables, menu, service orders/order lines, reservations, payments, inventory/stock movements, and sales report.
- Seat-capacity suitability is represented (`60..80`) and seeded tables total 78 seats.
- Domain behavior is still limited:
  - Most transition rules live in application service.
  - Entities are mostly immutable records with minimal domain methods.

## 5. FnB functionality coverage

Required operational flow coverage:

1. **Create order for table** — implemented.
2. **Add/remove items** — implemented.
3. **Send order to kitchen** — implemented.
4. **Process payment** — implemented.
5. **Deduct inventory** — implemented at order closing.
6. **Close order** — implemented with settled-payment guard.

Additional required capability:

- **Basic reporting** — implemented via sales report query endpoint.

Hardening completed:

- Order creation now resolves prices server-side from menu catalog (client no longer supplies `UnitPrice`), reducing pricing-tampering risk.

## 6. Dependency direction analysis

Observed project references:

- `Application -> Domain`
- `Infrastructure -> Application`
- `Presentation -> Application + Infrastructure`
- `Domain` has no reference to outer layers.

This preserves inward dependency flow and keeps framework/infrastructure concerns out of the domain layer.

## 7. Code quality review

### Strengths

- DI registration is explicit and clear.
- Async signatures are used across ports/services/adapters/endpoints.
- Validation/guard clauses are present for critical flows.
- Endpoint business logic remains thin and delegated to application service.

### Risks / quality issues

- No automated test project is present.
- In-memory adapters are non-durable and not production-grade for concurrent restaurant operations.
- The orchestration service remains large and could be split for maintainability.

## 8. Identified architectural violations

1. **Strict structure mismatch**: expected explicit `Adapters`/`Api` segmentation is not yet mirrored in project names/layout.
2. **Anemic domain**: core lifecycle rules are still primarily outside domain entities.
3. **No production persistence**: infrastructure has no database or resilient external integration implementation.
4. **Testing gap**: no unit/integration test coverage for core business flows.

## 9. Recommended refactoring

1. **Restructure projects by hexagonal roles explicitly**
   - Introduce dedicated API adapter and persistence adapter projects/folders.

2. **Split orchestration into use-case handlers**
   - `CreateOrder`, `AddOrderItem`, `SendOrderToKitchen`, `ProcessPayment`, `CloseOrder`, `GetSalesReport`, etc.

3. **Move critical invariants into domain behavior**
   - Add aggregate methods for valid transitions and invariant enforcement.

4. **Introduce production persistence and integration adapters**
   - Database-backed repositories and robust external gateway adapters.

5. **Add test suites**
   - Domain invariant tests, application use-case tests, and API integration tests.

## 10. Overall verdict

**PARTIAL** — The repository now addresses the key review findings around coarse adapter boundaries and client-driven pricing, and it provides strong layered architecture with DI/async and complete core FnB flow coverage. It still falls short of full strict hexagonal compliance due to missing explicit adapter packaging, an anemic domain model, and absence of production-grade persistence/testing.
