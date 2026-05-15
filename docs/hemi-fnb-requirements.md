# Hemi FnB Application Requirement Note

## Requested business context

- Restaurant: **Hemi Steak & Seafood Grill**
- Location: **Not specified**
- Expected service scale: **~60-80 seats**

## Technical requirements captured

1. Update this repository to implement an FnB management application for the restaurant context above.
2. Use **C# 14** and **.NET 10** as implementation baseline.
3. Preserve this requirement note inside the `docs/` folder.

## Current design baseline

- Layered/hexagonal C# solution with Domain, Application, Infrastructure, and Presentation projects.
- SQL Server is the default persistence path for FnB data and durable workflow state.
- In-memory FnB adapters are retained for tests and local demos through `Fnb:UseInMemory=true`.
- Order fulfillment uses the durable workflow orchestrator; the historical `/orders/{orderId}/fulfillment-saga` route is retained for API compatibility.

## PMP compliance note

This requirement note is intentionally concise and business-oriented; it does not enumerate all PMBOK 6 49 processes by itself.

For PMP-aligned process coverage, refer to:
- `docs/pmp-49-process-gap-analysis.md`
- `docs/pmp-49-process-framework-for-hemi-fnb.md`
