# Hemi FnB Management Application

This repository now contains a layered C# application for **Hemi Steak & Seafood Grill**.

## Restaurant scope

- Venue: Hemi Steak & Seafood Grill
- Capacity target: approximately **60–80 seats**

## Tech stack

- **C# 14**
- **.NET 10** (`net10.0`)
- Layered architecture:
  - Presentation (Minimal API)
  - Application (use-case service + ports)
  - Infrastructure (in-memory adapters)
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
- Saga-based orchestration endpoint to fulfill an order (kitchen -> payment -> inventory -> close) with compensation on failure
- Saga persistence model on SQL Server with 4 tables: `SagaInstance`, `SagaStep`, `OutboxMessage`, and `InboxMessage` (schema file at `src/Infrastructure/Persistence/SagaCoreTables.sql`)
- Reservation creation and upcoming reservation listing
- Inventory snapshot endpoint
- Basic sales report endpoint
- Food-app order integration endpoint

## Run

```bash
dotnet run --project src/Presentation/Hemi.Presentation.csproj
```

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
- `POST /orders/{orderId}/fulfillment-saga`
- `GET /inventory`
- `GET /reports/sales`
- `GET /reservations/upcoming`
- `POST /reservations`
- `POST /integrations/food-app/orders`
