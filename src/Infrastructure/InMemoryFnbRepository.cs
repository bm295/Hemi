using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryFnbStore
{
    public RestaurantProfile Profile { get; } = new(
        "Hemi Steak & Seafood Grill",
        "Not specified",
        60,
        80);

    public List<DiningTable> Tables { get; } =
    [
        new(Guid.NewGuid(), "T01", 4, TableStatus.Available),
        new(Guid.NewGuid(), "T02", 4, TableStatus.Available),
        new(Guid.NewGuid(), "T03", 4, TableStatus.Reserved),
        new(Guid.NewGuid(), "T04", 4, TableStatus.Occupied),
        new(Guid.NewGuid(), "T05", 6, TableStatus.Available),
        new(Guid.NewGuid(), "T06", 6, TableStatus.Available),
        new(Guid.NewGuid(), "T07", 6, TableStatus.Reserved),
        new(Guid.NewGuid(), "T08", 8, TableStatus.Available),
        new(Guid.NewGuid(), "T09", 8, TableStatus.Occupied),
        new(Guid.NewGuid(), "T10", 8, TableStatus.Available),
        new(Guid.NewGuid(), "P01", 10, TableStatus.Available),
        new(Guid.NewGuid(), "P02", 10, TableStatus.Reserved)
    ];

    public List<MenuItem> MenuItems { get; } =
    [
        new(Guid.NewGuid(), "USDA Prime Ribeye", "Steak", 1350000m, true),
        new(Guid.NewGuid(), "Grilled Lobster Tail", "Seafood", 1490000m, true),
        new(Guid.NewGuid(), "Pan-Seared Salmon", "Seafood", 690000m, true),
        new(Guid.NewGuid(), "Wagyu Beef Carpaccio", "Starter", 480000m, true),
        new(Guid.NewGuid(), "Caesar Salad", "Starter", 260000m, true),
        new(Guid.NewGuid(), "Chocolate Lava Cake", "Dessert", 240000m, true),
        new(Guid.NewGuid(), "Seasonal Oyster Platter", "Seafood", 820000m, false)
    ];

    public List<ServiceOrder> Orders { get; } = [];

    public List<Reservation> Reservations { get; } =
    [
        new(Guid.NewGuid(), "Nguyen Minh Anh", 4, DateTimeOffset.UtcNow.AddHours(2), "0901234567", "Window-side table", ReservationStatus.Confirmed),
        new(Guid.NewGuid(), "Tran Hoang Long", 6, DateTimeOffset.UtcNow.AddHours(4), "0912345678", null, ReservationStatus.Pending)
    ];

    public List<Payment> Payments { get; } = [];
    public List<StockMovement> StockMovements { get; } = [];
    public Dictionary<Guid, OrderFulfillmentSagaState> SagaStatesByOrderId { get; } = [];
    public List<InventoryItem> Inventory { get; }

    public InMemoryFnbStore()
    {
        Inventory = MenuItems
            .Select(item => new InventoryItem(Guid.NewGuid(), item.Id, item.Name, 100, "portion"))
            .ToList();
    }
}

public sealed class InMemoryRestaurantAdapter(InMemoryFnbStore store) : IRestaurantQueryPort
{
    public Task<RestaurantProfile> GetRestaurantProfileAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(store.Profile);
}

public sealed class InMemoryTableAdapter(InMemoryFnbStore store) : ITableQueryPort
{
    public Task<IReadOnlyCollection<DiningTable>> GetTablesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<DiningTable>>(store.Tables.AsReadOnly());
}

public sealed class InMemoryMenuAdapter(InMemoryFnbStore store) : IMenuQueryPort
{
    public Task<IReadOnlyCollection<MenuItem>> GetMenuItemsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<MenuItem>>(store.MenuItems.AsReadOnly());
}

public sealed class InMemoryOrderAdapter(InMemoryFnbStore store) : IOrderQueryPort, IOrderCommandPort
{
    public Task<IReadOnlyCollection<ServiceOrder>> GetOrdersAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<ServiceOrder>>(store.Orders.AsReadOnly());

    public Task<ServiceOrder> AddOrderAsync(Guid tableId, IReadOnlyCollection<OrderLine> lines, CancellationToken cancellationToken = default)
    {
        var table = store.Tables.SingleOrDefault(x => x.Id == tableId)
            ?? throw new InvalidOperationException("Table not found.");

        if (table.Status is TableStatus.Available)
        {
            var tableIndex = store.Tables.IndexOf(table);
            store.Tables[tableIndex] = table with { Status = TableStatus.Occupied };
        }

        var order = new ServiceOrder(Guid.NewGuid(), tableId, DateTimeOffset.UtcNow, OrderStatus.Open, lines);
        store.Orders.Add(order);
        return Task.FromResult(order);
    }

    public Task<ServiceOrder> UpdateOrderLinesAsync(Guid orderId, IReadOnlyCollection<OrderLine> lines, CancellationToken cancellationToken = default)
    {
        var order = store.Orders.SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        var updatedOrder = order with { Lines = lines };
        store.Orders[store.Orders.IndexOf(order)] = updatedOrder;
        return Task.FromResult(updatedOrder);
    }

    public Task<ServiceOrder> UpdateOrderStatusAsync(Guid orderId, OrderStatus status, CancellationToken cancellationToken = default)
    {
        var order = store.Orders.SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        var updatedOrder = order with { Status = status };
        store.Orders[store.Orders.IndexOf(order)] = updatedOrder;

        if (status is OrderStatus.Completed)
        {
            var table = store.Tables.Single(x => x.Id == order.TableId);
            store.Tables[store.Tables.IndexOf(table)] = table with { Status = TableStatus.Available };
        }

        return Task.FromResult(updatedOrder);
    }
}

public sealed class InMemoryReservationAdapter(InMemoryFnbStore store) : IReservationQueryPort, IReservationCommandPort
{
    public Task<IReadOnlyCollection<Reservation>> GetReservationsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<Reservation>>(store.Reservations.AsReadOnly());

    public Task<Reservation> AddReservationAsync(string guestName, int partySize, DateTimeOffset reservedFor, string contactPhone, string? notes, CancellationToken cancellationToken = default)
    {
        var reservation = new Reservation(
            Guid.NewGuid(),
            guestName,
            partySize,
            reservedFor,
            contactPhone,
            notes,
            ReservationStatus.Pending);

        store.Reservations.Add(reservation);
        return Task.FromResult(reservation);
    }
}

public sealed class InMemoryPaymentAdapter(InMemoryFnbStore store) : IPaymentQueryPort, IPaymentCommandPort
{
    public Task<IReadOnlyCollection<Payment>> GetPaymentsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<Payment>>(store.Payments.AsReadOnly());

    public Task<Payment> AddPaymentAsync(Guid orderId, decimal amount, PaymentMethod paymentMethod, CancellationToken cancellationToken = default)
    {
        _ = store.Orders.SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        var payment = new Payment(
            Guid.NewGuid(),
            orderId,
            amount,
            paymentMethod,
            PaymentStatus.Settled,
            DateTimeOffset.UtcNow);

        store.Payments.Add(payment);
        return Task.FromResult(payment);
    }

    public Task<IReadOnlyCollection<Payment>> RefundPaymentsForOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var updated = new List<Payment>();

        for (var i = 0; i < store.Payments.Count; i++)
        {
            var payment = store.Payments[i];
            if (payment.OrderId != orderId || payment.Status is not PaymentStatus.Settled)
            {
                continue;
            }

            var refunded = payment with
            {
                Status = PaymentStatus.Refunded,
                PaidAt = DateTimeOffset.UtcNow
            };
            store.Payments[i] = refunded;
            updated.Add(refunded);
        }

        return Task.FromResult<IReadOnlyCollection<Payment>>(updated.AsReadOnly());
    }
}

public sealed class InMemoryInventoryAdapter(InMemoryFnbStore store) : IInventoryQueryPort, IInventoryCommandPort
{
    public Task<IReadOnlyCollection<InventoryItem>> GetInventoryAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<InventoryItem>>(store.Inventory.AsReadOnly());

    public Task<IReadOnlyCollection<StockMovement>> DeductInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var order = store.Orders.SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        var movements = new List<StockMovement>();

        foreach (var line in order.Lines)
        {
            var inventoryItem = store.Inventory.SingleOrDefault(x => x.MenuItemId == line.MenuItemId)
                ?? throw new InvalidOperationException("Inventory item mapping not found.");

            var remaining = inventoryItem.StockQuantity - line.Quantity;
            if (remaining < 0)
            {
                throw new InvalidOperationException($"Insufficient inventory for '{inventoryItem.Name}'.");
            }

            var movement = new StockMovement(
                Guid.NewGuid(),
                inventoryItem.Id,
                orderId,
                -line.Quantity,
                DateTimeOffset.UtcNow,
                "Order Closed");

            movements.Add(movement);
            store.StockMovements.Add(movement);

            store.Inventory[store.Inventory.IndexOf(inventoryItem)] = inventoryItem with { StockQuantity = remaining };
        }

        return Task.FromResult<IReadOnlyCollection<StockMovement>>(movements.AsReadOnly());
    }

    public Task<IReadOnlyCollection<StockMovement>> RestoreInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var deductedMovements = store.StockMovements
            .Where(x => x.OrderId == orderId && x.QuantityChanged < 0)
            .ToArray();

        var restorations = new List<StockMovement>();
        foreach (var movement in deductedMovements)
        {
            var inventoryItem = store.Inventory.Single(x => x.Id == movement.InventoryItemId);
            var restoredQuantity = inventoryItem.StockQuantity + Math.Abs(movement.QuantityChanged);

            store.Inventory[store.Inventory.IndexOf(inventoryItem)] = inventoryItem with { StockQuantity = restoredQuantity };

            var restoreMovement = new StockMovement(
                Guid.NewGuid(),
                movement.InventoryItemId,
                orderId,
                Math.Abs(movement.QuantityChanged),
                DateTimeOffset.UtcNow,
                "Saga Compensation: Inventory Restored");

            store.StockMovements.Add(restoreMovement);
            restorations.Add(restoreMovement);
        }

        return Task.FromResult<IReadOnlyCollection<StockMovement>>(restorations.AsReadOnly());
    }
}

public sealed class InMemorySagaStateAdapter(InMemoryFnbStore store) : ISagaStateQueryPort, ISagaStateCommandPort
{
    public Task<OrderFulfillmentSagaState?> GetOrderFulfillmentSagaAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        store.SagaStatesByOrderId.TryGetValue(orderId, out var sagaState);
        return Task.FromResult(sagaState);
    }

    public Task SaveOrderFulfillmentSagaAsync(OrderFulfillmentSagaState sagaState, CancellationToken cancellationToken = default)
    {
        store.SagaStatesByOrderId[sagaState.OrderId] = sagaState;
        return Task.CompletedTask;
    }
}
