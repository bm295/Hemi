using Hemi.Domain;

namespace Hemi.Application;

public sealed class FnbManagementService(
    IRestaurantQueryPort restaurantQueryPort,
    ITableQueryPort tableQueryPort,
    IMenuQueryPort menuQueryPort,
    IOrderQueryPort orderQueryPort,
    IOrderCommandPort orderCommandPort,
    IReservationQueryPort reservationQueryPort,
    IReservationCommandPort reservationCommandPort,
    IInventoryQueryPort inventoryQueryPort,
    IInventoryCommandPort inventoryCommandPort,
    IPaymentQueryPort paymentQueryPort,
    IPaymentCommandPort paymentCommandPort,
    OrderFulfillmentSagaOrchestrator orderFulfillmentSagaOrchestrator)
{
    public Task<RestaurantProfile> GetProfileAsync(CancellationToken cancellationToken = default) =>
        restaurantQueryPort.GetRestaurantProfileAsync(cancellationToken);

    public Task<IReadOnlyCollection<DiningTable>> GetTablesAsync(CancellationToken cancellationToken = default) =>
        tableQueryPort.GetTablesAsync(cancellationToken);

    public async Task<IReadOnlyCollection<MenuItem>> GetAvailableMenuItemsAsync(CancellationToken cancellationToken = default) =>
        (await menuQueryPort.GetMenuItemsAsync(cancellationToken))
        .Where(item => item.IsAvailable)
        .OrderBy(item => item.Category, StringComparer.Ordinal)
        .ThenBy(item => item.Name, StringComparer.Ordinal)
        .ToArray();

    public async Task<IReadOnlyCollection<ServiceOrder>> GetOpenOrdersAsync(CancellationToken cancellationToken = default) =>
        (await orderQueryPort.GetOrdersAsync(cancellationToken))
        .Where(order => order.Status is OrderStatus.Open or OrderStatus.SentToKitchen)
        .OrderByDescending(order => order.CreatedAt)
        .ToArray();

    public async Task<IReadOnlyCollection<Reservation>> GetUpcomingReservationsAsync(DateTimeOffset now, CancellationToken cancellationToken = default) =>
        (await reservationQueryPort.GetReservationsAsync(cancellationToken))
        .Where(reservation => reservation.ReservedFor >= now && reservation.Status is not ReservationStatus.Cancelled)
        .OrderBy(reservation => reservation.ReservedFor)
        .ToArray();

    public Task<IReadOnlyCollection<InventoryItem>> GetInventoryAsync(CancellationToken cancellationToken = default) =>
        inventoryQueryPort.GetInventoryAsync(cancellationToken);

    public Task<OrderFulfillmentSagaState> ExecuteOrderFulfillmentSagaAsync(Guid orderId, PaymentMethod paymentMethod, decimal? paymentAmount = null, CancellationToken cancellationToken = default) =>
        orderFulfillmentSagaOrchestrator.ExecuteAsync(orderId, paymentMethod, paymentAmount, cancellationToken);

    public Task<OrderFulfillmentSagaState?> GetOrderFulfillmentSagaStateAsync(Guid orderId, CancellationToken cancellationToken = default) =>
        orderFulfillmentSagaOrchestrator.GetSagaStateAsync(orderId, cancellationToken);

    public async Task<SalesReport> GetSalesReportAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken cancellationToken = default)
    {
        var payments = await paymentQueryPort.GetPaymentsAsync(cancellationToken);
        var orders = await orderQueryPort.GetOrdersAsync(cancellationToken);

        var successfulPayments = payments
            .Where(x => x.Status is PaymentStatus.Settled && x.PaidAt >= from && x.PaidAt <= to)
            .ToArray();

        var totalRevenue = successfulPayments.Sum(x => x.Amount);
        var paidOrderIds = successfulPayments.Select(x => x.OrderId).Distinct().ToHashSet();
        var closedOrders = orders.Count(x => x.Status is OrderStatus.Completed && paidOrderIds.Contains(x.Id));

        return new SalesReport(from, to, totalRevenue, successfulPayments.Length, closedOrders);
    }

    public async Task<ServiceOrder> CreateOrderAsync(Guid tableId, IReadOnlyCollection<CreateOrderLineInput> items, CancellationToken cancellationToken = default)
    {
        ValidateOrderItems(items);

        var lines = await BuildOrderLinesFromMenuAsync(items, cancellationToken);
        return await orderCommandPort.AddOrderAsync(tableId, lines, cancellationToken);
    }

    public async Task<ServiceOrder> AddOrderItemAsync(Guid orderId, Guid menuItemId, int quantity, CancellationToken cancellationToken = default)
    {
        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), "Quantity must be greater than zero.");
        }

        var order = await GetOrderByIdAsync(orderId, cancellationToken);
        EnsureOrderEditable(order);

        var menuItem = await GetAvailableMenuItemAsync(menuItemId, cancellationToken);

        var lines = order.Lines.ToList();
        var existingLine = lines.SingleOrDefault(x => x.MenuItemId == menuItemId);

        if (existingLine is null)
        {
            lines.Add(new OrderLine(menuItem.Id, quantity, menuItem.Price));
        }
        else
        {
            var index = lines.IndexOf(existingLine);
            lines[index] = existingLine with { Quantity = existingLine.Quantity + quantity };
        }

        return await orderCommandPort.UpdateOrderLinesAsync(orderId, lines, cancellationToken);
    }

    public async Task<ServiceOrder> RemoveOrderItemAsync(Guid orderId, Guid menuItemId, int quantity, CancellationToken cancellationToken = default)
    {
        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), "Quantity must be greater than zero.");
        }

        var order = await GetOrderByIdAsync(orderId, cancellationToken);
        EnsureOrderEditable(order);

        var lines = order.Lines.ToList();
        var existingLine = lines.SingleOrDefault(x => x.MenuItemId == menuItemId)
            ?? throw new InvalidOperationException("Order line not found.");

        var remainingQuantity = existingLine.Quantity - quantity;
        if (remainingQuantity < 0)
        {
            throw new InvalidOperationException("Cannot remove more items than currently in the order.");
        }

        lines.Remove(existingLine);
        if (remainingQuantity > 0)
        {
            lines.Add(existingLine with { Quantity = remainingQuantity });
        }

        ValidateOrderLines(lines);
        return await orderCommandPort.UpdateOrderLinesAsync(orderId, lines, cancellationToken);
    }

    public async Task<ServiceOrder> SendOrderToKitchenAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var order = await GetOrderByIdAsync(orderId, cancellationToken);
        if (order.Status is not OrderStatus.Open)
        {
            throw new InvalidOperationException("Only open orders can be sent to kitchen.");
        }

        return await orderCommandPort.UpdateOrderStatusAsync(orderId, OrderStatus.SentToKitchen, cancellationToken);
    }

    public async Task<Payment> ProcessPaymentAsync(Guid orderId, decimal amount, PaymentMethod paymentMethod, CancellationToken cancellationToken = default)
    {
        if (amount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(amount), "Payment amount must be greater than zero.");
        }

        var order = await GetOrderByIdAsync(orderId, cancellationToken);
        if (order.Status is not (OrderStatus.SentToKitchen or OrderStatus.Open))
        {
            throw new InvalidOperationException("Only active orders can be paid.");
        }

        if (amount < order.TotalAmount)
        {
            throw new InvalidOperationException("Payment amount is less than order total.");
        }

        return await paymentCommandPort.AddPaymentAsync(orderId, amount, paymentMethod, cancellationToken);
    }

    public async Task<ServiceOrder> CloseOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        _ = await GetOrderByIdAsync(orderId, cancellationToken);

        var payments = await paymentQueryPort.GetPaymentsAsync(cancellationToken);
        var hasSettledPayment = payments.Any(x => x.OrderId == orderId && x.Status is PaymentStatus.Settled);

        if (!hasSettledPayment)
        {
            throw new InvalidOperationException("Order cannot be closed before successful payment.");
        }

        await inventoryCommandPort.DeductInventoryForOrderAsync(orderId, cancellationToken);
        return await orderCommandPort.UpdateOrderStatusAsync(orderId, OrderStatus.Completed, cancellationToken);
    }

    public Task<Reservation> CreateReservationAsync(string guestName, int partySize, DateTimeOffset reservedFor, string contactPhone, string? notes, CancellationToken cancellationToken = default)
    {
        if (partySize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partySize), "Party size must be greater than zero.");
        }

        return reservationCommandPort.AddReservationAsync(guestName, partySize, reservedFor, contactPhone, notes, cancellationToken);
    }

    public async Task<FoodAppIntegrationResult> IntegrateFoodAppOrderAsync(FoodAppOrderRequest request, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(request.SourceApp))
        {
            throw new ArgumentException("Source app is required.", nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.ExternalOrderId))
        {
            throw new ArgumentException("External order id is required.", nameof(request));
        }

        if (request.Items.Count == 0)
        {
            throw new ArgumentException("At least one food app item is required.", nameof(request));
        }

        var table = (await tableQueryPort.GetTablesAsync(cancellationToken))
            .SingleOrDefault(x => string.Equals(x.Code, request.TableCode, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Table code '{request.TableCode}' was not found.");

        var lines = await BuildOrderLinesFromMenuAsync(
            request.Items.Select(x => new CreateOrderLineInput(x.MenuItemId, x.Quantity)).ToArray(),
            cancellationToken);

        var order = await orderCommandPort.AddOrderAsync(table.Id, lines, cancellationToken);

        return new FoodAppIntegrationResult(
            request.SourceApp,
            request.ExternalOrderId,
            order.Id,
            table.Code,
            order.CreatedAt,
            order.TotalAmount);
    }

    private async Task<IReadOnlyCollection<OrderLine>> BuildOrderLinesFromMenuAsync(IReadOnlyCollection<CreateOrderLineInput> items, CancellationToken cancellationToken)
    {
        var menuItemsById = (await menuQueryPort.GetMenuItemsAsync(cancellationToken)).ToDictionary(item => item.Id, item => item);

        return items
            .Select(item =>
            {
                if (!menuItemsById.TryGetValue(item.MenuItemId, out var menuItem))
                {
                    throw new InvalidOperationException($"Menu item '{item.MenuItemId}' was not found.");
                }

                if (!menuItem.IsAvailable)
                {
                    throw new InvalidOperationException($"Menu item '{menuItem.Name}' is not currently available.");
                }

                return new OrderLine(menuItem.Id, item.Quantity, menuItem.Price);
            })
            .ToArray();
    }

    private async Task<ServiceOrder> GetOrderByIdAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var order = (await orderQueryPort.GetOrdersAsync(cancellationToken)).SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        return order;
    }

    private async Task<MenuItem> GetAvailableMenuItemAsync(Guid menuItemId, CancellationToken cancellationToken)
    {
        var menuItem = (await menuQueryPort.GetMenuItemsAsync(cancellationToken)).SingleOrDefault(x => x.Id == menuItemId)
            ?? throw new InvalidOperationException("Menu item not found.");

        if (!menuItem.IsAvailable)
        {
            throw new InvalidOperationException("Menu item is not currently available.");
        }

        return menuItem;
    }

    private static void EnsureOrderEditable(ServiceOrder order)
    {
        if (order.Status is not OrderStatus.Open)
        {
            throw new InvalidOperationException("Only open orders can be edited.");
        }
    }

    private static void ValidateOrderItems(IReadOnlyCollection<CreateOrderLineInput> lines)
    {
        if (lines.Count == 0)
        {
            throw new ArgumentException("An order must contain at least one line item.", nameof(lines));
        }

        if (lines.Any(x => x.Quantity <= 0))
        {
            throw new ArgumentException("Order line quantity must be greater than zero.", nameof(lines));
        }
    }

    private static void ValidateOrderLines(IReadOnlyCollection<OrderLine> lines)
    {
        if (lines.Count == 0)
        {
            throw new ArgumentException("An order must contain at least one line item.", nameof(lines));
        }

        if (lines.Any(x => x.Quantity <= 0))
        {
            throw new ArgumentException("Order line quantity must be greater than zero.", nameof(lines));
        }
    }
}
