using Hemi.Domain;
using Hemi.Infrastructure;

namespace Hemi.Tests.Infrastructure.InMemory;

public sealed class InMemoryOrderFulfillmentSideEffectTests
{
    [Fact]
    public async Task DeductInventoryForOrderAsync_returns_existing_deductions_when_retried()
    {
        var (store, order) = await CreateOrderAsync();
        var inventoryAdapter = new InMemoryInventoryAdapter(store);
        var inventoryItemId = GetInventoryItemId(store, order);
        var startingStock = GetStockQuantity(store, inventoryItemId);

        var firstDeduction = await inventoryAdapter.DeductInventoryForOrderAsync(order.Id);
        var secondDeduction = await inventoryAdapter.DeductInventoryForOrderAsync(order.Id);

        Assert.Equal(
            firstDeduction.Select(x => x.Id),
            secondDeduction.Select(x => x.Id));
        Assert.Equal(
            startingStock - order.Lines.Sum(x => x.Quantity),
            GetStockQuantity(store, inventoryItemId));
        Assert.Single(
            store.StockMovements,
            x => x.OrderId == order.Id && x.QuantityChanged < 0);
    }

    [Fact]
    public async Task RestoreInventoryForOrderAsync_returns_existing_restorations_when_retried()
    {
        var (store, order) = await CreateOrderAsync();
        var inventoryAdapter = new InMemoryInventoryAdapter(store);
        var inventoryItemId = GetInventoryItemId(store, order);
        var startingStock = GetStockQuantity(store, inventoryItemId);

        _ = await inventoryAdapter.DeductInventoryForOrderAsync(order.Id);
        var firstRestoration = await inventoryAdapter.RestoreInventoryForOrderAsync(order.Id);
        var secondRestoration = await inventoryAdapter.RestoreInventoryForOrderAsync(order.Id);

        Assert.Equal(
            firstRestoration.Select(x => x.Id),
            secondRestoration.Select(x => x.Id));
        Assert.Equal(startingStock, GetStockQuantity(store, inventoryItemId));
        Assert.Single(
            store.StockMovements,
            x => x.OrderId == order.Id && x.QuantityChanged > 0);
    }

    [Fact]
    public async Task AddPaymentAsync_returns_existing_settled_payment_when_retried()
    {
        var (store, order) = await CreateOrderAsync();
        var paymentAdapter = new InMemoryPaymentAdapter(store);

        var firstPayment = await paymentAdapter.AddPaymentAsync(
            order.Id,
            order.TotalAmount,
            PaymentMethod.Card);
        var secondPayment = await paymentAdapter.AddPaymentAsync(
            order.Id,
            order.TotalAmount,
            PaymentMethod.Card);

        Assert.Equal(firstPayment.Id, secondPayment.Id);
        Assert.Single(
            store.Payments,
            x => x.OrderId == order.Id && x.Status is PaymentStatus.Settled);
    }

    [Fact]
    public async Task UpdateOrderStatusAsync_returns_current_order_when_status_is_unchanged()
    {
        var (store, order) = await CreateOrderAsync();
        var orderAdapter = new InMemoryOrderAdapter(store);

        var completedOrder = await orderAdapter.UpdateOrderStatusAsync(
            order.Id,
            OrderStatus.Completed);
        var repeatedOrder = await orderAdapter.UpdateOrderStatusAsync(
            order.Id,
            OrderStatus.Completed);

        Assert.Equal(completedOrder, repeatedOrder);
        Assert.Equal(
            TableStatus.Available,
            store.Tables.Single(x => x.Id == order.TableId).Status);
    }

    private static async Task<(InMemoryFnbStore Store, ServiceOrder Order)> CreateOrderAsync()
    {
        var store = new InMemoryFnbStore();
        var orderAdapter = new InMemoryOrderAdapter(store);
        var table = store.Tables.First(x => x.Status is TableStatus.Available);
        var menuItem = store.MenuItems.First(x => x.IsAvailable);
        var order = await orderAdapter.AddOrderAsync(
            table.Id,
            [new OrderLine(menuItem.Id, 2, menuItem.Price)]);

        return (store, order);
    }

    private static Guid GetInventoryItemId(InMemoryFnbStore store, ServiceOrder order) =>
        store.Inventory
            .Single(x => x.MenuItemId == order.Lines.Single().MenuItemId)
            .Id;

    private static decimal GetStockQuantity(InMemoryFnbStore store, Guid inventoryItemId) =>
        store.Inventory
            .Single(x => x.Id == inventoryItemId)
            .StockQuantity;
}
