using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

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
