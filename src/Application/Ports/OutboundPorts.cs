using Hemi.Domain;

namespace Hemi.Application;

public interface IRestaurantQueryPort
{
    Task<RestaurantProfile> GetRestaurantProfileAsync(CancellationToken cancellationToken = default);
}

public interface ITableQueryPort
{
    Task<IReadOnlyCollection<DiningTable>> GetTablesAsync(CancellationToken cancellationToken = default);
}

public interface IMenuQueryPort
{
    Task<IReadOnlyCollection<MenuItem>> GetMenuItemsAsync(CancellationToken cancellationToken = default);
}

public interface IOrderQueryPort
{
    Task<IReadOnlyCollection<ServiceOrder>> GetOrdersAsync(CancellationToken cancellationToken = default);
}

public interface IOrderCommandPort
{
    Task<ServiceOrder> AddOrderAsync(Guid tableId, IReadOnlyCollection<OrderLine> lines, CancellationToken cancellationToken = default);
    Task<ServiceOrder> UpdateOrderLinesAsync(Guid orderId, IReadOnlyCollection<OrderLine> lines, CancellationToken cancellationToken = default);
    Task<ServiceOrder> UpdateOrderStatusAsync(Guid orderId, OrderStatus status, CancellationToken cancellationToken = default);
}

public interface IReservationQueryPort
{
    Task<IReadOnlyCollection<Reservation>> GetReservationsAsync(CancellationToken cancellationToken = default);
}

public interface IReservationCommandPort
{
    Task<Reservation> AddReservationAsync(string guestName, int partySize, DateTimeOffset reservedFor, string contactPhone, string? notes, CancellationToken cancellationToken = default);
}

public interface IInventoryQueryPort
{
    Task<IReadOnlyCollection<InventoryItem>> GetInventoryAsync(CancellationToken cancellationToken = default);
}

public interface IInventoryCommandPort
{
    Task<IReadOnlyCollection<StockMovement>> DeductInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default);
    Task<IReadOnlyCollection<StockMovement>> RestoreInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default);
}

public interface IPaymentQueryPort
{
    Task<IReadOnlyCollection<Payment>> GetPaymentsAsync(CancellationToken cancellationToken = default);
}

public interface IPaymentCommandPort
{
    Task<Payment> AddPaymentAsync(Guid orderId, decimal amount, PaymentMethod paymentMethod, CancellationToken cancellationToken = default);
    Task<IReadOnlyCollection<Payment>> RefundPaymentsForOrderAsync(Guid orderId, CancellationToken cancellationToken = default);
}

public interface ISagaStateQueryPort
{
    Task<OrderFulfillmentSagaState?> GetOrderFulfillmentSagaAsync(Guid orderId, CancellationToken cancellationToken = default);
}

public interface ISagaStateCommandPort
{
    Task SaveOrderFulfillmentSagaAsync(OrderFulfillmentSagaState sagaState, CancellationToken cancellationToken = default);
}
