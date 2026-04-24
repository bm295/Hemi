namespace Hemi.Application;

public sealed record CreateOrderLineInput(Guid MenuItemId, int Quantity);

public sealed record FoodAppOrderRequest(
    string SourceApp,
    string ExternalOrderId,
    string TableCode,
    IReadOnlyCollection<FoodAppOrderItemRequest> Items);

public sealed record FoodAppOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record FoodAppIntegrationResult(
    string SourceApp,
    string ExternalOrderId,
    Guid InternalOrderId,
    string TableCode,
    DateTimeOffset SyncedAt,
    decimal TotalVnd);
