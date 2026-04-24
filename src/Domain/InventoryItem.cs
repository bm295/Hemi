namespace Hemi.Domain;

public sealed record InventoryItem(
    Guid Id,
    Guid MenuItemId,
    string Name,
    decimal StockQuantity,
    string Unit);

public sealed record StockMovement(
    Guid Id,
    Guid InventoryItemId,
    Guid OrderId,
    decimal QuantityChanged,
    DateTimeOffset OccurredAt,
    string Reason);
