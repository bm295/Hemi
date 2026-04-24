namespace Hemi.Domain;

public enum OrderStatus
{
    Open,
    SentToKitchen,
    Completed,
    Cancelled
}

public sealed record OrderLine(Guid MenuItemId, int Quantity, decimal UnitPrice)
{
    public decimal LineTotal => Quantity * UnitPrice;
}

public sealed record ServiceOrder(
    Guid Id,
    Guid TableId,
    DateTimeOffset CreatedAt,
    OrderStatus Status,
    IReadOnlyCollection<OrderLine> Lines)
{
    public decimal TotalAmount => Lines.Sum(line => line.LineTotal);
}
