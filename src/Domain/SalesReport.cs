namespace Hemi.Domain;

public sealed record SalesReport(
    DateTimeOffset From,
    DateTimeOffset To,
    decimal TotalRevenue,
    int SettledPayments,
    int ClosedOrders);
