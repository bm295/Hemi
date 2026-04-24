namespace Hemi.Domain;

public enum PaymentMethod
{
    Cash,
    Card,
    BankTransfer,
    EWallet
}

public enum PaymentStatus
{
    Pending,
    Settled,
    Failed,
    Refunded
}

public sealed record Payment(
    Guid Id,
    Guid OrderId,
    decimal Amount,
    PaymentMethod Method,
    PaymentStatus Status,
    DateTimeOffset PaidAt);
