using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

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
