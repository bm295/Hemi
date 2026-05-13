using Hemi.Application;
using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;
using Hemi.Domain;

namespace Hemi.Tests.Workflows.Definitions;

public sealed class OrderFulfillmentStepRetrySafetyTests
{
    [Fact]
    public async Task CaptureOrderPaymentStep_reuses_settled_payment_when_retried()
    {
        var order = CreateOrder();
        var orderPort = new RecordingOrderPort(order);
        var paymentPort = new RecordingPaymentPort();
        var step = new CaptureOrderPaymentStep(
            orderPort,
            paymentPort,
            paymentPort,
            new RefundOrderPaymentCompensation(paymentPort));
        var context = OrderFulfillmentWorkflow.CreateContext(
            order.Id,
            PaymentMethod.Card);

        await step.ExecuteAsync(context);
        await step.ExecuteAsync(context);

        Assert.Equal(1, paymentPort.AddPaymentCalls);
        Assert.Single(
            paymentPort.Payments,
            payment => payment.OrderId == order.Id && payment.Status is PaymentStatus.Settled);
        Assert.False(OrderFulfillmentWorkflowContext.GetFlag(
            context,
            OrderFulfillmentWorkflowContext.PaymentCreated));
        Assert.NotNull(OrderFulfillmentWorkflowContext.GetOptionalGuid(
            context,
            OrderFulfillmentWorkflowContext.PaymentId));
    }

    [Fact]
    public async Task DeductOrderInventoryStep_skips_deduction_when_context_already_marked_deducted()
    {
        var order = CreateOrder();
        var orderPort = new RecordingOrderPort(order);
        var inventoryPort = new RecordingInventoryPort();
        var step = new DeductOrderInventoryStep(
            orderPort,
            inventoryPort,
            new RestoreOrderInventoryCompensation(inventoryPort));
        var context = OrderFulfillmentWorkflow.CreateContext(
            order.Id,
            PaymentMethod.Card);

        await step.ExecuteAsync(context);
        await step.ExecuteAsync(context);

        Assert.Equal(1, inventoryPort.DeductInventoryCalls);
        Assert.True(OrderFulfillmentWorkflowContext.GetFlag(
            context,
            OrderFulfillmentWorkflowContext.InventoryDeducted));
        Assert.NotEmpty(context.GetRequired<Guid[]>(
            OrderFulfillmentWorkflowContext.InventoryMovementIds));
    }

    private static ServiceOrder CreateOrder()
    {
        var menuItemId = Guid.NewGuid();

        return new ServiceOrder(
            Guid.NewGuid(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow,
            OrderStatus.Open,
            [new OrderLine(menuItemId, 2, 10m)]);
    }

    private sealed class RecordingOrderPort(ServiceOrder order) : IOrderQueryPort
    {
        public Task<IReadOnlyCollection<ServiceOrder>> GetOrdersAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<ServiceOrder>>([order]);
    }

    private sealed class RecordingPaymentPort : IPaymentQueryPort, IPaymentCommandPort
    {
        public List<Payment> Payments { get; } = [];

        public int AddPaymentCalls { get; private set; }

        public Task<IReadOnlyCollection<Payment>> GetPaymentsAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<Payment>>(Payments);

        public Task<Payment> AddPaymentAsync(
            Guid orderId,
            decimal amount,
            PaymentMethod paymentMethod,
            CancellationToken cancellationToken = default)
        {
            AddPaymentCalls++;
            var payment = new Payment(
                Guid.NewGuid(),
                orderId,
                amount,
                paymentMethod,
                PaymentStatus.Settled,
                DateTimeOffset.UtcNow);
            Payments.Add(payment);

            return Task.FromResult(payment);
        }

        public Task<IReadOnlyCollection<Payment>> RefundPaymentsForOrderAsync(
            Guid orderId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<Payment>>([]);
    }

    private sealed class RecordingInventoryPort : IInventoryCommandPort
    {
        public int DeductInventoryCalls { get; private set; }

        public Task<IReadOnlyCollection<StockMovement>> DeductInventoryForOrderAsync(
            Guid orderId,
            CancellationToken cancellationToken = default)
        {
            DeductInventoryCalls++;
            return Task.FromResult<IReadOnlyCollection<StockMovement>>(
            [
                new StockMovement(
                    Guid.NewGuid(),
                    Guid.NewGuid(),
                    orderId,
                    -2,
                    DateTimeOffset.UtcNow,
                    "Order Closed")
            ]);
        }

        public Task<IReadOnlyCollection<StockMovement>> RestoreInventoryForOrderAsync(
            Guid orderId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<StockMovement>>([]);
    }
}
