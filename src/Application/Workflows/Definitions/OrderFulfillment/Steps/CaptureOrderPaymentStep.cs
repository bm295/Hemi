using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;

public sealed class CaptureOrderPaymentStep(
    IOrderQueryPort orderQueryPort,
    IPaymentQueryPort paymentQueryPort,
    IPaymentCommandPort paymentCommandPort,
    RefundOrderPaymentCompensation compensation)
    : ICompensableWorkflowStep<WorkflowContext>
{
    public async Task ExecuteAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        var order = await GetOrderAsync(orderId, cancellationToken);

        if (order.Status is OrderStatus.Completed)
        {
            context.Set(
                OrderFulfillmentWorkflowContext.PaymentCreated,
                false);
            return;
        }

        if (order.Status is OrderStatus.Cancelled)
        {
            throw new InvalidOperationException(
                "Cancelled orders cannot be paid.");
        }

        var settledPayment = (await paymentQueryPort.GetPaymentsAsync(cancellationToken))
            .FirstOrDefault(payment =>
                payment.OrderId == orderId &&
                payment.Status is PaymentStatus.Settled);

        if (settledPayment is not null)
        {
            context.Set(
                OrderFulfillmentWorkflowContext.PaymentId,
                settledPayment.Id);
            context.Set(
                OrderFulfillmentWorkflowContext.PaymentCreated,
                false);
            return;
        }

        var paymentAmount =
            OrderFulfillmentWorkflowContext.GetPaymentAmount(context) ??
            order.TotalAmount;

        if (paymentAmount < order.TotalAmount)
        {
            throw new InvalidOperationException(
                "Payment amount is less than order total.");
        }

        var paymentMethod =
            OrderFulfillmentWorkflowContext.GetPaymentMethod(context);

        var payment = await paymentCommandPort.AddPaymentAsync(
            orderId,
            paymentAmount,
            paymentMethod,
            cancellationToken);

        context.Set(OrderFulfillmentWorkflowContext.PaymentId, payment.Id);
        context.Set(OrderFulfillmentWorkflowContext.PaymentCreated, true);
    }

    public Task CompensateAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default) =>
        compensation.CompensateAsync(context, cancellationToken);

    private async Task<ServiceOrder> GetOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken) =>
        (await orderQueryPort.GetOrdersAsync(cancellationToken))
            .SingleOrDefault(order => order.Id == orderId)
        ?? throw new InvalidOperationException("Order not found.");
}
