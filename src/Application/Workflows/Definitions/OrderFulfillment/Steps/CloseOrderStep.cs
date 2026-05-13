using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;

public sealed class CloseOrderStep(
    IOrderQueryPort orderQueryPort,
    IOrderCommandPort orderCommandPort,
    IPaymentQueryPort paymentQueryPort)
    : IWorkflowStep<WorkflowContext>
{
    public async Task ExecuteAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        var order = await GetOrderAsync(orderId, cancellationToken);

        if (order.Status is OrderStatus.Completed)
        {
            context.Set(OrderFulfillmentWorkflowContext.OrderClosed, false);
            return;
        }

        if (order.Status is OrderStatus.Cancelled)
        {
            throw new InvalidOperationException(
                "Cancelled orders cannot be closed.");
        }

        var hasSettledPayment = (await paymentQueryPort.GetPaymentsAsync(cancellationToken))
            .Any(payment =>
                payment.OrderId == orderId &&
                payment.Status is PaymentStatus.Settled);

        if (!hasSettledPayment)
        {
            throw new InvalidOperationException(
                "Order cannot be closed before successful payment.");
        }

        await orderCommandPort.UpdateOrderStatusAsync(
            orderId,
            OrderStatus.Completed,
            cancellationToken);

        context.Set(OrderFulfillmentWorkflowContext.OrderClosed, true);
    }

    private async Task<ServiceOrder> GetOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken) =>
        (await orderQueryPort.GetOrdersAsync(cancellationToken))
            .SingleOrDefault(order => order.Id == orderId)
        ?? throw new InvalidOperationException("Order not found.");
}
