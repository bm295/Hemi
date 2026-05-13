using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;

public sealed class SendOrderToKitchenStep(
    IOrderQueryPort orderQueryPort,
    IOrderCommandPort orderCommandPort,
    ReopenKitchenOrderCompensation compensation)
    : ICompensableWorkflowStep<WorkflowContext>
{
    public async Task ExecuteAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        var order = await GetOrderAsync(orderId, cancellationToken);

        context.Set(
            OrderFulfillmentWorkflowContext.OriginalOrderStatus,
            order.Status);

        if (order.Status is OrderStatus.Completed)
        {
            context.Set(
                OrderFulfillmentWorkflowContext.KitchenStatusChanged,
                false);
            return;
        }

        if (order.Status is OrderStatus.Cancelled)
        {
            throw new InvalidOperationException(
                "Cancelled orders cannot be fulfilled.");
        }

        if (order.Status is OrderStatus.SentToKitchen)
        {
            context.Set(
                OrderFulfillmentWorkflowContext.KitchenStatusChanged,
                false);
            return;
        }

        await orderCommandPort.UpdateOrderStatusAsync(
            orderId,
            OrderStatus.SentToKitchen,
            cancellationToken);

        context.Set(
            OrderFulfillmentWorkflowContext.KitchenStatusChanged,
            true);
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
