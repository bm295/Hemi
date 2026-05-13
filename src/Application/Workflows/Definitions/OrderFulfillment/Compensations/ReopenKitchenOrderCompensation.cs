using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;

public sealed class ReopenKitchenOrderCompensation(
    IOrderQueryPort orderQueryPort,
    IOrderCommandPort orderCommandPort)
{
    public async Task CompensateAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        if (!OrderFulfillmentWorkflowContext.GetFlag(
                context,
                OrderFulfillmentWorkflowContext.KitchenStatusChanged))
        {
            return;
        }

        if (OrderFulfillmentWorkflowContext.GetOriginalOrderStatus(context) is not OrderStatus.Open)
        {
            return;
        }

        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        var order = await GetOrderAsync(orderId, cancellationToken);
        if (order.Status is not OrderStatus.SentToKitchen)
        {
            return;
        }

        await orderCommandPort.UpdateOrderStatusAsync(
            orderId,
            OrderStatus.Open,
            cancellationToken);
    }

    private async Task<ServiceOrder> GetOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken) =>
        (await orderQueryPort.GetOrdersAsync(cancellationToken))
            .SingleOrDefault(order => order.Id == orderId)
        ?? throw new InvalidOperationException("Order not found.");
}
