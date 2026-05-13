using Hemi.Application.Workflows.Execution;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;

public sealed class RestoreOrderInventoryCompensation(
    IInventoryCommandPort inventoryCommandPort)
{
    public async Task CompensateAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        if (!OrderFulfillmentWorkflowContext.GetFlag(
                context,
                OrderFulfillmentWorkflowContext.InventoryDeducted) ||
            OrderFulfillmentWorkflowContext.GetFlag(
                context,
                OrderFulfillmentWorkflowContext.InventoryRestored))
        {
            return;
        }

        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        await inventoryCommandPort.RestoreInventoryForOrderAsync(
            orderId,
            cancellationToken);

        context.Set(OrderFulfillmentWorkflowContext.InventoryRestored, true);
    }
}
