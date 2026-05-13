using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;

public sealed class DeductOrderInventoryStep(
    IOrderQueryPort orderQueryPort,
    IInventoryCommandPort inventoryCommandPort,
    RestoreOrderInventoryCompensation compensation)
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
                OrderFulfillmentWorkflowContext.InventoryDeducted,
                false);
            return;
        }

        if (OrderFulfillmentWorkflowContext.GetFlag(
                context,
                OrderFulfillmentWorkflowContext.InventoryDeducted))
        {
            return;
        }

        var movements = await inventoryCommandPort.DeductInventoryForOrderAsync(
            orderId,
            cancellationToken);

        context.Set(
            OrderFulfillmentWorkflowContext.InventoryMovementIds,
            movements.Select(movement => movement.Id).ToArray());
        context.Set(
            OrderFulfillmentWorkflowContext.InventoryDeducted,
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
