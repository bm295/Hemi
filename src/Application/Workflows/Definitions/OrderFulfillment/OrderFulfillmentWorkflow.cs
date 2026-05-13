using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment;

public sealed class OrderFulfillmentWorkflow : IWorkflowDefinition
{
    public string Name => WorkflowIds.OrderFulfillment;

    public IReadOnlyCollection<Type> Steps { get; } =
    [
        typeof(SendOrderToKitchenStep),
        typeof(CaptureOrderPaymentStep),
        typeof(DeductOrderInventoryStep),
        typeof(CloseOrderStep)
    ];

    public static WorkflowContext CreateContext(
        Guid orderId,
        PaymentMethod paymentMethod,
        decimal? paymentAmount = null,
        string? correlationId = null)
    {
        var context = new WorkflowContext(
            WorkflowIds.OrderFulfillment,
            correlationId ?? orderId.ToString("D"));

        context.Set(OrderFulfillmentWorkflowContext.OrderId, orderId);
        context.Set(OrderFulfillmentWorkflowContext.PaymentMethod, paymentMethod);

        if (paymentAmount.HasValue)
        {
            context.Set(
                OrderFulfillmentWorkflowContext.PaymentAmount,
                paymentAmount.Value);
        }

        return context;
    }
}
