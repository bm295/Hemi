using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;
using Hemi.Domain;
using Hemi.Domain.Workflows;

namespace Hemi.Tests.Workflows.Definitions;

public sealed class OrderFulfillmentWorkflowTests
{
    [Fact]
    public void Definition_uses_order_fulfillment_id_and_expected_step_order()
    {
        var workflow = new OrderFulfillmentWorkflow();

        Assert.Equal(WorkflowIds.OrderFulfillment, workflow.Name);
        Assert.Equal(
        [
            typeof(SendOrderToKitchenStep),
            typeof(CaptureOrderPaymentStep),
            typeof(DeductOrderInventoryStep),
            typeof(CloseOrderStep)
        ],
            workflow.Steps);
    }

    [Fact]
    public void CreateContext_sets_order_payment_and_correlation_items()
    {
        var orderId = Guid.NewGuid();
        const string correlationId = "order-fulfillment-test";

        var context = OrderFulfillmentWorkflow.CreateContext(
            orderId,
            PaymentMethod.Card,
            paymentAmount: 45.25m,
            correlationId);

        Assert.Equal(WorkflowIds.OrderFulfillment, context.WorkflowId);
        Assert.Equal(correlationId, context.CorrelationId);
        Assert.Equal(orderId, OrderFulfillmentWorkflowContext.GetOrderId(context));
        Assert.Equal(PaymentMethod.Card, OrderFulfillmentWorkflowContext.GetPaymentMethod(context));
        Assert.Equal(45.25m, OrderFulfillmentWorkflowContext.GetPaymentAmount(context));
    }

    [Fact]
    public void CreateContext_uses_order_id_as_default_correlation()
    {
        var orderId = Guid.NewGuid();

        var context = OrderFulfillmentWorkflow.CreateContext(
            orderId,
            PaymentMethod.Cash);

        Assert.Equal(orderId.ToString("D"), context.CorrelationId);
        Assert.Null(OrderFulfillmentWorkflowContext.GetPaymentAmount(context));
    }
}
