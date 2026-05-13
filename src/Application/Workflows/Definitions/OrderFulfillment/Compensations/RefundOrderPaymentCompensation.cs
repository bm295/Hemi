using Hemi.Application.Workflows.Execution;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;

public sealed class RefundOrderPaymentCompensation(
    IPaymentCommandPort paymentCommandPort)
{
    public async Task CompensateAsync(
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        if (!OrderFulfillmentWorkflowContext.GetFlag(
                context,
                OrderFulfillmentWorkflowContext.PaymentCreated))
        {
            return;
        }

        var orderId = OrderFulfillmentWorkflowContext.GetOrderId(context);
        await paymentCommandPort.RefundPaymentsForOrderAsync(
            orderId,
            cancellationToken);
    }
}
