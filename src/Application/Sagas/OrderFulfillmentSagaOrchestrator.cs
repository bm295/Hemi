using Hemi.Domain;

namespace Hemi.Application;

public enum SagaStepStatus
{
    Pending,
    Completed,
    Compensated
}

public enum OrderFulfillmentSagaStatus
{
    Running,
    Completed,
    Failed,
    Compensated
}

public sealed record OrderFulfillmentSagaState(
    Guid SagaId,
    Guid OrderId,
    OrderFulfillmentSagaStatus Status,
    SagaStepStatus KitchenStep,
    SagaStepStatus PaymentStep,
    SagaStepStatus InventoryStep,
    SagaStepStatus CloseOrderStep,
    DateTimeOffset StartedAt,
    DateTimeOffset UpdatedAt,
    string? LastError);

public sealed class OrderFulfillmentSagaOrchestrator(
    IOrderQueryPort orderQueryPort,
    IOrderCommandPort orderCommandPort,
    IPaymentQueryPort paymentQueryPort,
    IPaymentCommandPort paymentCommandPort,
    IInventoryCommandPort inventoryCommandPort,
    ISagaStateQueryPort sagaStateQueryPort,
    ISagaStateCommandPort sagaStateCommandPort)
{
    public Task<OrderFulfillmentSagaState?> GetSagaStateAsync(Guid orderId, CancellationToken cancellationToken = default) =>
        sagaStateQueryPort.GetOrderFulfillmentSagaAsync(orderId, cancellationToken);

    public async Task<OrderFulfillmentSagaState> ExecuteAsync(Guid orderId, PaymentMethod paymentMethod, decimal? paymentAmount = null, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var saga = new OrderFulfillmentSagaState(
            Guid.NewGuid(),
            orderId,
            OrderFulfillmentSagaStatus.Running,
            SagaStepStatus.Pending,
            SagaStepStatus.Pending,
            SagaStepStatus.Pending,
            SagaStepStatus.Pending,
            now,
            now,
            null);

        await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);

        try
        {
            var order = await GetOrderByIdAsync(orderId, cancellationToken);
            if (order.Status is OrderStatus.Open)
            {
                order = await orderCommandPort.UpdateOrderStatusAsync(orderId, OrderStatus.SentToKitchen, cancellationToken);
            }

            saga = MarkStep(saga, kitchen: SagaStepStatus.Completed);
            await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);

            var settledPaymentExists = (await paymentQueryPort.GetPaymentsAsync(cancellationToken))
                .Any(x => x.OrderId == orderId && x.Status is PaymentStatus.Settled);

            if (!settledPaymentExists)
            {
                var paymentToCharge = paymentAmount ?? order.TotalAmount;
                if (paymentToCharge < order.TotalAmount)
                {
                    throw new InvalidOperationException("Payment amount is less than order total.");
                }

                _ = await paymentCommandPort.AddPaymentAsync(orderId, paymentToCharge, paymentMethod, cancellationToken);
            }

            saga = MarkStep(saga, payment: SagaStepStatus.Completed);
            await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);

            _ = await inventoryCommandPort.DeductInventoryForOrderAsync(orderId, cancellationToken);
            saga = MarkStep(saga, inventory: SagaStepStatus.Completed);
            await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);

            _ = await orderCommandPort.UpdateOrderStatusAsync(orderId, OrderStatus.Completed, cancellationToken);
            saga = MarkStep(saga, closeOrder: SagaStepStatus.Completed, sagaStatus: OrderFulfillmentSagaStatus.Completed);
            await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);
            return saga;
        }
        catch (Exception ex)
        {
            saga = saga with
            {
                Status = OrderFulfillmentSagaStatus.Failed,
                LastError = ex.Message,
                UpdatedAt = DateTimeOffset.UtcNow
            };
            await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(saga, cancellationToken);
            return await CompensateAsync(saga, cancellationToken);
        }
    }

    private async Task<OrderFulfillmentSagaState> CompensateAsync(OrderFulfillmentSagaState saga, CancellationToken cancellationToken)
    {
        var mutableSaga = saga;

        if (mutableSaga.InventoryStep is SagaStepStatus.Completed)
        {
            _ = await inventoryCommandPort.RestoreInventoryForOrderAsync(saga.OrderId, cancellationToken);
            mutableSaga = MarkStep(mutableSaga, inventory: SagaStepStatus.Compensated);
        }

        if (mutableSaga.PaymentStep is SagaStepStatus.Completed)
        {
            _ = await paymentCommandPort.RefundPaymentsForOrderAsync(saga.OrderId, cancellationToken);
            mutableSaga = MarkStep(mutableSaga, payment: SagaStepStatus.Compensated);
        }

        if (mutableSaga.KitchenStep is SagaStepStatus.Completed)
        {
            var order = await GetOrderByIdAsync(saga.OrderId, cancellationToken);
            if (order.Status is OrderStatus.SentToKitchen)
            {
                _ = await orderCommandPort.UpdateOrderStatusAsync(order.Id, OrderStatus.Open, cancellationToken);
            }

            mutableSaga = MarkStep(mutableSaga, kitchen: SagaStepStatus.Compensated);
        }

        mutableSaga = mutableSaga with
        {
            Status = OrderFulfillmentSagaStatus.Compensated,
            UpdatedAt = DateTimeOffset.UtcNow
        };

        await sagaStateCommandPort.SaveOrderFulfillmentSagaAsync(mutableSaga, cancellationToken);
        return mutableSaga;
    }

    private async Task<ServiceOrder> GetOrderByIdAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var order = (await orderQueryPort.GetOrdersAsync(cancellationToken)).SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");
        return order;
    }

    private static OrderFulfillmentSagaState MarkStep(
        OrderFulfillmentSagaState saga,
        SagaStepStatus? kitchen = null,
        SagaStepStatus? payment = null,
        SagaStepStatus? inventory = null,
        SagaStepStatus? closeOrder = null,
        OrderFulfillmentSagaStatus? sagaStatus = null) =>
        saga with
        {
            KitchenStep = kitchen ?? saga.KitchenStep,
            PaymentStep = payment ?? saga.PaymentStep,
            InventoryStep = inventory ?? saga.InventoryStep,
            CloseOrderStep = closeOrder ?? saga.CloseOrderStep,
            Status = sagaStatus ?? saga.Status,
            UpdatedAt = DateTimeOffset.UtcNow
        };
}
