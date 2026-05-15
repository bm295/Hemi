namespace Hemi.Application.Sagas.Legacy;

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
