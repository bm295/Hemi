using Hemi.Application;

namespace Hemi.Infrastructure;

public enum SagaInstanceStatus
{
    Started,
    Waiting,
    Completed,
    Failed,
    Compensating,
    Compensated
}

public enum SagaStepExecutionStatus
{
    Pending,
    Sent,
    Succeeded,
    Failed,
    Compensated
}

public enum OutboxMessageStatus
{
    Pending,
    Published,
    Failed
}

public sealed record SagaInstanceRow(
    Guid Id,
    string SagaType,
    Guid CorrelationId,
    string CurrentStep,
    SagaInstanceStatus Status,
    string PayloadJson,
    int Version,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public sealed record SagaStepRow(
    Guid Id,
    Guid SagaInstanceId,
    string StepName,
    int StepOrder,
    SagaStepExecutionStatus Status,
    Guid? CommandId,
    Guid? ReplyMessageId,
    string? ErrorMessage,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt);

public sealed record OutboxMessageRow(
    Guid Id,
    Guid SagaInstanceId,
    string MessageType,
    string Destination,
    string PayloadJson,
    OutboxMessageStatus Status,
    int RetryCount,
    DateTimeOffset CreatedAt,
    DateTimeOffset? PublishedAt);

public sealed record InboxMessageRow(
    Guid MessageId,
    string Source,
    DateTimeOffset ReceivedAt,
    DateTimeOffset? ProcessedAt);

public sealed record SagaConcurrencyToken(
    Guid SagaInstanceId,
    int Version,
    DateTimeOffset UpdatedAt);

public sealed record SagaMessageEnvelope(
    Guid OrderId,
    Guid SagaId,
    OrderFulfillmentSagaStatus Status,
    SagaStepStatus KitchenStep,
    SagaStepStatus PaymentStep,
    SagaStepStatus InventoryStep,
    SagaStepStatus CloseOrderStep,
    string? LastError,
    DateTimeOffset UpdatedAt,
    DateTimeOffset StartedAt);
