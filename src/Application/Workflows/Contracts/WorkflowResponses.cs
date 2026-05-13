using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Contracts;

public sealed record WorkflowAcceptedResponse(
    Guid WorkflowInstanceId,
    Guid CommandId,
    string WorkflowId,
    string CorrelationId,
    WorkflowState State,
    DateTimeOffset AcceptedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    string? LastError,
    IReadOnlyCollection<WorkflowStepSummaryResponse> Steps,
    string? IdempotencyKey = null);

public sealed record WorkflowExecutionResponse(
    Guid? CommandId,
    string WorkflowId,
    string WorkflowName,
    string CorrelationId,
    WorkflowState State,
    IReadOnlyDictionary<string, object?> Items,
    string? ErrorMessage,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc);

public sealed record WorkflowStatusResponse(
    Guid WorkflowInstanceId,
    Guid CommandId,
    string WorkflowId,
    string CorrelationId,
    WorkflowState State,
    DateTimeOffset AcceptedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    string? LastError,
    IReadOnlyCollection<WorkflowStepSummaryResponse> Steps,
    IReadOnlyDictionary<string, object?> Items,
    string? IdempotencyKey = null);

public sealed record WorkflowStepSummaryResponse(
    int Order,
    string Name,
    WorkflowStepAttemptStatus Status,
    int Attempt,
    string? ErrorMessage,
    DateTimeOffset? StartedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    DateTimeOffset? CompensatedAtUtc);

public sealed record WorkflowEventResponse(
    string EventName,
    string WorkflowId,
    string WorkflowName,
    string CorrelationId,
    WorkflowState State,
    string? StepName,
    string? ErrorMessage,
    DateTimeOffset OccurredAtUtc);

public sealed record WorkflowDefinitionResponse(
    string WorkflowId,
    IReadOnlyCollection<WorkflowStepResponse> Steps);

public sealed record WorkflowStepResponse(
    int Order,
    string Name);

public sealed record WorkflowErrorResponse(
    string Message,
    string? Code = null,
    string? WorkflowId = null,
    string? CorrelationId = null);
