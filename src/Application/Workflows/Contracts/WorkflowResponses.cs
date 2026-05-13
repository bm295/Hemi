using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Contracts;

public sealed record WorkflowAcceptedResponse(
    Guid CommandId,
    string WorkflowId,
    string CorrelationId,
    WorkflowState State,
    DateTimeOffset AcceptedAtUtc,
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
    string WorkflowId,
    string CorrelationId,
    WorkflowState State,
    IReadOnlyDictionary<string, object?> Items,
    string? LastError,
    DateTimeOffset UpdatedAtUtc);

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
