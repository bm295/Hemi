namespace Hemi.Application.Workflows.Contracts;

public sealed record StartWorkflowCommand(
    string WorkflowId,
    string CorrelationId,
    IReadOnlyDictionary<string, object?> Items,
    string? IdempotencyKey = null,
    string? RequestedBy = null,
    DateTimeOffset? RequestedAtUtc = null);

public sealed record WorkflowWorkerCommand(
    Guid CommandId,
    string WorkflowId,
    string CorrelationId,
    IReadOnlyDictionary<string, object?> Items,
    int Attempt,
    DateTimeOffset EnqueuedAtUtc,
    string? IdempotencyKey = null,
    string? Source = null);
