using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Abstractions;

public enum WorkflowStartStatus
{
    Created,
    Existing,
    IdempotencyConflict,
    CorrelationConflict
}

public sealed record WorkflowStartRequest(
    string WorkflowId,
    string WorkflowName,
    string CorrelationId,
    string PayloadJson,
    string IdempotencyKey,
    string RequestHash,
    string? RequestedBy,
    DateTimeOffset RequestedAtUtc,
    DateTimeOffset? NextAttemptAtUtc = null);

public sealed record WorkflowStartResult(
    WorkflowStartStatus Status,
    WorkflowInstanceRecord? Instance,
    string? ExistingRequestHash = null);

public sealed record WorkflowInstanceRecord(
    Guid Id,
    Guid CommandId,
    string WorkflowId,
    string WorkflowName,
    string CorrelationId,
    WorkflowState State,
    string PayloadJson,
    string? LastError,
    int Version,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    string? IdempotencyKey,
    string? RequestHash,
    string? RequestedBy,
    int Attempt,
    DateTimeOffset? NextAttemptAtUtc,
    string? LeaseOwner,
    DateTimeOffset? LeaseUntilUtc);

public interface IWorkflowInstanceStore
{
    Task<WorkflowStartResult> StartWorkflowAsync(
        WorkflowStartRequest request,
        CancellationToken cancellationToken = default);

    Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default);

    Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
        string workflowId,
        string correlationId,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<WorkflowInstanceRecord>> ClaimDueInstancesAsync(
        DateTimeOffset nowUtc,
        string leaseOwner,
        TimeSpan leaseDuration,
        int batchSize = 10,
        CancellationToken cancellationToken = default);

    Task<bool> TryUpdateStateAsync(
        Guid id,
        WorkflowState state,
        int expectedVersion,
        string? lastError = null,
        DateTimeOffset? completedAtUtc = null,
        DateTimeOffset? nextAttemptAtUtc = null,
        CancellationToken cancellationToken = default);

    Task<bool> TryUpdatePayloadAsync(
        Guid id,
        int expectedVersion,
        string payloadJson,
        CancellationToken cancellationToken = default);

    Task<bool> TryReleaseLeaseAsync(
        Guid id,
        string leaseOwner,
        int expectedVersion,
        CancellationToken cancellationToken = default);
}
