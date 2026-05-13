namespace Hemi.Application.Workflows.Abstractions;

public enum WorkflowOutboxStatus
{
    Pending,
    Published,
    Failed
}

public sealed record WorkflowOutboxMessageDraft(
    Guid WorkflowInstanceId,
    string MessageType,
    string Destination,
    string PayloadJson,
    string HeadersJson = "{}",
    DateTimeOffset? CreatedAtUtc = null,
    DateTimeOffset? NextAttemptAtUtc = null);

public sealed record WorkflowOutboxMessageRecord(
    Guid Id,
    Guid WorkflowInstanceId,
    string MessageType,
    string Destination,
    string PayloadJson,
    string HeadersJson,
    WorkflowOutboxStatus Status,
    int RetryCount,
    string? ErrorMessage,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? LastAttemptAtUtc,
    DateTimeOffset? NextAttemptAtUtc,
    DateTimeOffset? PublishedAtUtc);

public interface IWorkflowOutboxStore
{
    Task<WorkflowOutboxMessageRecord> SaveMessageAsync(
        WorkflowOutboxMessageDraft message,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> GetMessagesForWorkflowAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> GetPendingMessagesAsync(
        int batchSize = 50,
        DateTimeOffset? dueAtUtc = null,
        CancellationToken cancellationToken = default);

    Task MarkMessagePublishedAsync(
        Guid messageId,
        DateTimeOffset publishedAtUtc,
        CancellationToken cancellationToken = default);

    Task MarkMessageFailedAsync(
        Guid messageId,
        string errorMessage,
        DateTimeOffset lastAttemptAtUtc,
        DateTimeOffset? nextAttemptAtUtc,
        CancellationToken cancellationToken = default);
}
