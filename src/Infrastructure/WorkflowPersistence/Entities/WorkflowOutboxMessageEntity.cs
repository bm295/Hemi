namespace Hemi.Infrastructure.WorkflowPersistence.Entities;

public enum WorkflowOutboxMessageStatus
{
    Pending,
    Published,
    Failed
}

public sealed class WorkflowOutboxMessageEntity
{
    public Guid Id { get; set; }

    public Guid WorkflowInstanceId { get; set; }

    public string MessageType { get; set; } = string.Empty;

    public string Destination { get; set; } = string.Empty;

    public string PayloadJson { get; set; } = "{}";

    public string HeadersJson { get; set; } = "{}";

    public WorkflowOutboxMessageStatus Status { get; set; }

    public int RetryCount { get; set; }

    public string? ErrorMessage { get; set; }

    public DateTimeOffset CreatedAtUtc { get; set; }

    public DateTimeOffset? LastAttemptAtUtc { get; set; }

    public DateTimeOffset? NextAttemptAtUtc { get; set; }

    public DateTimeOffset? PublishedAtUtc { get; set; }

    public WorkflowInstanceEntity? WorkflowInstance { get; set; }
}
