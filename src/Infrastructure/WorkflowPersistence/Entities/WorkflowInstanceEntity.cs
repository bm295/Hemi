using Hemi.Domain.Workflows;

namespace Hemi.Infrastructure.WorkflowPersistence.Entities;

public sealed class WorkflowInstanceEntity
{
    public Guid Id { get; set; }

    public string WorkflowId { get; set; } = string.Empty;

    public string WorkflowName { get; set; } = string.Empty;

    public string CorrelationId { get; set; } = string.Empty;

    public WorkflowState State { get; set; }

    public string PayloadJson { get; set; } = "{}";

    public string? LastError { get; set; }

    public int Version { get; set; }

    public DateTimeOffset CreatedAtUtc { get; set; }

    public DateTimeOffset UpdatedAtUtc { get; set; }

    public DateTimeOffset? CompletedAtUtc { get; set; }

    public ICollection<WorkflowStepExecutionEntity> Steps { get; } = [];

    public ICollection<WorkflowOutboxMessageEntity> OutboxMessages { get; } = [];
}
