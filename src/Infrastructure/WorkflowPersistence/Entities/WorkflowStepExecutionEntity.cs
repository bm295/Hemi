namespace Hemi.Infrastructure.WorkflowPersistence.Entities;

public enum WorkflowStepExecutionStatus
{
    Pending,
    Running,
    Succeeded,
    Failed,
    Compensated,
    CompensationFailed
}

public sealed class WorkflowStepExecutionEntity
{
    public Guid Id { get; set; }

    public Guid WorkflowInstanceId { get; set; }

    public string StepName { get; set; } = string.Empty;

    public int StepOrder { get; set; }

    public WorkflowStepExecutionStatus Status { get; set; }

    public int Attempt { get; set; }

    public Guid? CommandId { get; set; }

    public string? ErrorMessage { get; set; }

    public DateTimeOffset? StartedAtUtc { get; set; }

    public DateTimeOffset? CompletedAtUtc { get; set; }

    public DateTimeOffset? CompensatedAtUtc { get; set; }

    public WorkflowInstanceEntity? WorkflowInstance { get; set; }
}
