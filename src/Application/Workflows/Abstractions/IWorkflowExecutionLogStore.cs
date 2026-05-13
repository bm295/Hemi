namespace Hemi.Application.Workflows.Abstractions;

public enum WorkflowStepAttemptStatus
{
    Pending,
    Running,
    Succeeded,
    Failed,
    Compensated,
    CompensationFailed
}

public sealed record WorkflowStepAttemptStart(
    Guid WorkflowInstanceId,
    string StepName,
    int StepOrder,
    int Attempt,
    Guid? CommandId = null,
    DateTimeOffset? StartedAtUtc = null);

public sealed record WorkflowStepAttemptRecord(
    Guid Id,
    Guid WorkflowInstanceId,
    string StepName,
    int StepOrder,
    WorkflowStepAttemptStatus Status,
    int Attempt,
    Guid? CommandId,
    string? ErrorMessage,
    DateTimeOffset? StartedAtUtc,
    DateTimeOffset? CompletedAtUtc,
    DateTimeOffset? CompensatedAtUtc);

public interface IWorkflowExecutionLogStore
{
    Task<IReadOnlyCollection<WorkflowStepAttemptRecord>> GetStepAttemptsAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default);

    Task<WorkflowStepAttemptRecord> MarkStepRunningAsync(
        WorkflowStepAttemptStart request,
        CancellationToken cancellationToken = default);

    Task<bool> MarkStepSucceededAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        DateTimeOffset completedAtUtc,
        CancellationToken cancellationToken = default);

    Task<bool> MarkStepFailedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        string errorMessage,
        DateTimeOffset completedAtUtc,
        CancellationToken cancellationToken = default);

    Task<bool> MarkStepCompensatedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        DateTimeOffset compensatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<bool> MarkStepCompensationFailedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        string errorMessage,
        DateTimeOffset compensatedAtUtc,
        CancellationToken cancellationToken = default);
}
