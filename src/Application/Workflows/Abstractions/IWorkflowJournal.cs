using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Abstractions;

public enum WorkflowStepJournalAction
{
    Running,
    Succeeded,
    Failed,
    Compensated,
    CompensationFailed
}

public sealed record WorkflowStateJournalEntry(
    WorkflowState State,
    string? LastError = null,
    DateTimeOffset? CompletedAtUtc = null,
    DateTimeOffset? NextAttemptAtUtc = null);

public sealed record WorkflowStepJournalEntry(
    WorkflowStepJournalAction Action,
    string StepName,
    int StepOrder,
    int Attempt,
    Guid? CommandId = null,
    string? ErrorMessage = null,
    DateTimeOffset? StartedAtUtc = null,
    DateTimeOffset? CompletedAtUtc = null,
    DateTimeOffset? CompensatedAtUtc = null);

public sealed record WorkflowJournalEntry(
    Guid WorkflowInstanceId,
    int ExpectedWorkflowVersion,
    string? PayloadJson = null,
    WorkflowStateJournalEntry? State = null,
    WorkflowStepJournalEntry? Step = null,
    WorkflowEvent? Event = null);

public sealed record WorkflowJournalResult(
    int WorkflowInstanceVersion,
    WorkflowStepAttemptRecord? StepAttempt = null);

public interface IWorkflowJournal
{
    Task<WorkflowJournalResult> AppendAsync(
        WorkflowJournalEntry entry,
        CancellationToken cancellationToken = default);
}
