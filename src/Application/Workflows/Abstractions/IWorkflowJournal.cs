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
    DateTimeOffset? NextAttemptAtUtc = null,
    bool ClearLease = false);

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

public sealed record WorkflowPayloadJournalEntry(
    Guid WorkflowInstanceId,
    int ExpectedWorkflowVersion,
    string ExpectedLeaseOwner,
    string PayloadJson);

public sealed record WorkflowStateTransitionJournalEntry(
    Guid WorkflowInstanceId,
    int ExpectedWorkflowVersion,
    string ExpectedLeaseOwner,
    WorkflowStateJournalEntry State,
    WorkflowEvent Event,
    string? PayloadJson = null);

public sealed record WorkflowStepAttemptTransitionJournalEntry(
    Guid WorkflowInstanceId,
    int ExpectedWorkflowVersion,
    string ExpectedLeaseOwner,
    WorkflowStepJournalEntry Step,
    WorkflowEvent Event,
    string? PayloadJson = null);

public sealed record WorkflowJournalEntry(
    Guid WorkflowInstanceId,
    int ExpectedWorkflowVersion,
    string ExpectedLeaseOwner,
    string? PayloadJson = null,
    WorkflowStateJournalEntry? State = null,
    WorkflowStepJournalEntry? Step = null,
    WorkflowEvent? Event = null);

public sealed record WorkflowJournalResult(
    int WorkflowInstanceVersion,
    WorkflowStepAttemptRecord? StepAttempt = null);

public interface IWorkflowJournal
{
    Task<WorkflowJournalResult> UpdateWorkflowPayloadAsync(
        WorkflowPayloadJournalEntry entry,
        CancellationToken cancellationToken = default) =>
        AppendAsync(
            new WorkflowJournalEntry(
                entry.WorkflowInstanceId,
                entry.ExpectedWorkflowVersion,
                entry.ExpectedLeaseOwner,
                PayloadJson: entry.PayloadJson),
            cancellationToken);

    Task<WorkflowJournalResult> AppendWorkflowStateTransitionAsync(
        WorkflowStateTransitionJournalEntry entry,
        CancellationToken cancellationToken = default) =>
        AppendAsync(
            new WorkflowJournalEntry(
                entry.WorkflowInstanceId,
                entry.ExpectedWorkflowVersion,
                entry.ExpectedLeaseOwner,
                PayloadJson: entry.PayloadJson,
                State: entry.State,
                Event: entry.Event),
            cancellationToken);

    Task<WorkflowJournalResult> AppendStepAttemptTransitionAsync(
        WorkflowStepAttemptTransitionJournalEntry entry,
        CancellationToken cancellationToken = default) =>
        AppendAsync(
            new WorkflowJournalEntry(
                entry.WorkflowInstanceId,
                entry.ExpectedWorkflowVersion,
                entry.ExpectedLeaseOwner,
                PayloadJson: entry.PayloadJson,
                Step: entry.Step,
                Event: entry.Event),
            cancellationToken);

    Task<WorkflowJournalResult> AppendAsync(
        WorkflowJournalEntry entry,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            "This workflow journal implementation does not support generic journal appends.");
}
