using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Abstractions;

public sealed record WorkflowEvent(
    string EventName,
    string WorkflowId,
    string WorkflowName,
    string CorrelationId,
    WorkflowState State,
    string? StepName,
    Exception? Error,
    DateTimeOffset OccurredAtUtc);

public interface IWorkflowEventPublisher
{
    Task PublishAsync(
        WorkflowEvent workflowEvent,
        CancellationToken cancellationToken = default);
}
