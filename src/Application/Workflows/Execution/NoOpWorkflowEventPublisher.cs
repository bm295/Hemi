using Hemi.Application.Workflows.Abstractions;

namespace Hemi.Application.Workflows.Execution;

public sealed class NoOpWorkflowEventPublisher : IWorkflowEventPublisher
{
    public static NoOpWorkflowEventPublisher Instance { get; } = new();

    public Task PublishAsync(
        WorkflowEvent workflowEvent,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
