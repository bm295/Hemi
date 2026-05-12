using Hemi.Application.Workflows.Execution;

namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowDispatcher
{
    Task DispatchAsync(
        string workflowId,
        WorkflowContext context,
        CancellationToken cancellationToken = default);
}