using Hemi.Application.Workflows.Execution;

namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowEngine
{
    Task ExecuteAsync(
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowContext context,
        CancellationToken cancellationToken = default);
}