using Hemi.Application.Workflows.Abstractions;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowDispatcher : IWorkflowDispatcher
{
    private readonly IWorkflowRegistry _workflowRegistry;
    private readonly IWorkflowEngine _workflowEngine;

    public WorkflowDispatcher(
        IWorkflowRegistry workflowRegistry,
        IWorkflowEngine workflowEngine)
    {
        _workflowRegistry = workflowRegistry;
        _workflowEngine = workflowEngine;
    }

    public async Task DispatchAsync(
        string workflowId,
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        var workflowDefinition =
            _workflowRegistry.GetRequired(workflowId);

        await _workflowEngine.ExecuteAsync(
            workflowId,
            workflowDefinition,
            context,
            cancellationToken);
    }
}