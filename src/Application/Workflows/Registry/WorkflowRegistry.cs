using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Registry;

public sealed class WorkflowRegistry : IWorkflowRegistry
{
    private readonly Dictionary<string, IWorkflowDefinition> _definitions =
        new(StringComparer.Ordinal);

    public WorkflowRegistry()
    {
    }

    public WorkflowRegistry(
        IEnumerable<IWorkflowDefinition> workflowDefinitions)
    {
        ArgumentNullException.ThrowIfNull(workflowDefinitions);

        foreach (var workflowDefinition in workflowDefinitions)
        {
            Register(workflowDefinition);
        }
    }

    public IReadOnlyCollection<IWorkflowDefinition> GetAll() =>
        _definitions.Values.ToArray();

    public IWorkflowDefinition GetRequired(string workflowName)
    {
        if (TryGet(workflowName, out var workflowDefinition))
        {
            return workflowDefinition
                ?? throw new KeyNotFoundException(
                    $"Workflow '{workflowName}' is not registered.");
        }

        throw new KeyNotFoundException(
            $"Workflow '{workflowName}' is not registered.");
    }

    public bool TryGet(
        string workflowName,
        out IWorkflowDefinition? workflowDefinition)
    {
        workflowDefinition = null;

        if (string.IsNullOrWhiteSpace(workflowName))
        {
            return false;
        }

        return _definitions.TryGetValue(
            workflowName,
            out workflowDefinition);
    }

    public WorkflowRegistry Register(
        string workflowName,
        params Type[] steps) =>
        Register(new WorkflowDefinition(workflowName, steps));

    public WorkflowRegistry Register(
        IWorkflowDefinition workflowDefinition)
    {
        ArgumentNullException.ThrowIfNull(workflowDefinition);

        if (string.IsNullOrWhiteSpace(workflowDefinition.Name))
        {
            throw new ArgumentException(
                "Workflow name is required.",
                nameof(workflowDefinition));
        }

        if (!_definitions.TryAdd(
                workflowDefinition.Name,
                workflowDefinition))
        {
            throw new InvalidOperationException(
                $"Workflow '{workflowDefinition.Name}' is already registered.");
        }

        return this;
    }

    public WorkflowRegistry RegisterAll() =>
        this
            .Register(WorkflowIds.OrderFulfillment)
            .Register(WorkflowIds.OrderCancellation)
            .Register(WorkflowIds.PaymentConfirmation)
            .Register(WorkflowIds.InventoryDeduction)
            .Register(WorkflowIds.InventoryReconciliation);

    public static WorkflowRegistry RegisterAllWorkflows() =>
        new WorkflowRegistry().RegisterAll();
}
