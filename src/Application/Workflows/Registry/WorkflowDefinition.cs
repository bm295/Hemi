using Hemi.Application.Workflows.Abstractions;

namespace Hemi.Application.Workflows.Registry;

public sealed class WorkflowDefinition : IWorkflowDefinition
{
    public WorkflowDefinition(
        string name,
        IEnumerable<Type>? steps = null)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException(
                "Workflow name is required.",
                nameof(name));
        }

        Name = name;
        Steps = (steps ?? Enumerable.Empty<Type>()).ToArray();
    }

    public string Name { get; }

    public IReadOnlyCollection<Type> Steps { get; }

    public static WorkflowDefinition Create(
        string name,
        params Type[] steps) =>
        new(name, steps);
}
