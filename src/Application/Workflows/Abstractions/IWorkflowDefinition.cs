namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowDefinition
{
    string Name { get; }

    IReadOnlyCollection<Type> Steps { get; }
}