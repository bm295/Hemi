namespace Hemi.Application.Workflows.Abstractions;

public interface ICompensableWorkflowStep<TContext>
    : IWorkflowStep<TContext>
{
    Task CompensateAsync(
        TContext context,
        CancellationToken cancellationToken = default);
}