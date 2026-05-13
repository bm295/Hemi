using System.Threading;
using System.Threading.Tasks;

namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowOrchestrator<TRequest, TResult>
{
    string WorkflowKey { get; }

    Task<TResult> ExecuteAsync(
        TRequest request,
        CancellationToken cancellationToken = default);
}