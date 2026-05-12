using System;
using System.Collections.Generic;
using System.Text;

namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowStep<TContext>
{
    Task ExecuteAsync(
        TContext context,
        CancellationToken cancellationToken = default);
}
