using System;
using System.Collections.Generic;
using System.Text;

namespace Hemi.Application.Workflows.Abstractions;

public interface ICompensationHandler<TContext>
{
    Task CompensateAsync(
        TContext context,
        CancellationToken cancellationToken = default);
}
