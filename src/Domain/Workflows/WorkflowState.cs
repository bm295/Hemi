using System;
using System.Collections.Generic;
using System.Text;

namespace Hemi.Domain.Workflows;

public enum WorkflowState
{
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    Failed = 3,
    Compensating = 4,
    Compensated = 5,
    CompensationFailed = 6,
    Cancelled = 7
}
