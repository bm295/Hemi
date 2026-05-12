using System;
using System.Collections.Generic;
using System.Text;

namespace Hemi.Application.Workflows.Abstractions;

public interface IWorkflowRegistry
{
    IReadOnlyCollection<IWorkflowDefinition> GetAll();

    IWorkflowDefinition GetRequired(string workflowName);

    bool TryGet(
        string workflowName,
        out IWorkflowDefinition? workflowDefinition);
}