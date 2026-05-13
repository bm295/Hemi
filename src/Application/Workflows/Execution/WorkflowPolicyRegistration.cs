using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowPolicyRegistration
{
    public WorkflowPolicyRegistration(
        string workflowId,
        WorkflowPolicies policy)
    {
        if (string.IsNullOrWhiteSpace(workflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(workflowId));
        }

        WorkflowId = workflowId;
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
    }

    public string WorkflowId { get; }

    public WorkflowPolicies Policy { get; }
}
