using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class RetryPolicyProvider : IRetryPolicyProvider
{
    private readonly IReadOnlyDictionary<string, WorkflowPolicies> _policies;

    public RetryPolicyProvider()
    {
        _policies = new Dictionary<string, WorkflowPolicies>
        {
            [WorkflowIds.OrderFulfillment] = WorkflowPolicies.Default,
            [WorkflowIds.OrderCancellation] = WorkflowPolicies.NoRetry,
            [WorkflowIds.InventoryReconciliation] = WorkflowPolicies.Default
        };
    }

    public WorkflowPolicies GetPolicy(string workflowId)
    {
        if (_policies.TryGetValue(workflowId, out var policy))
        {
            return policy;
        }

        return WorkflowPolicies.Default;
    }
}