using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class RetryPolicyProvider : IRetryPolicyProvider
{
    private readonly IReadOnlyDictionary<string, WorkflowPolicies> _policies;

    public RetryPolicyProvider(
        IEnumerable<WorkflowPolicyRegistration> policyRegistrations)
    {
        ArgumentNullException.ThrowIfNull(policyRegistrations);

        var policies =
            new Dictionary<string, WorkflowPolicies>(StringComparer.Ordinal);

        foreach (var registration in policyRegistrations)
        {
            if (!policies.TryAdd(registration.WorkflowId, registration.Policy))
            {
                throw new InvalidOperationException(
                    $"Workflow policy '{registration.WorkflowId}' is already registered.");
            }
        }

        _policies = policies;
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
