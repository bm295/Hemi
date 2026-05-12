using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Abstractions;

public interface IRetryPolicyProvider
{
    WorkflowPolicies GetPolicy(string workflowId);
}