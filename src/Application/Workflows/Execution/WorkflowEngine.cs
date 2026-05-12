using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowEngine : IWorkflowEngine
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IRetryPolicyProvider _retryPolicyProvider;

    public WorkflowEngine(
        IServiceProvider serviceProvider,
        IRetryPolicyProvider retryPolicyProvider)
    {
        _serviceProvider = serviceProvider;
        _retryPolicyProvider = retryPolicyProvider;
    }

    public async Task ExecuteAsync(
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        var policies = _retryPolicyProvider.GetPolicy(workflowId);

        using var timeoutCts =
            new CancellationTokenSource(policies.Timeout);

        using var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                timeoutCts.Token);

        var executionToken = linkedCts.Token;

        context.State = WorkflowState.Running;

        var executedSteps =
            new Stack<ICompensableWorkflowStep<WorkflowContext>>();


        try
        {
            foreach (var stepType in workflowDefinition.Steps)
            {
                executionToken.ThrowIfCancellationRequested();

                var step = ResolveStep(stepType);

                await ExecuteWithRetryAsync(
                    step,
                    context,
                    policies,
                    executionToken);

                if (step is ICompensableWorkflowStep<WorkflowContext> compensableStep)
                {
                    executedSteps.Push(compensableStep);
                }
            }

            context.State = WorkflowState.Succeeded;
        }
        catch (OperationCanceledException ex)
            when (timeoutCts.IsCancellationRequested &&
                  !cancellationToken.IsCancellationRequested)
        {
            context.LastError = ex;
            context.State = WorkflowState.Failed;

            if (policies.EnableCompensation)
            {
                await CompensateAsync(
                    executedSteps,
                    context,
                    CancellationToken.None);
            }

            throw new TimeoutException(
                $"Workflow '{workflowId}' timed out after {policies.Timeout}.",
                ex);
        }
        catch (Exception ex)
        {
            context.LastError = ex;
            context.State = WorkflowState.Failed;

            if (policies.EnableCompensation)
            {
                await CompensateAsync(
                    executedSteps,
                    context,
                    cancellationToken);
            }

            throw;
        }
    }

    private IWorkflowStep<WorkflowContext> ResolveStep(Type stepType)
    {
        var step = _serviceProvider.GetService(stepType);

        if (step is not IWorkflowStep<WorkflowContext> workflowStep)
        {
            throw new InvalidOperationException(
                $"Step '{stepType.Name}' does not implement IWorkflowStep<WorkflowContext>.");
        }

        return workflowStep;
    }

    private static async Task ExecuteWithRetryAsync(
        IWorkflowStep<WorkflowContext> step,
        WorkflowContext context,
        WorkflowPolicies policies,
        CancellationToken cancellationToken)
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                await step.ExecuteAsync(context, cancellationToken);
                return;
            }
            catch
            {
                if (attempt >= policies.MaxRetryAttempts)
                {
                    throw;
                }

                attempt++;

                if (policies.RetryDelay > TimeSpan.Zero)
                {
                    await Task.Delay(policies.RetryDelay, cancellationToken);
                }
            }
        }
    }

    private static async Task CompensateAsync(
        Stack<ICompensableWorkflowStep<WorkflowContext>> executedSteps,
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        context.State = WorkflowState.Compensating;

        try
        {
            while (executedSteps.Count > 0)
            {
                var step = executedSteps.Pop();

                await step.CompensateAsync(
                    context,
                    cancellationToken);
            }

            context.State = WorkflowState.Compensated;
        }
        catch (Exception ex)
        {
            context.LastError = ex;
            context.State = WorkflowState.CompensationFailed;
            throw;
        }
    }
}