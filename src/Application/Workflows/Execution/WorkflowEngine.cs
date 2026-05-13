using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowEngine : IWorkflowEngine
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IRetryPolicyProvider _retryPolicyProvider;
    private readonly IWorkflowEventPublisher _workflowEventPublisher;

    public WorkflowEngine(
        IServiceProvider serviceProvider,
        IRetryPolicyProvider retryPolicyProvider,
        IWorkflowEventPublisher? workflowEventPublisher = null)
    {
        _serviceProvider = serviceProvider;
        _retryPolicyProvider = retryPolicyProvider;
        _workflowEventPublisher =
            workflowEventPublisher ?? NoOpWorkflowEventPublisher.Instance;
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

        var executedSteps =
            new Stack<ICompensableWorkflowStep<WorkflowContext>>();

        try
        {
            context.State = WorkflowState.Running;

            await PublishAsync(
                WorkflowEvents.WorkflowStarted,
                workflowId,
                workflowDefinition,
                context,
                stepType: null,
                error: null,
                executionToken);

            foreach (var stepType in workflowDefinition.Steps)
            {
                executionToken.ThrowIfCancellationRequested();

                var step = ResolveStep(stepType);

                await PublishAsync(
                    WorkflowEvents.StepStarted,
                    workflowId,
                    workflowDefinition,
                    context,
                    stepType,
                    error: null,
                    executionToken);

                try
                {
                    await ExecuteWithRetryAsync(
                        step,
                        context,
                        policies,
                        executionToken);
                }
                catch (Exception ex)
                {
                    await PublishAsync(
                        WorkflowEvents.StepFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        stepType,
                        ex,
                        CancellationToken.None);

                    throw;
                }

                if (step is ICompensableWorkflowStep<WorkflowContext> compensableStep)
                {
                    executedSteps.Push(compensableStep);
                }

                await PublishAsync(
                    WorkflowEvents.StepCompleted,
                    workflowId,
                    workflowDefinition,
                    context,
                    stepType,
                    error: null,
                    executionToken);
            }

            context.State = WorkflowState.Succeeded;

            await PublishAsync(
                WorkflowEvents.WorkflowSucceeded,
                workflowId,
                workflowDefinition,
                context,
                stepType: null,
                error: null,
                executionToken);
        }
        catch (OperationCanceledException ex)
            when (timeoutCts.IsCancellationRequested &&
                  !cancellationToken.IsCancellationRequested)
        {
            context.LastError = ex;
            context.State = WorkflowState.Failed;

            await PublishAsync(
                WorkflowEvents.WorkflowFailed,
                workflowId,
                workflowDefinition,
                context,
                stepType: null,
                ex,
                CancellationToken.None);

            if (policies.EnableCompensation)
            {
                await CompensateAsync(
                    workflowId,
                    workflowDefinition,
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
            context.State =
                ex is OperationCanceledException &&
                cancellationToken.IsCancellationRequested
                    ? WorkflowState.Cancelled
                    : WorkflowState.Failed;

            await PublishAsync(
                context.State == WorkflowState.Cancelled
                    ? WorkflowEvents.WorkflowCancelled
                    : WorkflowEvents.WorkflowFailed,
                workflowId,
                workflowDefinition,
                context,
                stepType: null,
                ex,
                CancellationToken.None);

            if (policies.EnableCompensation)
            {
                await CompensateAsync(
                    workflowId,
                    workflowDefinition,
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

    private async Task CompensateAsync(
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        Stack<ICompensableWorkflowStep<WorkflowContext>> executedSteps,
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        context.State = WorkflowState.Compensating;

        await PublishAsync(
            WorkflowEvents.CompensationStarted,
            workflowId,
            workflowDefinition,
            context,
            stepType: null,
            error: null,
            CancellationToken.None);

        try
        {
            while (executedSteps.Count > 0)
            {
                var step = executedSteps.Pop();

                try
                {
                    await step.CompensateAsync(
                        context,
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    context.LastError = ex;
                    context.State = WorkflowState.CompensationFailed;

                    await PublishAsync(
                        WorkflowEvents.CompensationFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        step.GetType(),
                        ex,
                        CancellationToken.None);

                    throw;
                }
            }

            context.State = WorkflowState.Compensated;

            await PublishAsync(
                WorkflowEvents.CompensationCompleted,
                workflowId,
                workflowDefinition,
                context,
                stepType: null,
                error: null,
                CancellationToken.None);
        }
        catch (Exception ex)
        {
            context.LastError = ex;
            context.State = WorkflowState.CompensationFailed;
            throw;
        }
    }

    private Task PublishAsync(
        string eventName,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowContext context,
        Type? stepType,
        Exception? error,
        CancellationToken cancellationToken)
    {
        var workflowEvent = new WorkflowEvent(
            eventName,
            workflowId,
            workflowDefinition.Name,
            context.CorrelationId,
            context.State,
            stepType?.Name,
            error,
            DateTimeOffset.UtcNow);

        return _workflowEventPublisher.PublishAsync(
            workflowEvent,
            cancellationToken);
    }
}
