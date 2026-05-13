using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowEngine : IWorkflowEngine
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IServiceProvider _serviceProvider;
    private readonly IRetryPolicyProvider _retryPolicyProvider;
    private readonly IWorkflowEventPublisher _workflowEventPublisher;
    private readonly IWorkflowInstanceStore? _workflowInstanceStore;
    private readonly IWorkflowExecutionLogStore? _workflowExecutionLogStore;

    public WorkflowEngine(
        IServiceProvider serviceProvider,
        IRetryPolicyProvider retryPolicyProvider,
        IWorkflowEventPublisher? workflowEventPublisher = null)
    {
        _serviceProvider = serviceProvider;
        _retryPolicyProvider = retryPolicyProvider;
        _workflowEventPublisher =
            workflowEventPublisher ?? NoOpWorkflowEventPublisher.Instance;
        _workflowInstanceStore =
            serviceProvider.GetService(typeof(IWorkflowInstanceStore)) as IWorkflowInstanceStore;
        _workflowExecutionLogStore =
            serviceProvider.GetService(typeof(IWorkflowExecutionLogStore))
                as IWorkflowExecutionLogStore;
    }

    public async Task ExecuteAsync(
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowContext context,
        CancellationToken cancellationToken = default)
    {
        await EnsureWorkflowInstanceAsync(
            workflowId,
            context,
            cancellationToken);

        var persistedAttempts = await GetPersistedAttemptsAsync(
            context,
            cancellationToken);
        var succeededStepAttempts = GetSucceededStepAttempts(persistedAttempts);
        var compensatedStepOrders = GetCompensatedStepOrders(persistedAttempts);
        var compensationSteps = BuildPersistedCompensationStack(
            workflowDefinition,
            succeededStepAttempts,
            compensatedStepOrders);
        var policies = _retryPolicyProvider.GetPolicy(workflowId);

        using var timeoutCts =
            new CancellationTokenSource(policies.Timeout);

        using var linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                timeoutCts.Token);

        var executionToken = linkedCts.Token;

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

            foreach (var indexedStep in EnumerateSteps(workflowDefinition))
            {
                executionToken.ThrowIfCancellationRequested();

                if (succeededStepAttempts.ContainsKey(indexedStep.Order))
                {
                    continue;
                }

                var step = ResolveStep(indexedStep.Type);
                var stepAttempt = GetWorkflowAttempt(context);

                await MarkStepRunningAsync(
                    context,
                    indexedStep,
                    stepAttempt,
                    cancellationToken);

                await PublishAsync(
                    WorkflowEvents.StepStarted,
                    workflowId,
                    workflowDefinition,
                    context,
                    indexedStep.Type,
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
                    await MarkStepFailedAsync(
                        context,
                        indexedStep.Order,
                        stepAttempt,
                        ex,
                        CancellationToken.None);

                    await PersistContextAsync(
                        context,
                        CancellationToken.None);

                    await PublishAsync(
                        WorkflowEvents.StepFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        indexedStep.Type,
                        ex,
                        CancellationToken.None);

                    throw;
                }

                await PersistContextAsync(
                    context,
                    executionToken);

                await MarkStepSucceededAsync(
                    context,
                    indexedStep.Order,
                    stepAttempt,
                    executionToken);

                if (step is ICompensableWorkflowStep<WorkflowContext> compensableStep)
                {
                    compensationSteps.Push(
                        new CompensationStep(
                            indexedStep.Order,
                            stepAttempt,
                            compensableStep));
                }

                await PublishAsync(
                    WorkflowEvents.StepCompleted,
                    workflowId,
                    workflowDefinition,
                    context,
                    indexedStep.Type,
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

            await PersistWorkflowFailureAsync(
                context,
                ex,
                CancellationToken.None);

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
                    compensationSteps,
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

            await PersistWorkflowFailureAsync(
                context,
                ex,
                CancellationToken.None);

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

            if (policies.EnableCompensation &&
                context.State != WorkflowState.Cancelled)
            {
                await CompensateAsync(
                    workflowId,
                    workflowDefinition,
                    compensationSteps,
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
        Stack<CompensationStep> compensationSteps,
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        context.State = WorkflowState.Compensating;

        await PersistWorkflowStateAsync(
            context,
            WorkflowState.Compensating,
            error: null,
            completedAtUtc: null,
            cancellationToken);

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
            while (compensationSteps.Count > 0)
            {
                var compensationStep = compensationSteps.Pop();

                try
                {
                    await compensationStep.Step.CompensateAsync(
                        context,
                        cancellationToken);

                    await PersistContextAsync(
                        context,
                        CancellationToken.None);

                    await MarkStepCompensatedAsync(
                        context,
                        compensationStep,
                        CancellationToken.None);
                }
                catch (Exception ex)
                {
                    context.LastError = ex;
                    context.State = WorkflowState.CompensationFailed;

                    await PersistContextAsync(
                        context,
                        CancellationToken.None);

                    await MarkStepCompensationFailedAsync(
                        context,
                        compensationStep,
                        ex,
                        CancellationToken.None);

                    await PersistWorkflowStateAsync(
                        context,
                        WorkflowState.CompensationFailed,
                        ex,
                        DateTimeOffset.UtcNow,
                        CancellationToken.None);

                    await PublishAsync(
                        WorkflowEvents.CompensationFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        compensationStep.Step.GetType(),
                        ex,
                        CancellationToken.None);

                    throw;
                }
            }

            context.State = WorkflowState.Compensated;

            await PersistWorkflowStateAsync(
                context,
                WorkflowState.Compensated,
                error: null,
                completedAtUtc: DateTimeOffset.UtcNow,
                CancellationToken.None);

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

    private async Task EnsureWorkflowInstanceAsync(
        string workflowId,
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        if (context.WorkflowInstanceId.HasValue ||
            _workflowInstanceStore is null)
        {
            return;
        }

        var instance = await _workflowInstanceStore.GetInstanceByCorrelationAsync(
            workflowId,
            context.CorrelationId,
            cancellationToken);

        if (instance is null)
        {
            return;
        }

        context.WorkflowInstanceId = instance.Id;
        context.WorkflowInstanceVersion = instance.Version;
        context.WorkflowAttempt = instance.Attempt;
        context.CommandId = instance.CommandId;
    }

    private async Task<IReadOnlyCollection<WorkflowStepAttemptRecord>> GetPersistedAttemptsAsync(
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return [];
        }

        return await _workflowExecutionLogStore.GetStepAttemptsAsync(
            context.WorkflowInstanceId.Value,
            cancellationToken);
    }

    private Stack<CompensationStep> BuildPersistedCompensationStack(
        IWorkflowDefinition workflowDefinition,
        IReadOnlyDictionary<int, WorkflowStepAttemptRecord> succeededStepAttempts,
        IReadOnlySet<int> compensatedStepOrders)
    {
        var compensationSteps = new Stack<CompensationStep>();

        foreach (var indexedStep in EnumerateSteps(workflowDefinition))
        {
            if (compensatedStepOrders.Contains(indexedStep.Order) ||
                !succeededStepAttempts.TryGetValue(indexedStep.Order, out var attempt))
            {
                continue;
            }

            if (ResolveStep(indexedStep.Type) is ICompensableWorkflowStep<WorkflowContext> step)
            {
                compensationSteps.Push(
                    new CompensationStep(
                        indexedStep.Order,
                        attempt.Attempt,
                        step));
            }
        }

        return compensationSteps;
    }

    private async Task PersistContextAsync(
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        if (_workflowInstanceStore is null ||
            !context.WorkflowInstanceId.HasValue ||
            context.WorkflowInstanceVersion <= 0)
        {
            return;
        }

        var payloadJson = JsonSerializer.Serialize(
            context.Items,
            SerializerOptions);

        var updated = await _workflowInstanceStore.TryUpdatePayloadAsync(
            context.WorkflowInstanceId.Value,
            context.WorkflowInstanceVersion,
            payloadJson,
            cancellationToken);

        if (!updated)
        {
            throw new InvalidOperationException(
                "Workflow context update failed due to optimistic concurrency.");
        }

        context.WorkflowInstanceVersion++;
    }

    private async Task PersistWorkflowFailureAsync(
        WorkflowContext context,
        Exception error,
        CancellationToken cancellationToken)
    {
        await PersistContextAsync(
            context,
            cancellationToken);

        await PersistWorkflowStateAsync(
            context,
            context.State,
            error,
            completedAtUtc: IsTerminal(context.State)
                ? DateTimeOffset.UtcNow
                : null,
            cancellationToken);
    }

    private async Task PersistWorkflowStateAsync(
        WorkflowContext context,
        WorkflowState state,
        Exception? error,
        DateTimeOffset? completedAtUtc,
        CancellationToken cancellationToken)
    {
        if (_workflowInstanceStore is null ||
            !context.WorkflowInstanceId.HasValue ||
            context.WorkflowInstanceVersion <= 0)
        {
            return;
        }

        var updated = await _workflowInstanceStore.TryUpdateStateAsync(
            context.WorkflowInstanceId.Value,
            state,
            context.WorkflowInstanceVersion,
            error?.Message,
            completedAtUtc,
            nextAttemptAtUtc: null,
            cancellationToken);

        if (!updated)
        {
            throw new InvalidOperationException(
                "Workflow state update failed due to optimistic concurrency.");
        }

        context.WorkflowInstanceVersion++;
    }

    private async Task MarkStepRunningAsync(
        WorkflowContext context,
        IndexedStep indexedStep,
        int attempt,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return;
        }

        await _workflowExecutionLogStore.MarkStepRunningAsync(
            new WorkflowStepAttemptStart(
                context.WorkflowInstanceId.Value,
                indexedStep.Type.Name,
                indexedStep.Order,
                attempt,
                context.CommandId,
                DateTimeOffset.UtcNow),
            cancellationToken);
    }

    private async Task MarkStepSucceededAsync(
        WorkflowContext context,
        int stepOrder,
        int attempt,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return;
        }

        await _workflowExecutionLogStore.MarkStepSucceededAsync(
            context.WorkflowInstanceId.Value,
            stepOrder,
            attempt,
            DateTimeOffset.UtcNow,
            cancellationToken);
    }

    private async Task MarkStepFailedAsync(
        WorkflowContext context,
        int stepOrder,
        int attempt,
        Exception error,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return;
        }

        await _workflowExecutionLogStore.MarkStepFailedAsync(
            context.WorkflowInstanceId.Value,
            stepOrder,
            attempt,
            error.Message,
            DateTimeOffset.UtcNow,
            cancellationToken);
    }

    private async Task MarkStepCompensatedAsync(
        WorkflowContext context,
        CompensationStep compensationStep,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return;
        }

        await _workflowExecutionLogStore.MarkStepCompensatedAsync(
            context.WorkflowInstanceId.Value,
            compensationStep.Order,
            compensationStep.Attempt,
            DateTimeOffset.UtcNow,
            cancellationToken);
    }

    private async Task MarkStepCompensationFailedAsync(
        WorkflowContext context,
        CompensationStep compensationStep,
        Exception error,
        CancellationToken cancellationToken)
    {
        if (_workflowExecutionLogStore is null ||
            !context.WorkflowInstanceId.HasValue)
        {
            return;
        }

        await _workflowExecutionLogStore.MarkStepCompensationFailedAsync(
            context.WorkflowInstanceId.Value,
            compensationStep.Order,
            compensationStep.Attempt,
            error.Message,
            DateTimeOffset.UtcNow,
            cancellationToken);
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

    private static Dictionary<int, WorkflowStepAttemptRecord> GetSucceededStepAttempts(
        IEnumerable<WorkflowStepAttemptRecord> attempts) =>
        attempts
            .Where(attempt => attempt.Status == WorkflowStepAttemptStatus.Succeeded)
            .GroupBy(attempt => attempt.StepOrder)
            .ToDictionary(
                group => group.Key,
                group => group
                    .OrderByDescending(attempt => attempt.Attempt)
                    .First());

    private static HashSet<int> GetCompensatedStepOrders(
        IEnumerable<WorkflowStepAttemptRecord> attempts) =>
        attempts
            .Where(attempt =>
                attempt.Status is WorkflowStepAttemptStatus.Compensated
                    or WorkflowStepAttemptStatus.CompensationFailed)
            .Select(attempt => attempt.StepOrder)
            .ToHashSet();

    private static IEnumerable<IndexedStep> EnumerateSteps(
        IWorkflowDefinition workflowDefinition) =>
        workflowDefinition.Steps.Select((stepType, index) =>
            new IndexedStep(
                index + 1,
                stepType));

    private static int GetWorkflowAttempt(WorkflowContext context) =>
        context.WorkflowAttempt > 0
            ? context.WorkflowAttempt
            : 1;

    private static bool IsTerminal(WorkflowState state) =>
        state is WorkflowState.Succeeded
            or WorkflowState.Failed
            or WorkflowState.Compensated
            or WorkflowState.CompensationFailed
            or WorkflowState.Cancelled;

    private sealed record IndexedStep(
        int Order,
        Type Type);

    private sealed record CompensationStep(
        int Order,
        int Attempt,
        ICompensableWorkflowStep<WorkflowContext> Step);
}
