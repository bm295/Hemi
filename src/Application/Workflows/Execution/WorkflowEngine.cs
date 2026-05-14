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
    private readonly IWorkflowJournal? _workflowJournal;
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
        _workflowJournal =
            serviceProvider.GetService(typeof(IWorkflowJournal)) as IWorkflowJournal;
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

            await JournalWorkflowStateAsync(
                context,
                workflowId,
                workflowDefinition,
                WorkflowState.Running,
                WorkflowEvents.WorkflowStarted,
                stepType: null,
                error: null,
                completedAtUtc: null,
                executionToken);

            foreach (var indexedStep in EnumerateSteps(workflowDefinition))
            {
                executionToken.ThrowIfCancellationRequested();

                if (succeededStepAttempts.ContainsKey(indexedStep.Order))
                {
                    continue;
                }

                var step = ResolveStep(indexedStep.Type);
                var stepAttempt = await ExecuteStepWithRetryAsync(
                    step,
                    context,
                    policies,
                    workflowId,
                    workflowDefinition,
                    indexedStep,
                    GetNextStepAttempt(persistedAttempts, indexedStep.Order),
                    executionToken);

                if (step is ICompensableWorkflowStep<WorkflowContext> compensableStep)
                {
                    compensationSteps.Push(
                        new CompensationStep(
                            indexedStep.Order,
                            stepAttempt,
                            compensableStep));
                }

            }

            context.State = WorkflowState.Succeeded;

            await JournalWorkflowStateAsync(
                context,
                workflowId,
                workflowDefinition,
                WorkflowState.Succeeded,
                WorkflowEvents.WorkflowSucceeded,
                stepType: null,
                error: null,
                completedAtUtc: DateTimeOffset.UtcNow,
                CancellationToken.None);
        }
        catch (OperationCanceledException ex)
            when (timeoutCts.IsCancellationRequested &&
                  !cancellationToken.IsCancellationRequested)
        {
            context.LastError = ex;
            context.State = WorkflowState.Failed;

            await JournalWorkflowFailureAsync(
                context,
                workflowId,
                workflowDefinition,
                ex,
                WorkflowEvents.WorkflowFailed,
                clearLease: !policies.EnableCompensation,
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
            if (IsTerminal(context.State))
            {
                throw;
            }

            context.LastError = ex;
            context.State =
                ex is OperationCanceledException &&
                cancellationToken.IsCancellationRequested
                    ? WorkflowState.Cancelled
                    : WorkflowState.Failed;

            await JournalWorkflowFailureAsync(
                context,
                workflowId,
                workflowDefinition,
                ex,
                context.State == WorkflowState.Cancelled
                    ? WorkflowEvents.WorkflowCancelled
                    : WorkflowEvents.WorkflowFailed,
                clearLease:
                    !(policies.EnableCompensation &&
                        context.State != WorkflowState.Cancelled),
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

    private async Task<int> ExecuteStepWithRetryAsync(
        IWorkflowStep<WorkflowContext> step,
        WorkflowContext context,
        WorkflowPolicies policies,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        IndexedStep indexedStep,
        int firstStepAttempt,
        CancellationToken cancellationToken)
    {
        var retryAttempt = 0;
        var stepAttempt = firstStepAttempt;

        while (true)
        {
            await JournalStepRunningAsync(
                context,
                indexedStep,
                stepAttempt,
                workflowId,
                workflowDefinition,
                cancellationToken);

            try
            {
                await step.ExecuteAsync(context, cancellationToken);
            }
            catch (Exception ex)
            {
                await JournalStepFailedAsync(
                    context,
                    workflowId,
                    workflowDefinition,
                    indexedStep,
                    indexedStep.Order,
                    stepAttempt,
                    ex,
                    CancellationToken.None);

                if (retryAttempt >= policies.MaxRetryAttempts)
                {
                    throw;
                }

                retryAttempt++;
                stepAttempt++;

                if (policies.RetryDelay > TimeSpan.Zero)
                {
                    await Task.Delay(policies.RetryDelay, cancellationToken);
                }

                continue;
            }

            await JournalStepSucceededAsync(
                context,
                workflowId,
                workflowDefinition,
                indexedStep,
                stepAttempt,
                cancellationToken);

            return stepAttempt;
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
        var terminalStatePersisted = false;

        try
        {
            await JournalWorkflowStateAsync(
                context,
                workflowId,
                workflowDefinition,
                WorkflowState.Compensating,
                WorkflowEvents.CompensationStarted,
                stepType: null,
                error: null,
                completedAtUtc: null,
                cancellationToken);

            while (compensationSteps.Count > 0)
            {
                var compensationStep = compensationSteps.Pop();

                try
                {
                    await compensationStep.Step.CompensateAsync(
                        context,
                        cancellationToken);

                    await JournalStepCompensatedAsync(
                        context,
                        workflowId,
                        workflowDefinition,
                        compensationStep,
                        CancellationToken.None);
                }
                catch (Exception ex)
                {
                    context.LastError = ex;
                    context.State = WorkflowState.CompensationFailed;

                    await JournalStepCompensationFailedAsync(
                        context,
                        workflowId,
                        workflowDefinition,
                        compensationStep,
                        ex,
                        DateTimeOffset.UtcNow,
                        CancellationToken.None);
                    terminalStatePersisted = true;

                    throw;
                }
            }

            context.State = WorkflowState.Compensated;

            await JournalWorkflowStateAsync(
                context,
                workflowId,
                workflowDefinition,
                WorkflowState.Compensated,
                WorkflowEvents.CompensationCompleted,
                stepType: null,
                error: null,
                completedAtUtc: DateTimeOffset.UtcNow,
                CancellationToken.None);
            terminalStatePersisted = true;
        }
        catch (Exception ex)
        {
            if (terminalStatePersisted && IsTerminal(context.State))
            {
                throw;
            }

            context.LastError = ex;
            context.State = WorkflowState.CompensationFailed;

            await JournalWorkflowStateAsync(
                context,
                workflowId,
                workflowDefinition,
                WorkflowState.CompensationFailed,
                WorkflowEvents.CompensationFailed,
                stepType: null,
                ex,
                DateTimeOffset.UtcNow,
                CancellationToken.None);

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

    private async Task JournalWorkflowStateAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowState state,
        string eventName,
        Type? stepType,
        Exception? error,
        DateTimeOffset? completedAtUtc,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            await AppendWorkflowStateTransitionAsync(
                context,
                new WorkflowStateTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    State: new WorkflowStateJournalEntry(
                        state,
                        error?.Message,
                        completedAtUtc,
                        ClearLease: IsTerminal(state)),
                    Event: CreateWorkflowEvent(
                        eventName,
                        workflowId,
                        workflowDefinition,
                        context,
                        stepType,
                        error)),
                cancellationToken);
            return;
        }

        await PersistWorkflowStateAsync(
            context,
            state,
            error,
            completedAtUtc,
            cancellationToken);

        await PublishAsync(
            eventName,
            workflowId,
            workflowDefinition,
            context,
            stepType,
            error,
            cancellationToken);
    }

    private async Task JournalWorkflowFailureAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        Exception error,
        string eventName,
        bool clearLease,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            await AppendWorkflowStateTransitionAsync(
                context,
                new WorkflowStateTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    PayloadJson: SerializeContext(context),
                    State: new WorkflowStateJournalEntry(
                        context.State,
                        error.Message,
                        IsTerminal(context.State)
                            ? DateTimeOffset.UtcNow
                            : null,
                        ClearLease: clearLease),
                    Event: CreateWorkflowEvent(
                        eventName,
                        workflowId,
                        workflowDefinition,
                        context,
                        stepType: null,
                        error)),
                cancellationToken);
            return;
        }

        await PersistWorkflowFailureAsync(
            context,
            error,
            cancellationToken);

        await PublishAsync(
            eventName,
            workflowId,
            workflowDefinition,
            context,
            stepType: null,
            error,
            cancellationToken);
    }

    private async Task JournalStepRunningAsync(
        WorkflowContext context,
        IndexedStep indexedStep,
        int attempt,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            var startedAtUtc = DateTimeOffset.UtcNow;
            await AppendStepAttemptTransitionAsync(
                context,
                new WorkflowStepAttemptTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.Running,
                        indexedStep.Type.Name,
                        indexedStep.Order,
                        attempt,
                        context.CommandId,
                        StartedAtUtc: startedAtUtc),
                    Event: CreateWorkflowEvent(
                        WorkflowEvents.StepStarted,
                        workflowId,
                        workflowDefinition,
                        context,
                        indexedStep.Type,
                        error: null,
                        startedAtUtc)),
                cancellationToken);
            return;
        }

        await MarkStepRunningAsync(
            context,
            indexedStep,
            attempt,
            cancellationToken);

        await PublishAsync(
            WorkflowEvents.StepStarted,
            workflowId,
            workflowDefinition,
            context,
            indexedStep.Type,
            error: null,
            cancellationToken);
    }

    private async Task JournalStepSucceededAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        IndexedStep indexedStep,
        int attempt,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            var completedAtUtc = DateTimeOffset.UtcNow;
            await AppendStepAttemptTransitionAsync(
                context,
                new WorkflowStepAttemptTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    PayloadJson: SerializeContext(context),
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.Succeeded,
                        indexedStep.Type.Name,
                        indexedStep.Order,
                        attempt,
                        CompletedAtUtc: completedAtUtc),
                    Event: CreateWorkflowEvent(
                        WorkflowEvents.StepCompleted,
                        workflowId,
                        workflowDefinition,
                        context,
                        indexedStep.Type,
                        error: null,
                        completedAtUtc)),
                cancellationToken);
            return;
        }

        await PersistContextAsync(
            context,
            cancellationToken);

        await MarkStepSucceededAsync(
            context,
            indexedStep.Order,
            attempt,
            cancellationToken);

        await PublishAsync(
            WorkflowEvents.StepCompleted,
            workflowId,
            workflowDefinition,
            context,
            indexedStep.Type,
            error: null,
            cancellationToken);
    }

    private async Task JournalStepFailedAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        IndexedStep indexedStep,
        int stepOrder,
        int attempt,
        Exception error,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            var completedAtUtc = DateTimeOffset.UtcNow;
            await AppendStepAttemptTransitionAsync(
                context,
                new WorkflowStepAttemptTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    PayloadJson: SerializeContext(context),
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.Failed,
                        indexedStep.Type.Name,
                        stepOrder,
                        attempt,
                        ErrorMessage: error.Message,
                        CompletedAtUtc: completedAtUtc),
                    Event: CreateWorkflowEvent(
                        WorkflowEvents.StepFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        indexedStep.Type,
                        error,
                        completedAtUtc)),
                cancellationToken);
            return;
        }

        await MarkStepFailedAsync(
            context,
            stepOrder,
            attempt,
            error,
            cancellationToken);

        await PersistContextAsync(
            context,
            cancellationToken);

        await PublishAsync(
            WorkflowEvents.StepFailed,
            workflowId,
            workflowDefinition,
            context,
            indexedStep.Type,
            error,
            cancellationToken);
    }

    private async Task JournalStepCompensatedAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        CompensationStep compensationStep,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            var compensatedAtUtc = DateTimeOffset.UtcNow;
            await AppendStepAttemptTransitionAsync(
                context,
                new WorkflowStepAttemptTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    PayloadJson: SerializeContext(context),
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.Compensated,
                        compensationStep.Step.GetType().Name,
                        compensationStep.Order,
                        compensationStep.Attempt,
                        CompensatedAtUtc: compensatedAtUtc),
                    Event: CreateWorkflowEvent(
                        WorkflowEvents.StepCompensated,
                        workflowId,
                        workflowDefinition,
                        context,
                        compensationStep.Step.GetType(),
                        error: null,
                        compensatedAtUtc)),
                cancellationToken);
            return;
        }

        await PersistContextAsync(
            context,
            cancellationToken);

        await MarkStepCompensatedAsync(
            context,
            compensationStep,
            cancellationToken);

        await PublishAsync(
            WorkflowEvents.StepCompensated,
            workflowId,
            workflowDefinition,
            context,
            compensationStep.Step.GetType(),
            error: null,
            cancellationToken);
    }

    private async Task JournalStepCompensationFailedAsync(
        WorkflowContext context,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        CompensationStep compensationStep,
        Exception error,
        DateTimeOffset completedAtUtc,
        CancellationToken cancellationToken)
    {
        if (CanUseJournal(context))
        {
            await AppendStepAttemptTransitionAsync(
                context,
                new WorkflowStepAttemptTransitionJournalEntry(
                    context.WorkflowInstanceId!.Value,
                    context.WorkflowInstanceVersion,
                    GetRequiredWorkflowLeaseOwner(context),
                    PayloadJson: SerializeContext(context),
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.CompensationFailed,
                        compensationStep.Step.GetType().Name,
                        compensationStep.Order,
                        compensationStep.Attempt,
                        ErrorMessage: error.Message,
                        CompensatedAtUtc: completedAtUtc),
                    Event: CreateWorkflowEvent(
                        WorkflowEvents.CompensationFailed,
                        workflowId,
                        workflowDefinition,
                        context,
                        compensationStep.Step.GetType(),
                        error,
                        completedAtUtc),
                    State: new WorkflowStateJournalEntry(
                        WorkflowState.CompensationFailed,
                        error.Message,
                        completedAtUtc,
                        ClearLease: true)),
                cancellationToken);
            return;
        }

        await PersistContextAsync(
            context,
            cancellationToken);

        await MarkStepCompensationFailedAsync(
            context,
            compensationStep,
            error,
            cancellationToken);

        await PersistWorkflowStateAsync(
            context,
            WorkflowState.CompensationFailed,
            error,
            completedAtUtc,
            cancellationToken);

        await PublishAsync(
            WorkflowEvents.CompensationFailed,
            workflowId,
            workflowDefinition,
            context,
            compensationStep.Step.GetType(),
            error,
            cancellationToken);
    }

    private async Task AppendWorkflowStateTransitionAsync(
        WorkflowContext context,
        WorkflowStateTransitionJournalEntry entry,
        CancellationToken cancellationToken)
    {
        var result = await _workflowJournal!.AppendWorkflowStateTransitionAsync(
            entry,
            cancellationToken);

        context.WorkflowInstanceVersion = result.WorkflowInstanceVersion;
    }

    private async Task AppendStepAttemptTransitionAsync(
        WorkflowContext context,
        WorkflowStepAttemptTransitionJournalEntry entry,
        CancellationToken cancellationToken)
    {
        var result = await _workflowJournal!.AppendStepAttemptTransitionAsync(
            entry,
            cancellationToken);

        context.WorkflowInstanceVersion = result.WorkflowInstanceVersion;
    }

    private bool CanUseJournal(WorkflowContext context) =>
        _workflowJournal is not null &&
        context.WorkflowInstanceId.HasValue &&
        context.WorkflowInstanceVersion > 0;

    private static string GetRequiredWorkflowLeaseOwner(WorkflowContext context)
    {
        if (string.IsNullOrWhiteSpace(context.WorkflowLeaseOwner))
        {
            throw new InvalidOperationException(
                "Workflow journal writes require an active workflow lease owner.");
        }

        return context.WorkflowLeaseOwner;
    }

    private static string SerializeContext(WorkflowContext context) =>
        JsonSerializer.Serialize(
            context.Items,
            SerializerOptions);

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
        var workflowEvent = CreateWorkflowEvent(
            eventName,
            workflowId,
            workflowDefinition.Name,
            context,
            stepType,
            error);

        return _workflowEventPublisher.PublishAsync(
            workflowEvent,
            cancellationToken);
    }

    private static WorkflowEvent CreateWorkflowEvent(
        string eventName,
        string workflowId,
        IWorkflowDefinition workflowDefinition,
        WorkflowContext context,
        Type? stepType,
        Exception? error,
        DateTimeOffset? occurredAtUtc = null) =>
        CreateWorkflowEvent(
            eventName,
            workflowId,
            workflowDefinition.Name,
            context,
            stepType,
            error,
            occurredAtUtc);

    private static WorkflowEvent CreateWorkflowEvent(
        string eventName,
        string workflowId,
        string workflowName,
        WorkflowContext context,
        Type? stepType,
        Exception? error,
        DateTimeOffset? occurredAtUtc = null) =>
        new(
            eventName,
            workflowId,
            workflowName,
            context.CorrelationId,
            context.State,
            stepType?.Name,
            error,
            occurredAtUtc ?? DateTimeOffset.UtcNow)
        {
            WorkflowInstanceId = context.WorkflowInstanceId
        };

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

    private static int GetNextStepAttempt(
        IEnumerable<WorkflowStepAttemptRecord> attempts,
        int stepOrder) =>
        attempts
            .Where(attempt => attempt.StepOrder == stepOrder)
            .Select(attempt => attempt.Attempt)
            .DefaultIfEmpty(0)
            .Max() + 1;

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
