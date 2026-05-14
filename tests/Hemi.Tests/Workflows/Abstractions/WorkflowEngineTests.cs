using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Execution;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain.Workflows;
using Microsoft.Extensions.DependencyInjection;

namespace Hemi.Tests.Workflows.Abstractions;

public sealed class WorkflowEngineTests
{
    [Fact]
    public async Task ExecuteAsync_runs_registered_steps_in_order()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new StepRecorder());
        services.AddTransient<FirstStep>();
        services.AddTransient<SecondStep>();

        var publisher = new RecordingWorkflowEventPublisher();
        var engine = CreateEngine(services, publisher);
        var context = new WorkflowContext("test-workflow", "correlation-1");
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep),
            typeof(SecondStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        var recorder = services.BuildServiceProvider()
            .GetRequiredService<StepRecorder>();

        Assert.Equal(WorkflowState.Succeeded, context.State);
        Assert.Equal(["first", "second"], recorder.Executed);
        Assert.Contains(
            publisher.Events,
            workflowEvent => workflowEvent.EventName == WorkflowEvents.WorkflowSucceeded);
    }

    [Fact]
    public async Task ExecuteAsync_compensates_completed_steps_when_a_later_step_fails()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        services.AddSingleton(recorder);
        services.AddTransient<CompensableStep>();
        services.AddTransient<FailingStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-2");
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(CompensableStep),
            typeof(FailingStep));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync("test-workflow", definition, context));

        Assert.Equal("Step failed.", exception.Message);
        Assert.Equal(WorkflowState.Compensated, context.State);
        Assert.Equal(["compensable", "compensated"], recorder.Executed);
    }

    [Fact]
    public async Task ExecuteAsync_records_step_lifecycle_and_persists_context()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore();
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FirstStep>();
        services.AddTransient<SecondStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var workflowInstanceId = Guid.NewGuid();
        var context = new WorkflowContext("test-workflow", "correlation-3")
        {
            WorkflowInstanceId = workflowInstanceId,
            WorkflowInstanceVersion = 1,
            WorkflowAttempt = 2,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep),
            typeof(SecondStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Equal(["first", "second"], recorder.Executed);
        Assert.Equal(
            ["running:1:1", "succeeded:1:1", "running:2:1", "succeeded:2:1"],
            logStore.Transitions);
        Assert.Equal(2, instanceStore.Payloads.Count);
        Assert.Contains(WorkflowState.Succeeded, instanceStore.States);
        Assert.Equal(4, context.WorkflowInstanceVersion);
    }

    [Fact]
    public async Task ExecuteAsync_persists_success_before_publishing_succeeded_event()
    {
        var services = new ServiceCollection();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore();
        services.AddSingleton(new StepRecorder());
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FirstStep>();

        var publisher = new RecordingWorkflowEventPublisher(workflowEvent =>
        {
            if (workflowEvent.EventName == WorkflowEvents.WorkflowSucceeded)
            {
                Assert.Contains(WorkflowState.Succeeded, instanceStore.States);
            }
        });
        var engine = CreateEngine(services, publisher);
        var context = new WorkflowContext("test-workflow", "correlation-success")
        {
            WorkflowInstanceId = Guid.NewGuid(),
            WorkflowInstanceVersion = 1,
            WorkflowAttempt = 1,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Equal(WorkflowState.Succeeded, instanceStore.States.Last());
        Assert.Contains(
            publisher.Events,
            workflowEvent => workflowEvent.EventName == WorkflowEvents.WorkflowSucceeded);
    }

    [Fact]
    public async Task ExecuteAsync_routes_persistent_transitions_through_journal()
    {
        var services = new ServiceCollection();
        var journal = new RecordingWorkflowJournal();
        services.AddSingleton(new StepRecorder());
        services.AddSingleton<IWorkflowJournal>(journal);
        services.AddTransient<FirstStep>();

        var publisher = new RecordingWorkflowEventPublisher();
        var engine = CreateEngine(services, publisher);
        var context = new WorkflowContext("test-workflow", "correlation-journal")
        {
            WorkflowInstanceId = Guid.NewGuid(),
            WorkflowInstanceVersion = 1,
            WorkflowAttempt = 1,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Empty(publisher.Events);
        Assert.Contains(
            journal.Entries,
            entry =>
                entry.Step?.Action == WorkflowStepJournalAction.Running &&
                entry.Event?.EventName == WorkflowEvents.StepStarted);
        Assert.Contains(
            journal.Entries,
            entry =>
                entry.PayloadJson is not null &&
                entry.Step?.Action == WorkflowStepJournalAction.Succeeded &&
                entry.Event?.EventName == WorkflowEvents.StepCompleted);
        Assert.Contains(
            journal.Entries,
            entry =>
                entry.State?.State == WorkflowState.Succeeded &&
                entry.Event?.EventName == WorkflowEvents.WorkflowSucceeded);
        Assert.Equal(3, context.WorkflowInstanceVersion);
    }

    [Fact]
    public async Task ExecuteAsync_records_failed_step_lifecycle_and_persists_workflow_failure()
    {
        var services = new ServiceCollection();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore();
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FailingStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-failed-step")
        {
            WorkflowInstanceId = Guid.NewGuid(),
            WorkflowInstanceVersion = 5,
            WorkflowAttempt = 3,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FailingStep));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync("test-workflow", definition, context));

        Assert.Equal("Step failed.", exception.Message);
        Assert.Equal(WorkflowState.Compensated, context.State);
        Assert.Equal(["running:1:1", "failed:1:1"], logStore.Transitions);
        Assert.Contains(WorkflowState.Failed, instanceStore.States);
    }

    [Fact]
    public async Task ExecuteAsync_records_each_internal_step_retry_as_separate_attempt()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore();
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FlakyStep>();

        var retryPolicy = new WorkflowPolicies(
            maxRetryAttempts: 1,
            retryDelay: TimeSpan.FromMilliseconds(1),
            enableCompensation: true,
            stopOnFirstFailure: true,
            timeout: TimeSpan.FromMinutes(5));
        var engine = CreateEngine(
            services,
            new RecordingWorkflowEventPublisher(),
            retryPolicy);
        var context = new WorkflowContext("test-workflow", "correlation-retry")
        {
            WorkflowInstanceId = Guid.NewGuid(),
            WorkflowInstanceVersion = 1,
            WorkflowAttempt = 5,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FlakyStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Equal(["flaky-1", "flaky-2"], recorder.Executed);
        Assert.Equal(
            ["running:1:1", "failed:1:1", "running:1:2", "succeeded:1:2"],
            logStore.Transitions);
        Assert.Equal(WorkflowState.Succeeded, context.State);
    }

    [Fact]
    public async Task ExecuteAsync_resumes_by_skipping_persisted_succeeded_steps()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var logStore = new RecordingWorkflowExecutionLogStore(
        [
            new WorkflowStepAttemptRecord(
                Guid.NewGuid(),
                WorkflowInstanceId: Guid.Parse("22222222-2222-2222-2222-222222222222"),
                StepName: nameof(FirstStep),
                StepOrder: 1,
                WorkflowStepAttemptStatus.Succeeded,
                Attempt: 1,
                CommandId: Guid.NewGuid(),
                ErrorMessage: null,
                StartedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-5),
                CompletedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-4),
                CompensatedAtUtc: null)
        ]);
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FirstStep>();
        services.AddTransient<SecondStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-resume")
        {
            WorkflowInstanceId = Guid.Parse("22222222-2222-2222-2222-222222222222"),
            WorkflowInstanceVersion = 2,
            WorkflowAttempt = 2,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep),
            typeof(SecondStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Equal(["second"], recorder.Executed);
        Assert.Equal(["running:2:1", "succeeded:2:1"], logStore.Transitions);
        Assert.Equal(WorkflowState.Succeeded, context.State);
    }

    [Fact]
    public async Task ExecuteAsync_continues_step_attempt_numbers_from_persisted_history()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var workflowInstanceId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var logStore = new RecordingWorkflowExecutionLogStore(
        [
            new WorkflowStepAttemptRecord(
                Guid.NewGuid(),
                workflowInstanceId,
                nameof(SecondStep),
                StepOrder: 2,
                WorkflowStepAttemptStatus.Failed,
                Attempt: 2,
                CommandId: Guid.NewGuid(),
                ErrorMessage: "previous failure",
                StartedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-5),
                CompletedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-4),
                CompensatedAtUtc: null)
        ]);
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<FirstStep>();
        services.AddTransient<SecondStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-resume-failed")
        {
            WorkflowInstanceId = workflowInstanceId,
            WorkflowInstanceVersion = 2,
            WorkflowAttempt = 7,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep),
            typeof(SecondStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        Assert.Equal(["first", "second"], recorder.Executed);
        Assert.Contains("running:2:3", logStore.Transitions);
        Assert.Contains("succeeded:2:3", logStore.Transitions);
    }

    [Fact]
    public async Task ExecuteAsync_persists_compensation_state_and_step_logs()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore();
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<CompensableStep>();
        services.AddTransient<FailingStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-compensation")
        {
            WorkflowInstanceId = Guid.NewGuid(),
            WorkflowInstanceVersion = 1,
            WorkflowAttempt = 1,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(CompensableStep),
            typeof(FailingStep));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync("test-workflow", definition, context));

        Assert.Equal("Step failed.", exception.Message);
        Assert.Equal(WorkflowState.Compensated, context.State);
        Assert.Equal(["compensable", "compensated"], recorder.Executed);
        Assert.Contains("running:1:1", logStore.Transitions);
        Assert.Contains("succeeded:1:1", logStore.Transitions);
        Assert.Contains("running:2:1", logStore.Transitions);
        Assert.Contains("failed:2:1", logStore.Transitions);
        Assert.Contains("compensated:1:1", logStore.Transitions);
        Assert.Contains(WorkflowState.Failed, instanceStore.States);
        Assert.Contains(WorkflowState.Compensating, instanceStore.States);
        Assert.Contains(WorkflowState.Compensated, instanceStore.States);
        Assert.True(instanceStore.Payloads.Count >= 2);
    }

    [Fact]
    public async Task ExecuteAsync_skips_persisted_succeeded_steps_and_compensates_them_on_failure()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        var instanceStore = new RecordingWorkflowInstanceStore();
        var logStore = new RecordingWorkflowExecutionLogStore(
        [
            new WorkflowStepAttemptRecord(
                Guid.NewGuid(),
                WorkflowInstanceId: Guid.Parse("11111111-1111-1111-1111-111111111111"),
                StepName: nameof(CompensableStep),
                StepOrder: 1,
                WorkflowStepAttemptStatus.Succeeded,
                Attempt: 1,
                CommandId: Guid.NewGuid(),
                ErrorMessage: null,
                StartedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-5),
                CompletedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-4),
                CompensatedAtUtc: null)
        ]);
        services.AddSingleton(recorder);
        services.AddSingleton<IWorkflowInstanceStore>(instanceStore);
        services.AddSingleton<IWorkflowExecutionLogStore>(logStore);
        services.AddTransient<CompensableStep>();
        services.AddTransient<FailingStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var workflowInstanceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var context = new WorkflowContext("test-workflow", "correlation-4")
        {
            WorkflowInstanceId = workflowInstanceId,
            WorkflowInstanceVersion = 10,
            WorkflowAttempt = 2,
            CommandId = Guid.NewGuid()
        };
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(CompensableStep),
            typeof(FailingStep));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync("test-workflow", definition, context));

        Assert.Equal("Step failed.", exception.Message);
        Assert.Equal(["compensated"], recorder.Executed);
        Assert.Contains("running:2:1", logStore.Transitions);
        Assert.Contains("failed:2:1", logStore.Transitions);
        Assert.Contains("compensated:1:1", logStore.Transitions);
        Assert.Contains(WorkflowState.Compensated, instanceStore.States);
        Assert.True(instanceStore.Payloads.Count >= 2);
    }

    private static WorkflowEngine CreateEngine(
        IServiceCollection services,
        IWorkflowEventPublisher eventPublisher,
        WorkflowPolicies? policy = null)
    {
        var provider = services.BuildServiceProvider();
        var retryPolicyProvider = new RetryPolicyProvider(
        [
            new WorkflowPolicyRegistration(
                "test-workflow",
                policy ?? WorkflowPolicies.NoRetry)
        ]);

        return new WorkflowEngine(
            provider,
            retryPolicyProvider,
            eventPublisher);
    }

    private sealed class StepRecorder
    {
        public List<string> Executed { get; } = [];
    }

    private sealed class FirstStep(StepRecorder recorder)
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("first");
            return Task.CompletedTask;
        }
    }

    private sealed class SecondStep(StepRecorder recorder)
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("second");
            return Task.CompletedTask;
        }
    }

    private sealed class FlakyStep(StepRecorder recorder)
        : IWorkflowStep<WorkflowContext>
    {
        private int _executions;

        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            _executions++;
            recorder.Executed.Add($"flaky-{_executions}");

            if (_executions == 1)
            {
                throw new InvalidOperationException("Transient step failure.");
            }

            return Task.CompletedTask;
        }
    }

    private sealed class CompensableStep(StepRecorder recorder)
        : ICompensableWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("compensable");
            return Task.CompletedTask;
        }

        public Task CompensateAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("compensated");
            return Task.CompletedTask;
        }
    }

    private sealed class FailingStep
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Step failed.");
    }

    private sealed class RecordingWorkflowInstanceStore
        : IWorkflowInstanceStore
    {
        public List<string> Payloads { get; } = [];

        public List<WorkflowState> States { get; } = [];

        public Task<WorkflowStartResult> StartWorkflowAsync(
            WorkflowStartRequest request,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
            Guid id,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<WorkflowInstanceRecord?>(null);

        public Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
            string workflowId,
            string correlationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<WorkflowInstanceRecord?>(null);

        public Task<IReadOnlyCollection<WorkflowInstanceRecord>> ClaimDueInstancesAsync(
            DateTimeOffset nowUtc,
            string leaseOwner,
            TimeSpan leaseDuration,
            int batchSize = 10,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<WorkflowInstanceRecord>>([]);

        public Task<bool> TryUpdateStateAsync(
            Guid id,
            WorkflowState state,
            int expectedVersion,
            string? lastError = null,
            DateTimeOffset? completedAtUtc = null,
            DateTimeOffset? nextAttemptAtUtc = null,
            CancellationToken cancellationToken = default)
        {
            States.Add(state);
            return Task.FromResult(true);
        }

        public Task<bool> TryUpdatePayloadAsync(
            Guid id,
            int expectedVersion,
            string payloadJson,
            CancellationToken cancellationToken = default)
        {
            Payloads.Add(payloadJson);
            return Task.FromResult(true);
        }

        public Task<bool> TryReleaseLeaseAsync(
            Guid id,
            string leaseOwner,
            int expectedVersion,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(true);
    }

    private sealed class RecordingWorkflowExecutionLogStore
        : IWorkflowExecutionLogStore
    {
        private readonly IReadOnlyCollection<WorkflowStepAttemptRecord> _attempts;

        public RecordingWorkflowExecutionLogStore()
            : this([])
        {
        }

        public RecordingWorkflowExecutionLogStore(
            IReadOnlyCollection<WorkflowStepAttemptRecord> attempts)
        {
            _attempts = attempts;
        }

        public List<string> Transitions { get; } = [];

        public Task<IReadOnlyCollection<WorkflowStepAttemptRecord>> GetStepAttemptsAsync(
            Guid workflowInstanceId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(_attempts);

        public Task<WorkflowStepAttemptRecord> MarkStepRunningAsync(
            WorkflowStepAttemptStart request,
            CancellationToken cancellationToken = default)
        {
            Transitions.Add($"running:{request.StepOrder}:{request.Attempt}");
            return Task.FromResult(new WorkflowStepAttemptRecord(
                Guid.NewGuid(),
                request.WorkflowInstanceId,
                request.StepName,
                request.StepOrder,
                WorkflowStepAttemptStatus.Running,
                request.Attempt,
                request.CommandId,
                ErrorMessage: null,
                request.StartedAtUtc,
                CompletedAtUtc: null,
                CompensatedAtUtc: null));
        }

        public Task<bool> MarkStepSucceededAsync(
            Guid workflowInstanceId,
            int stepOrder,
            int attempt,
            DateTimeOffset completedAtUtc,
            CancellationToken cancellationToken = default)
        {
            Transitions.Add($"succeeded:{stepOrder}:{attempt}");
            return Task.FromResult(true);
        }

        public Task<bool> MarkStepFailedAsync(
            Guid workflowInstanceId,
            int stepOrder,
            int attempt,
            string errorMessage,
            DateTimeOffset completedAtUtc,
            CancellationToken cancellationToken = default)
        {
            Transitions.Add($"failed:{stepOrder}:{attempt}");
            return Task.FromResult(true);
        }

        public Task<bool> MarkStepCompensatedAsync(
            Guid workflowInstanceId,
            int stepOrder,
            int attempt,
            DateTimeOffset compensatedAtUtc,
            CancellationToken cancellationToken = default)
        {
            Transitions.Add($"compensated:{stepOrder}:{attempt}");
            return Task.FromResult(true);
        }

        public Task<bool> MarkStepCompensationFailedAsync(
            Guid workflowInstanceId,
            int stepOrder,
            int attempt,
            string errorMessage,
            DateTimeOffset compensatedAtUtc,
            CancellationToken cancellationToken = default)
        {
            Transitions.Add($"compensation-failed:{stepOrder}:{attempt}");
            return Task.FromResult(true);
        }
    }

    private sealed class RecordingWorkflowJournal : IWorkflowJournal
    {
        public List<WorkflowJournalEntry> Entries { get; } = [];

        public Task<WorkflowJournalResult> AppendAsync(
            WorkflowJournalEntry entry,
            CancellationToken cancellationToken = default)
        {
            Entries.Add(entry);

            var version = entry.ExpectedWorkflowVersion;
            if (entry.PayloadJson is not null)
            {
                version++;
            }

            if (entry.State is not null)
            {
                version++;
            }

            return Task.FromResult(new WorkflowJournalResult(version));
        }
    }

    private sealed class RecordingWorkflowEventPublisher(
        Action<WorkflowEvent>? onPublish = null)
        : IWorkflowEventPublisher
    {
        public List<WorkflowEvent> Events { get; } = [];

        public Task PublishAsync(
            WorkflowEvent workflowEvent,
            CancellationToken cancellationToken = default)
        {
            onPublish?.Invoke(workflowEvent);
            Events.Add(workflowEvent);
            return Task.CompletedTask;
        }
    }
}
