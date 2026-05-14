using System.Diagnostics.Metrics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.Monitoring;
using Hemi.Presentation.BackgroundWorkers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hemi.Tests.Presentation;

public sealed class WorkflowWorkerServiceTests
{
    [Fact]
    public async Task Worker_hydrates_context_from_payload_and_delegates_lifecycle_state_to_dispatcher()
    {
        var instance = CreateInstance(
            attempt: 1,
            version: 2,
            payloadJson: """{"marker":"from-payload"}""");
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(context =>
        {
            context.Set("processed", true);
            context.State = WorkflowState.Succeeded;
            return Task.CompletedTask;
        });

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store);

        await worker.StartAsync(CancellationToken.None);

        var context = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.True(context.Items.TryGetValue("marker", out var marker));
        var markerJson = Assert.IsType<JsonElement>(marker);
        Assert.Equal("from-payload", markerJson.GetString());
        Assert.Equal(instance.LeaseOwner, context.WorkflowLeaseOwner);
        Assert.Null(store.State);
        Assert.Null(store.StateExpectedVersion);
        Assert.Null(store.NextAttemptAtUtc);
        Assert.Null(store.CompletedAtUtc);
        Assert.DoesNotContain(@"""processed"":true", store.PayloadJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Worker_marks_non_terminal_dispatch_failure_failed()
    {
        var instance = CreateInstance(
            attempt: 1,
            version: 4,
            payloadJson: """{"marker":"retry"}""");
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(_ =>
            throw new InvalidOperationException("temporary workflow failure"));

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store);

        await worker.StartAsync(CancellationToken.None);

        _ = await store.StateUpdated.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(WorkflowState.Failed, store.State);
        Assert.Equal("temporary workflow failure", store.LastError);
        Assert.NotNull(store.CompletedAtUtc);
        Assert.Null(store.NextAttemptAtUtc);
        Assert.Equal(instance.Version + 1, store.StateExpectedVersion);
    }

    [Fact]
    public async Task Worker_does_not_record_failed_metric_for_pre_engine_dispatch_failure()
    {
        var outcomes = await CaptureCommandMetricOutcomesAsync(async metrics =>
        {
            var instance = CreateInstance(
                attempt: 1,
                version: 4,
                payloadJson: """{"marker":"pre-engine-failure"}""");
            var store = new PollingWorkflowInstanceStore(instance);
            var dispatcher = new RecordingDispatcher(_ =>
                throw new InvalidOperationException("dispatcher failed before terminal state"));

            using var services = CreateServices(dispatcher);
            using var worker = CreateWorker(
                services,
                store,
                metrics);

            await worker.StartAsync(CancellationToken.None);

            _ = await store.StateUpdated.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await worker.StopAsync(CancellationToken.None);
        });

        Assert.Contains("received", outcomes);
        Assert.DoesNotContain("failed", outcomes);
    }

    [Theory]
    [InlineData(WorkflowState.Succeeded)]
    [InlineData(WorkflowState.Failed)]
    [InlineData(WorkflowState.Compensated)]
    [InlineData(WorkflowState.CompensationFailed)]
    [InlineData(WorkflowState.Cancelled)]
    public async Task Worker_does_not_change_terminal_dispatch_failures_to_pending(
        WorkflowState terminalState)
    {
        var instance = CreateInstance(
            attempt: 1,
            version: 4,
            payloadJson: """{"marker":"terminal"}""");
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(context =>
        {
            context.State = terminalState;
            throw new InvalidOperationException("workflow already terminal");
        });

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store);

        await worker.StartAsync(CancellationToken.None);

        _ = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Null(store.State);
        Assert.DoesNotContain(WorkflowState.Pending, store.StateUpdates);
        Assert.Null(store.LastError);
        Assert.Null(store.CompletedAtUtc);
        Assert.Null(store.NextAttemptAtUtc);
    }

    [Fact]
    public async Task Worker_dispatch_exceptions_after_compensation_do_not_schedule_retries()
    {
        var instance = CreateInstance(
            attempt: 3,
            version: 7,
            payloadJson: """{"marker":"compensated"}""");
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(context =>
        {
            context.State = WorkflowState.Compensated;
            throw new InvalidOperationException("dispatch failed after compensation");
        });

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store);

        await worker.StartAsync(CancellationToken.None);

        _ = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Empty(store.StateUpdates);
        Assert.Null(store.NextAttemptAtUtc);
        Assert.DoesNotContain(
            store.PayloadUpdateJsons,
            payloadJson => payloadJson.Contains("dispatch failed after compensation", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Worker_expired_running_leases_resume_normally()
    {
        var expiredLeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(-2);
        var instance = CreateInstance(
            attempt: 5,
            version: 9,
            payloadJson: """{"marker":"expired-running-lease"}""",
            leaseUntilUtc: expiredLeaseUntilUtc);
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(context =>
        {
            Assert.Equal(WorkflowState.Running, context.State);
            context.Set("resumed", true);
            context.State = WorkflowState.Succeeded;
            return Task.CompletedTask;
        });

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store);

        await worker.StartAsync(CancellationToken.None);

        var context = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(WorkflowState.Running, store.ClaimedState);
        Assert.Equal(expiredLeaseUntilUtc, store.ClaimedLeaseUntilUtc);
        Assert.Equal(WorkflowState.Succeeded, context.State);
        Assert.Equal(instance.Attempt, context.WorkflowAttempt);
        Assert.Equal(instance.Version, context.WorkflowInstanceVersion);
        Assert.Equal(instance.LeaseOwner, context.WorkflowLeaseOwner);
        Assert.Null(store.State);
        Assert.DoesNotContain(@"""resumed"":true", store.PayloadJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Worker_records_failed_metric_for_terminal_dispatch_failure()
    {
        var outcomes = await CaptureCommandMetricOutcomesAsync(async metrics =>
        {
            var instance = CreateInstance(
                attempt: 1,
                version: 4,
                payloadJson: """{"marker":"terminal-failure"}""");
            var store = new PollingWorkflowInstanceStore(instance);
            var dispatcher = new RecordingDispatcher(context =>
            {
                context.State = WorkflowState.Failed;
                throw new InvalidOperationException("engine persisted terminal failure");
            });

            using var services = CreateServices(dispatcher);
            using var worker = CreateWorker(
                services,
                store,
                metrics);

            await worker.StartAsync(CancellationToken.None);

            _ = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await worker.StopAsync(CancellationToken.None);
        });

        Assert.Contains("received", outcomes);
        Assert.Contains("failed", outcomes);
    }

    private static WorkflowWorkerService CreateWorker(
        ServiceProvider services,
        IWorkflowInstanceStore store,
        WorkflowMetrics? workflowMetrics = null) =>
        new(
            services.GetRequiredService<IServiceScopeFactory>(),
            store,
            NullLogger<WorkflowWorkerService>.Instance,
            workflowMetrics);

    private static async Task<IReadOnlyCollection<string>> CaptureCommandMetricOutcomesAsync(
        Func<WorkflowMetrics, Task> action)
    {
        var outcomes = new List<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == WorkflowMetrics.MeterName &&
                instrument.Name == "hemi.workflow.commands")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            var outcome = GetTagValue(tags, "outcome");
            if (outcome is not null)
            {
                outcomes.Add(outcome);
            }
        });
        listener.Start();

        using var metrics = new WorkflowMetrics();
        await action(metrics);

        return outcomes;
    }

    private static string? GetTagValue(
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        string key)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal))
            {
                return tag.Value?.ToString();
            }
        }

        return null;
    }

    private static ServiceProvider CreateServices(
        IWorkflowDispatcher dispatcher)
    {
        var services = new ServiceCollection();
        services.AddSingleton(dispatcher);
        return services.BuildServiceProvider();
    }

    private static WorkflowInstanceRecord CreateInstance(
        int attempt,
        int version,
        string payloadJson,
        WorkflowState state = WorkflowState.Running,
        DateTimeOffset? leaseUntilUtc = null) =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "test-workflow",
            "test-workflow",
            Guid.NewGuid().ToString("D"),
            state,
            payloadJson,
            LastError: null,
            version,
            DateTimeOffset.UtcNow.AddSeconds(-5),
            DateTimeOffset.UtcNow,
            CompletedAtUtc: null,
            IdempotencyKey: $"worker-test-{Guid.NewGuid():N}",
            RequestHash: new string('a', 64),
            RequestedBy: "worker-tests",
            attempt,
            NextAttemptAtUtc: null,
            LeaseOwner: "worker-tests",
            LeaseUntilUtc: leaseUntilUtc ?? DateTimeOffset.UtcNow.AddMinutes(5));

    private sealed class RecordingDispatcher(
        Func<WorkflowContext, Task> handle)
        : IWorkflowDispatcher
    {
        public TaskCompletionSource<WorkflowContext> Handled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task DispatchAsync(
            string workflowId,
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            try
            {
                await handle(context);
                Handled.TrySetResult(context);
            }
            catch
            {
                Handled.TrySetResult(context);
                throw;
            }
        }
    }

    private sealed class PollingWorkflowInstanceStore(
        WorkflowInstanceRecord instance)
        : IWorkflowInstanceStore
    {
        private bool _claimed;

        public TaskCompletionSource<bool> StateUpdated { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public WorkflowState? State { get; private set; }

        public List<WorkflowState> StateUpdates { get; } = [];

        public int? StateExpectedVersion { get; private set; }

        public string? PayloadJson { get; private set; } = instance.PayloadJson;

        public List<string> PayloadUpdateJsons { get; } = [];

        public string? LastError { get; private set; }

        public DateTimeOffset? CompletedAtUtc { get; private set; }

        public DateTimeOffset? NextAttemptAtUtc { get; private set; }

        public WorkflowState? ClaimedState { get; private set; }

        public DateTimeOffset? ClaimedLeaseUntilUtc { get; private set; }

        public Task<WorkflowStartResult> StartWorkflowAsync(
            WorkflowStartRequest request,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
            Guid id,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<WorkflowInstanceRecord?>(instance.Id == id ? instance : null);

        public Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
            string workflowId,
            string correlationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<WorkflowInstanceRecord?>(
                instance.WorkflowId == workflowId &&
                instance.CorrelationId == correlationId
                    ? instance
                    : null);

        public Task<IReadOnlyCollection<WorkflowInstanceRecord>> ClaimDueInstancesAsync(
            DateTimeOffset nowUtc,
            string leaseOwner,
            TimeSpan leaseDuration,
            int batchSize = 10,
            CancellationToken cancellationToken = default)
        {
            if (_claimed)
            {
                return Task.FromResult<IReadOnlyCollection<WorkflowInstanceRecord>>([]);
            }

            _claimed = true;
            ClaimedState = instance.State;
            ClaimedLeaseUntilUtc = instance.LeaseUntilUtc;
            return Task.FromResult<IReadOnlyCollection<WorkflowInstanceRecord>>([instance]);
        }

        public Task<bool> TryUpdateStateAsync(
            Guid id,
            WorkflowState state,
            int expectedVersion,
            string? lastError = null,
            DateTimeOffset? completedAtUtc = null,
            DateTimeOffset? nextAttemptAtUtc = null,
            CancellationToken cancellationToken = default)
        {
            State = state;
            StateUpdates.Add(state);
            StateExpectedVersion = expectedVersion;
            LastError = lastError;
            CompletedAtUtc = completedAtUtc;
            NextAttemptAtUtc = nextAttemptAtUtc;
            StateUpdated.TrySetResult(true);
            return Task.FromResult(id == instance.Id);
        }

        public Task<bool> TryUpdatePayloadAsync(
            Guid id,
            int expectedVersion,
            string payloadJson,
            CancellationToken cancellationToken = default)
        {
            PayloadJson = payloadJson;
            PayloadUpdateJsons.Add(payloadJson);
            return Task.FromResult(
                id == instance.Id &&
                expectedVersion == instance.Version);
        }

        public Task<bool> TryReleaseLeaseAsync(
            Guid id,
            string leaseOwner,
            int expectedVersion,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);
    }
}
