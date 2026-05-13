using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain.Workflows;
using Hemi.Presentation.BackgroundWorkers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hemi.Tests.Presentation;

public sealed class WorkflowWorkerServiceTests
{
    [Fact]
    public async Task Worker_hydrates_context_from_payload_and_finalizes_terminal_state()
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
            store,
            WorkflowPolicies.NoRetry);

        await worker.StartAsync(CancellationToken.None);

        var context = await dispatcher.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.True(context.Items.TryGetValue("marker", out var marker));
        var markerJson = Assert.IsType<JsonElement>(marker);
        Assert.Equal("from-payload", markerJson.GetString());
        Assert.Equal(WorkflowState.Succeeded, store.State);
        Assert.Equal(instance.Version + 1, store.StateExpectedVersion);
        Assert.Null(store.NextAttemptAtUtc);
        Assert.NotNull(store.CompletedAtUtc);
        Assert.Contains(@"""processed"":true", store.PayloadJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Worker_schedules_retry_for_failed_eligible_attempt()
    {
        var instance = CreateInstance(
            attempt: 1,
            version: 4,
            payloadJson: """{"marker":"retry"}""");
        var store = new PollingWorkflowInstanceStore(instance);
        var dispatcher = new RecordingDispatcher(context =>
        {
            context.State = WorkflowState.Failed;
            throw new InvalidOperationException("temporary workflow failure");
        });
        var policies = new WorkflowPolicies(
            maxRetryAttempts: 2,
            retryDelay: TimeSpan.FromMilliseconds(10),
            enableCompensation: false,
            stopOnFirstFailure: true,
            timeout: TimeSpan.FromSeconds(30));

        using var services = CreateServices(dispatcher);
        using var worker = CreateWorker(
            services,
            store,
            policies);

        await worker.StartAsync(CancellationToken.None);

        _ = await store.StateUpdated.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(WorkflowState.Pending, store.State);
        Assert.Equal("temporary workflow failure", store.LastError);
        Assert.Null(store.CompletedAtUtc);
        Assert.NotNull(store.NextAttemptAtUtc);
        Assert.Equal(instance.Version + 1, store.StateExpectedVersion);
    }

    private static WorkflowWorkerService CreateWorker(
        ServiceProvider services,
        IWorkflowInstanceStore store,
        WorkflowPolicies policies) =>
        new(
            services.GetRequiredService<IServiceScopeFactory>(),
            store,
            new StaticRetryPolicyProvider(policies),
            NullLogger<WorkflowWorkerService>.Instance);

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
        string payloadJson) =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "test-workflow",
            "test-workflow",
            Guid.NewGuid().ToString("D"),
            WorkflowState.Running,
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
            LeaseUntilUtc: DateTimeOffset.UtcNow.AddMinutes(5));

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

    private sealed class StaticRetryPolicyProvider(
        WorkflowPolicies policies)
        : IRetryPolicyProvider
    {
        public WorkflowPolicies GetPolicy(string workflowId) => policies;
    }

    private sealed class PollingWorkflowInstanceStore(
        WorkflowInstanceRecord instance)
        : IWorkflowInstanceStore
    {
        private bool _claimed;

        public TaskCompletionSource<bool> StateUpdated { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public WorkflowState? State { get; private set; }

        public int? StateExpectedVersion { get; private set; }

        public string? PayloadJson { get; private set; } = instance.PayloadJson;

        public string? LastError { get; private set; }

        public DateTimeOffset? CompletedAtUtc { get; private set; }

        public DateTimeOffset? NextAttemptAtUtc { get; private set; }

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
