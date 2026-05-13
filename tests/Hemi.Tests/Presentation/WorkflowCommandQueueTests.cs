using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain.Workflows;
using Hemi.Presentation.BackgroundWorkers;

namespace Hemi.Tests.Presentation;

public sealed class WorkflowCommandQueueTests
{
    [Fact]
    public async Task EnqueueAsync_returns_existing_instance_for_same_idempotency_key_and_hash()
    {
        var queue = CreateQueue();
        var request = CreateCommand(
            correlationId: "correlation-1",
            idempotencyKey: "idempotency-1",
            marker: "same-request");

        var firstResponse = await queue.EnqueueAsync(request);
        var secondResponse = await queue.EnqueueAsync(request);

        Assert.Equal(firstResponse.WorkflowInstanceId, secondResponse.WorkflowInstanceId);
        Assert.Equal(firstResponse.CommandId, secondResponse.CommandId);
        Assert.Equal(firstResponse.WorkflowId, secondResponse.WorkflowId);
        Assert.Equal(firstResponse.CorrelationId, secondResponse.CorrelationId);
        Assert.Equal(firstResponse.AcceptedAtUtc, secondResponse.AcceptedAtUtc);
    }

    [Fact]
    public async Task EnqueueAsync_rejects_same_idempotency_key_with_different_hash()
    {
        var queue = CreateQueue();
        var idempotencyKey = "idempotency-conflict";

        _ = await queue.EnqueueAsync(CreateCommand(
            correlationId: "correlation-2",
            idempotencyKey,
            marker: "first"));

        var exception = await Assert.ThrowsAsync<WorkflowStartConflictException>(
            () => queue.EnqueueAsync(CreateCommand(
                correlationId: "correlation-3",
                idempotencyKey,
                marker: "second")));

        Assert.Equal("workflow.idempotency_conflict", exception.Code);
    }

    [Fact]
    public async Task EnqueueAsync_rejects_same_workflow_correlation_with_different_hash()
    {
        var queue = CreateQueue();
        const string correlationId = "correlation-conflict";

        _ = await queue.EnqueueAsync(CreateCommand(
            correlationId,
            idempotencyKey: "idempotency-first",
            marker: "first"));

        var exception = await Assert.ThrowsAsync<WorkflowStartConflictException>(
            () => queue.EnqueueAsync(CreateCommand(
                correlationId,
                idempotencyKey: "idempotency-second",
                marker: "second")));

        Assert.Equal("workflow.correlation_conflict", exception.Code);
    }

    private static WorkflowCommandQueue CreateQueue() =>
        new(
            new InMemoryWorkflowInstanceStore(),
            new WorkflowRegistry(
            [
                WorkflowDefinition.Create("test-workflow")
            ]));

    private static StartWorkflowCommand CreateCommand(
        string correlationId,
        string idempotencyKey,
        string marker) =>
        new(
            "test-workflow",
            correlationId,
            new Dictionary<string, object?>
            {
                ["marker"] = marker
            },
            idempotencyKey,
            "unit-tests",
            DateTimeOffset.Parse("2026-05-13T00:00:00+00:00"));

    private sealed class InMemoryWorkflowInstanceStore : IWorkflowInstanceStore
    {
        private readonly Dictionary<string, WorkflowInstanceRecord> _instancesByIdempotencyKey =
            new(StringComparer.Ordinal);
        private readonly Dictionary<string, WorkflowInstanceRecord> _instancesByCorrelation =
            new(StringComparer.Ordinal);

        public Task<WorkflowStartResult> StartWorkflowAsync(
            WorkflowStartRequest request,
            CancellationToken cancellationToken = default)
        {
            if (_instancesByIdempotencyKey.TryGetValue(
                    request.IdempotencyKey,
                    out var existingByIdempotency))
            {
                return Task.FromResult(SameHash(existingByIdempotency, request.RequestHash)
                    ? new WorkflowStartResult(
                        WorkflowStartStatus.Existing,
                        existingByIdempotency,
                        existingByIdempotency.RequestHash)
                    : new WorkflowStartResult(
                        WorkflowStartStatus.IdempotencyConflict,
                        existingByIdempotency,
                        existingByIdempotency.RequestHash));
            }

            var correlationKey = CreateCorrelationKey(
                request.WorkflowId,
                request.CorrelationId);

            if (_instancesByCorrelation.TryGetValue(
                    correlationKey,
                    out var existingByCorrelation))
            {
                return Task.FromResult(SameHash(existingByCorrelation, request.RequestHash)
                    ? new WorkflowStartResult(
                        WorkflowStartStatus.Existing,
                        existingByCorrelation,
                        existingByCorrelation.RequestHash)
                    : new WorkflowStartResult(
                        WorkflowStartStatus.CorrelationConflict,
                        existingByCorrelation,
                        existingByCorrelation.RequestHash));
            }

            var instance = new WorkflowInstanceRecord(
                Guid.NewGuid(),
                Guid.NewGuid(),
                request.WorkflowId,
                request.WorkflowName,
                request.CorrelationId,
                WorkflowState.Pending,
                request.PayloadJson,
                LastError: null,
                Version: 1,
                request.RequestedAtUtc,
                request.RequestedAtUtc,
                CompletedAtUtc: null,
                request.IdempotencyKey,
                request.RequestHash,
                request.RequestedBy,
                Attempt: 0,
                request.NextAttemptAtUtc,
                LeaseOwner: null,
                LeaseUntilUtc: null);

            _instancesByIdempotencyKey.Add(
                request.IdempotencyKey,
                instance);
            _instancesByCorrelation.Add(
                correlationKey,
                instance);

            return Task.FromResult(new WorkflowStartResult(
                WorkflowStartStatus.Created,
                instance,
                instance.RequestHash));
        }

        public Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
            Guid id,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<WorkflowInstanceRecord?>(
                _instancesByCorrelation.Values.FirstOrDefault(instance => instance.Id == id));

        public Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
            string workflowId,
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            _instancesByCorrelation.TryGetValue(
                CreateCorrelationKey(workflowId, correlationId),
                out var instance);

            return Task.FromResult(instance);
        }

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
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public Task<bool> TryUpdatePayloadAsync(
            Guid id,
            int expectedVersion,
            string payloadJson,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public Task<bool> TryReleaseLeaseAsync(
            Guid id,
            string leaseOwner,
            int expectedVersion,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        private static string CreateCorrelationKey(
            string workflowId,
            string correlationId) =>
            $"{workflowId}:{correlationId}";

        private static bool SameHash(
            WorkflowInstanceRecord instance,
            string requestHash) =>
            string.Equals(
                instance.RequestHash,
                requestHash,
                StringComparison.Ordinal);
    }
}
