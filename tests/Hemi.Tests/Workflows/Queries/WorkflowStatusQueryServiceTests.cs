using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Queries;
using Hemi.Domain.Workflows;

namespace Hemi.Tests.Workflows.Queries;

public sealed class WorkflowStatusQueryServiceTests
{
    [Fact]
    public async Task GetStatusAsync_projects_payload_and_latest_attempt_for_each_step()
    {
        var instanceId = Guid.NewGuid();
        var commandId = Guid.NewGuid();
        var acceptedAt = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var store = new StubExecutionLogStore(
        [
            Attempt(instanceId, "payment", 2, 1, WorkflowStepAttemptStatus.Failed),
            Attempt(instanceId, "kitchen", 1, 1, WorkflowStepAttemptStatus.Succeeded),
            Attempt(instanceId, "payment", 2, 2, WorkflowStepAttemptStatus.Succeeded)
        ]);
        var service = new WorkflowStatusQueryService(store);
        var instance = new WorkflowInstanceRecord(
            instanceId,
            commandId,
            "order-fulfillment",
            "Order fulfillment",
            "order-42",
            WorkflowState.Running,
            "{\"orderId\":\"order-42\",\"amount\":12.5}",
            null,
            3,
            acceptedAt,
            acceptedAt.AddMinutes(1),
            null,
            "request-42",
            "hash",
            "tests",
            1,
            null,
            null,
            null);

        var result = await service.GetStatusAsync(instance);

        Assert.Equal(instanceId, result.WorkflowInstanceId);
        Assert.Equal(commandId, result.CommandId);
        Assert.Equal("order-42", result.Items["orderId"]?.ToString());
        Assert.Equal("12.5", result.Items["amount"]?.ToString());
        Assert.Collection(
            result.Steps,
            step => Assert.Equal((1, "kitchen", 1), (step.Order, step.Name, step.Attempt)),
            step => Assert.Equal((2, "payment", 2), (step.Order, step.Name, step.Attempt)));
        Assert.Equal(WorkflowStepAttemptStatus.Succeeded, result.Steps.Last().Status);
    }

    [Fact]
    public async Task GetStatusAsync_treats_blank_payload_as_empty_items()
    {
        var instance = new WorkflowInstanceRecord(
            Guid.NewGuid(), Guid.NewGuid(), "workflow", "Workflow", "correlation",
            WorkflowState.Pending, " ", null, 1,
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null,
            null, null, null, 0, null, null, null);

        var result = await new WorkflowStatusQueryService(
            new StubExecutionLogStore([])).GetStatusAsync(instance);

        Assert.Empty(result.Items);
        Assert.Empty(result.Steps);
    }

    private static WorkflowStepAttemptRecord Attempt(
        Guid instanceId,
        string name,
        int order,
        int attempt,
        WorkflowStepAttemptStatus status) =>
        new(
            Guid.NewGuid(), instanceId, name, order, status, attempt, null,
            status is WorkflowStepAttemptStatus.Failed ? "failed" : null,
            DateTimeOffset.UtcNow.AddMinutes(-attempt),
            DateTimeOffset.UtcNow,
            null);

    private sealed class StubExecutionLogStore(
        IReadOnlyCollection<WorkflowStepAttemptRecord> attempts)
        : IWorkflowExecutionLogStore
    {
        public Task<IReadOnlyCollection<WorkflowStepAttemptRecord>> GetStepAttemptsAsync(
            Guid workflowInstanceId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(attempts);

        public Task<WorkflowStepAttemptRecord> MarkStepRunningAsync(
            WorkflowStepAttemptStart request, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> MarkStepSucceededAsync(
            Guid workflowInstanceId, int stepOrder, int attempt,
            DateTimeOffset completedAtUtc, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> MarkStepFailedAsync(
            Guid workflowInstanceId, int stepOrder, int attempt, string errorMessage,
            DateTimeOffset completedAtUtc, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> MarkStepCompensatedAsync(
            Guid workflowInstanceId, int stepOrder, int attempt,
            DateTimeOffset compensatedAtUtc, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<bool> MarkStepCompensationFailedAsync(
            Guid workflowInstanceId, int stepOrder, int attempt, string errorMessage,
            DateTimeOffset compensatedAtUtc, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
