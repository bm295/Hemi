using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.Messaging;

namespace Hemi.Tests.Infrastructure.Messaging;

public sealed class WorkflowOutboxPublisherTests
{
    [Fact]
    public async Task OutboxWorkflowEventPublisher_saves_lifecycle_event_as_outbox_message()
    {
        var outboxStore = new RecordingWorkflowOutboxStore();
        var publisher = new OutboxWorkflowEventPublisher(outboxStore);
        var workflowInstanceId = Guid.NewGuid();
        var occurredAtUtc = DateTimeOffset.UtcNow;

        await publisher.PublishAsync(
            new WorkflowEvent(
                WorkflowEvents.WorkflowSucceeded,
                "order-fulfillment",
                "Order Fulfillment",
                "order-123",
                WorkflowState.Succeeded,
                StepName: null,
                Error: null,
                occurredAtUtc)
            {
                WorkflowInstanceId = workflowInstanceId
            });

        var draft = Assert.Single(outboxStore.SavedMessages);
        Assert.Equal(workflowInstanceId, draft.WorkflowInstanceId);
        Assert.Equal(WorkflowEvents.WorkflowSucceeded, draft.MessageType);
        Assert.Equal(OutboxWorkflowEventPublisher.Destination, draft.Destination);
        Assert.Equal(occurredAtUtc, draft.CreatedAtUtc);
        Assert.Equal(occurredAtUtc, draft.NextAttemptAtUtc);

        using var payload = JsonDocument.Parse(draft.PayloadJson);
        Assert.Equal(
            WorkflowEvents.WorkflowSucceeded,
            payload.RootElement.GetProperty("eventName").GetString());
        Assert.Equal(
            "order-fulfillment",
            payload.RootElement.GetProperty("workflowId").GetString());

        using var headers = JsonDocument.Parse(draft.HeadersJson);
        Assert.Equal(
            workflowInstanceId.ToString("D"),
            headers.RootElement.GetProperty("workflow-instance-id").GetString());
    }

    [Fact]
    public async Task PublishPendingAsync_publishes_pending_messages_and_marks_published()
    {
        var message = CreateOutboxMessage();
        var outboxStore = new RecordingWorkflowOutboxStore([message]);
        var transport = new RecordingWorkflowMessagePublisher();
        var publisher = new WorkflowOutboxPublisher(outboxStore, transport);

        var publishedCount = await publisher.PublishPendingAsync(
            batchSize: 10,
            dueAtUtc: DateTimeOffset.UtcNow);

        Assert.Equal(1, publishedCount);

        var envelope = Assert.Single(transport.PublishedMessages);
        Assert.Equal(message.Id, envelope.MessageId);
        Assert.Equal(message.MessageType, envelope.MessageType);
        Assert.Equal(message.Destination, envelope.Destination);
        Assert.Equal("order-fulfillment", envelope.Headers["workflow-id"]);
        Assert.Equal(message.Id, Assert.Single(outboxStore.PublishedMessageIds));
        var claim = Assert.Single(outboxStore.Claims);
        Assert.Equal(10, claim.BatchSize);
        Assert.False(string.IsNullOrWhiteSpace(claim.LeaseOwner));
        Assert.True(claim.LeaseDuration > TimeSpan.Zero);
        Assert.Empty(outboxStore.FailedMessages);
    }

    [Fact]
    public async Task PublishPendingAsync_marks_transport_failure_with_retry_metadata()
    {
        var message = CreateOutboxMessage();
        var outboxStore = new RecordingWorkflowOutboxStore([message]);
        var transport = new RecordingWorkflowMessagePublisher(
            _ => throw new InvalidOperationException("transport unavailable"));
        var publisher = new WorkflowOutboxPublisher(outboxStore, transport);

        var publishedCount = await publisher.PublishPendingAsync(
            batchSize: 10,
            dueAtUtc: DateTimeOffset.UtcNow,
            maxRetryAttempts: 2,
            retryDelay: TimeSpan.FromMinutes(1));

        Assert.Equal(0, publishedCount);
        Assert.Empty(outboxStore.PublishedMessageIds);

        var failed = Assert.Single(outboxStore.FailedMessages);
        Assert.Equal(message.Id, failed.MessageId);
        Assert.Equal("transport unavailable", failed.ErrorMessage);
        Assert.NotNull(failed.NextAttemptAtUtc);
        Assert.True(failed.NextAttemptAtUtc > failed.LastAttemptAtUtc);

        var storedMessage = Assert.Single(outboxStore.Messages);
        Assert.Equal(WorkflowOutboxStatus.Pending, storedMessage.Status);
        Assert.Equal(message.RetryCount + 1, storedMessage.RetryCount);
        Assert.Equal("transport unavailable", storedMessage.ErrorMessage);
        Assert.NotNull(storedMessage.LastAttemptAtUtc);
        Assert.NotNull(storedMessage.NextAttemptAtUtc);
        Assert.Null(storedMessage.LeaseOwner);
        Assert.Null(storedMessage.LeaseUntilUtc);
    }

    [Fact]
    public async Task PublishPendingAsync_two_publishers_cannot_claim_same_message_concurrently()
    {
        var message = CreateOutboxMessage();
        var outboxStore = new RecordingWorkflowOutboxStore([message]);
        var transport = new RecordingWorkflowMessagePublisher(async _ =>
            await Task.Delay(TimeSpan.FromMilliseconds(50)));
        var firstPublisher = new WorkflowOutboxPublisher(outboxStore, transport);
        var secondPublisher = new WorkflowOutboxPublisher(outboxStore, transport);
        var dueAtUtc = DateTimeOffset.UtcNow;

        var publishedCounts = await Task.WhenAll(
            firstPublisher.PublishPendingAsync(
                batchSize: 10,
                dueAtUtc: dueAtUtc),
            secondPublisher.PublishPendingAsync(
                batchSize: 10,
                dueAtUtc: dueAtUtc));

        Assert.Equal(1, publishedCounts.Sum());
        Assert.Single(transport.PublishedMessages);
        Assert.Equal(message.Id, Assert.Single(outboxStore.PublishedMessageIds));
        Assert.Equal(2, outboxStore.Claims.Count);
        Assert.Contains(outboxStore.Claims, claim => claim.ClaimedMessageIds.Contains(message.Id));
        Assert.Contains(outboxStore.Claims, claim => claim.ClaimedMessageIds.Count == 0);
    }

    [Fact]
    public async Task PublishPendingAsync_max_retry_exhaustion_marks_message_failed()
    {
        var message = CreateOutboxMessage(retryCount: 1);
        var outboxStore = new RecordingWorkflowOutboxStore([message]);
        var transport = new RecordingWorkflowMessagePublisher(
            _ => throw new InvalidOperationException("transport unavailable"));
        var publisher = new WorkflowOutboxPublisher(outboxStore, transport);

        var publishedCount = await publisher.PublishPendingAsync(
            batchSize: 10,
            dueAtUtc: DateTimeOffset.UtcNow,
            maxRetryAttempts: 2,
            retryDelay: TimeSpan.FromMinutes(1));

        Assert.Equal(0, publishedCount);
        Assert.Empty(outboxStore.PublishedMessageIds);

        var storedMessage = Assert.Single(outboxStore.Messages);
        Assert.Equal(WorkflowOutboxStatus.Failed, storedMessage.Status);
        Assert.Equal(2, storedMessage.RetryCount);
        Assert.Equal("transport unavailable", storedMessage.ErrorMessage);
        Assert.NotNull(storedMessage.LastAttemptAtUtc);
        Assert.Null(storedMessage.NextAttemptAtUtc);
        Assert.Null(storedMessage.LeaseOwner);
        Assert.Null(storedMessage.LeaseUntilUtc);
    }

    private static WorkflowOutboxMessageRecord CreateOutboxMessage(
        int retryCount = 0) =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            WorkflowEvents.WorkflowSucceeded,
            OutboxWorkflowEventPublisher.Destination,
            """{"eventName":"workflow.succeeded"}""",
            """{"workflow-id":"order-fulfillment"}""",
            WorkflowOutboxStatus.Pending,
            retryCount,
            ErrorMessage: null,
            DateTimeOffset.UtcNow.AddSeconds(-5),
            LastAttemptAtUtc: null,
            NextAttemptAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1),
            PublishedAtUtc: null);

    private sealed class RecordingWorkflowOutboxStore(
        IReadOnlyCollection<WorkflowOutboxMessageRecord>? pendingMessages = null)
        : IWorkflowOutboxStore
    {
        private readonly Lock _gate = new();
        private readonly List<WorkflowOutboxMessageRecord> _messages =
            pendingMessages?.ToList() ?? [];

        public List<WorkflowOutboxMessageDraft> SavedMessages { get; } = [];

        public List<ClaimedOutboxMessages> Claims { get; } = [];

        public List<Guid> PublishedMessageIds { get; } = [];

        public List<FailedOutboxMessage> FailedMessages { get; } = [];

        public IReadOnlyCollection<WorkflowOutboxMessageRecord> Messages
        {
            get
            {
                lock (_gate)
                {
                    return _messages.ToArray();
                }
            }
        }

        public Task<WorkflowOutboxMessageRecord> SaveMessageAsync(
            WorkflowOutboxMessageDraft message,
            CancellationToken cancellationToken = default)
        {
            SavedMessages.Add(message);
            var record = new WorkflowOutboxMessageRecord(
                Guid.NewGuid(),
                message.WorkflowInstanceId,
                message.MessageType,
                message.Destination,
                message.PayloadJson,
                message.HeadersJson,
                WorkflowOutboxStatus.Pending,
                RetryCount: 0,
                ErrorMessage: null,
                message.CreatedAtUtc ?? DateTimeOffset.UtcNow,
                LastAttemptAtUtc: null,
                message.NextAttemptAtUtc,
                PublishedAtUtc: null);

            lock (_gate)
            {
                _messages.Add(record);
            }

            return Task.FromResult(record);
        }

        public Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> GetMessagesForWorkflowAsync(
            Guid workflowInstanceId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<WorkflowOutboxMessageRecord>>([]);

        public Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> ClaimPendingMessagesAsync(
            DateTimeOffset nowUtc,
            string leaseOwner,
            TimeSpan leaseDuration,
            int batchSize,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<WorkflowOutboxMessageRecord>>(
                Claim(nowUtc, leaseOwner, leaseDuration, batchSize));

        private IReadOnlyCollection<WorkflowOutboxMessageRecord> Claim(
            DateTimeOffset nowUtc,
            string leaseOwner,
            TimeSpan leaseDuration,
            int batchSize)
        {
            lock (_gate)
            {
                var claimedMessages = _messages
                    .Where(message =>
                        message.Status == WorkflowOutboxStatus.Pending &&
                        (message.NextAttemptAtUtc is null ||
                            message.NextAttemptAtUtc <= nowUtc) &&
                        (message.LeaseUntilUtc is null ||
                            message.LeaseUntilUtc <= nowUtc))
                    .OrderBy(message => message.CreatedAtUtc)
                    .Take(batchSize)
                    .ToArray();

                foreach (var message in claimedMessages)
                {
                    UpdateMessage(message.Id, existing => existing with
                    {
                        LeaseOwner = leaseOwner,
                        LeaseUntilUtc = nowUtc.Add(leaseDuration)
                    });
                }

                Claims.Add(new ClaimedOutboxMessages(
                    nowUtc,
                    leaseOwner,
                    leaseDuration,
                    batchSize,
                    claimedMessages.Select(message => message.Id).ToArray()));

                return claimedMessages
                    .Select(message => message with
                    {
                        LeaseOwner = leaseOwner,
                        LeaseUntilUtc = nowUtc.Add(leaseDuration)
                    })
                    .ToArray();
            }
        }

        public Task MarkMessagePublishedAsync(
            Guid messageId,
            DateTimeOffset publishedAtUtc,
            CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                PublishedMessageIds.Add(messageId);
                UpdateMessage(messageId, message => message with
                {
                    Status = WorkflowOutboxStatus.Published,
                    ErrorMessage = null,
                    LastAttemptAtUtc = publishedAtUtc,
                    NextAttemptAtUtc = null,
                    PublishedAtUtc = publishedAtUtc,
                    LeaseOwner = null,
                    LeaseUntilUtc = null
                });
            }

            return Task.CompletedTask;
        }

        public Task MarkMessageFailedAsync(
            Guid messageId,
            string errorMessage,
            DateTimeOffset lastAttemptAtUtc,
            DateTimeOffset? nextAttemptAtUtc,
            CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                FailedMessages.Add(new FailedOutboxMessage(
                    messageId,
                    errorMessage,
                    lastAttemptAtUtc,
                    nextAttemptAtUtc));

                UpdateMessage(messageId, message => message with
                {
                    Status = nextAttemptAtUtc.HasValue
                        ? WorkflowOutboxStatus.Pending
                        : WorkflowOutboxStatus.Failed,
                    RetryCount = message.RetryCount + 1,
                    ErrorMessage = errorMessage,
                    LastAttemptAtUtc = lastAttemptAtUtc,
                    NextAttemptAtUtc = nextAttemptAtUtc,
                    LeaseOwner = null,
                    LeaseUntilUtc = null
                });
            }

            return Task.CompletedTask;
        }

        private void UpdateMessage(
            Guid messageId,
            Func<WorkflowOutboxMessageRecord, WorkflowOutboxMessageRecord> update)
        {
            var index = _messages.FindIndex(message => message.Id == messageId);
            if (index >= 0)
            {
                _messages[index] = update(_messages[index]);
            }
        }
    }

    private sealed class RecordingWorkflowMessagePublisher(
        Func<WorkflowMessageEnvelope, Task>? publish = null)
        : IWorkflowMessagePublisher
    {
        private readonly Lock _gate = new();
        private readonly Func<WorkflowMessageEnvelope, Task> _publish =
            publish ?? (_ => Task.CompletedTask);

        public List<WorkflowMessageEnvelope> PublishedMessages { get; } = [];

        public async Task PublishAsync(
            WorkflowMessageEnvelope message,
            CancellationToken cancellationToken = default)
        {
            await _publish(message);
            lock (_gate)
            {
                PublishedMessages.Add(message);
            }
        }
    }

    private sealed record FailedOutboxMessage(
        Guid MessageId,
        string ErrorMessage,
        DateTimeOffset LastAttemptAtUtc,
        DateTimeOffset? NextAttemptAtUtc);

    private sealed record ClaimedOutboxMessages(
        DateTimeOffset NowUtc,
        string LeaseOwner,
        TimeSpan LeaseDuration,
        int BatchSize,
        IReadOnlyCollection<Guid> ClaimedMessageIds);
}
