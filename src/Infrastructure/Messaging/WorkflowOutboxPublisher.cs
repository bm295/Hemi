using System.Diagnostics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Infrastructure.Monitoring;

namespace Hemi.Infrastructure.Messaging;

public sealed class WorkflowOutboxPublisher(
    IWorkflowOutboxStore outboxStore,
    IWorkflowMessagePublisher messagePublisher,
    WorkflowMetrics? workflowMetrics = null)
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    public async Task<int> PublishPendingAsync(
        int batchSize = 50,
        DateTimeOffset? dueAtUtc = null,
        int maxRetryAttempts = 5,
        TimeSpan? retryDelay = null,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                "Batch size must be greater than zero.");
        }

        var retryLimit = Math.Max(1, maxRetryAttempts);
        var delay = retryDelay ?? TimeSpan.FromSeconds(5);
        var nowUtc = dueAtUtc ?? DateTimeOffset.UtcNow;
        var pendingMessages = await outboxStore.GetPendingMessagesAsync(
            batchSize,
            nowUtc,
            cancellationToken);

        var publishedCount = 0;

        foreach (var message in pendingMessages)
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                await messagePublisher.PublishAsync(
                    ToEnvelope(message),
                    cancellationToken);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                workflowMetrics?.RecordMessageFailed(
                    message.MessageType,
                    message.Destination,
                    stopwatch.Elapsed,
                    ex);

                var failedAtUtc = DateTimeOffset.UtcNow;
                var nextRetryCount = message.RetryCount + 1;
                DateTimeOffset? nextAttemptAtUtc = nextRetryCount >= retryLimit
                    ? null
                    : failedAtUtc.Add(delay);

                await outboxStore.MarkMessageFailedAsync(
                    message.Id,
                    ex.Message,
                    failedAtUtc,
                    nextAttemptAtUtc,
                    CancellationToken.None);

                continue;
            }

            await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                DateTimeOffset.UtcNow,
                cancellationToken);

            workflowMetrics?.RecordMessagePublished(
                message.MessageType,
                message.Destination,
                stopwatch.Elapsed);

            publishedCount++;
        }

        return publishedCount;
    }

    private static WorkflowMessageEnvelope ToEnvelope(
        WorkflowOutboxMessageRecord message) =>
        new(
            message.Id,
            message.MessageType,
            message.Destination,
            message.PayloadJson,
            message.CreatedAtUtc,
            DeserializeHeaders(message.HeadersJson));

    private static IReadOnlyDictionary<string, string> DeserializeHeaders(
        string headersJson)
    {
        if (string.IsNullOrWhiteSpace(headersJson))
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        return JsonSerializer.Deserialize<Dictionary<string, string>>(
                headersJson,
                SerializerOptions) ??
            new Dictionary<string, string>(StringComparer.Ordinal);
    }
}
