using Hemi.Infrastructure.Messaging;

namespace Hemi.Presentation.BackgroundWorkers;

public sealed class WorkflowOutboxPublisherService(
    WorkflowOutboxPublisher workflowOutboxPublisher,
    ILogger<WorkflowOutboxPublisherService> logger)
    : BackgroundService
{
    private const int BatchSize = 50;
    private const int MaxRetryAttempts = 5;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _ = await workflowOutboxPublisher.PublishPendingAsync(
                    BatchSize,
                    DateTimeOffset.UtcNow,
                    MaxRetryAttempts,
                    RetryDelay,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Workflow outbox publishing failed.");
            }

            await Task.Delay(
                PollInterval,
                stoppingToken);
        }
    }
}
