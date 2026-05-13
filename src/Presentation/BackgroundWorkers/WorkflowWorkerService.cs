using Hemi.Infrastructure.Messaging;

namespace Hemi.Presentation.BackgroundWorkers;

public sealed class WorkflowWorkerService(
    WorkflowCommandQueue commandQueue,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<WorkflowWorkerService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        await foreach (var command in commandQueue.ReadAllAsync(stoppingToken))
        {
            try
            {
                using var scope = serviceScopeFactory.CreateScope();
                var subscriber =
                    scope.ServiceProvider.GetRequiredService<WorkflowCommandSubscriber>();

                await subscriber.HandleAsync(
                    command,
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
                    "Workflow command {CommandId} for workflow {WorkflowId} failed.",
                    command.CommandId,
                    command.WorkflowId);
            }
        }
    }
}
