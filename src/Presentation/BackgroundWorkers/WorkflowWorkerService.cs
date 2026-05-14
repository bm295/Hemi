using System.Diagnostics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.Monitoring;

namespace Hemi.Presentation.BackgroundWorkers;

public sealed class WorkflowWorkerService(
    IServiceScopeFactory serviceScopeFactory,
    IWorkflowInstanceStore workflowInstanceStore,
    ILogger<WorkflowWorkerService> logger,
    WorkflowMetrics? workflowMetrics = null,
    WorkflowTracing? workflowTracing = null)
    : BackgroundService
{
    private const int BatchSize = 10;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(5);
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    private const string CommandIdContextKey = "workflow.commandId";
    private const string AttemptContextKey = "workflow.commandAttempt";
    private const string EnqueuedAtContextKey = "workflow.commandEnqueuedAtUtc";
    private const string IdempotencyKeyContextKey = "workflow.idempotencyKey";
    private const string SourceContextKey = "workflow.commandSource";

    private readonly string _leaseOwner =
        $"{Environment.MachineName}:{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            IReadOnlyCollection<WorkflowInstanceRecord> claimedInstances;

            try
            {
                claimedInstances = await workflowInstanceStore.ClaimDueInstancesAsync(
                    DateTimeOffset.UtcNow,
                    _leaseOwner,
                    LeaseDuration,
                    BatchSize,
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
                    "Workflow polling failed.");

                await DelayAsync(stoppingToken);
                continue;
            }

            if (claimedInstances.Count == 0)
            {
                await DelayAsync(stoppingToken);
                continue;
            }

            foreach (var instance in claimedInstances)
            {
                try
                {
                    await ProcessInstanceAsync(
                        instance,
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
                        "Workflow instance {WorkflowInstanceId} for workflow {WorkflowId} failed during polling.",
                        instance.Id,
                        instance.WorkflowId);
                }
            }
        }
    }

    private async Task ProcessInstanceAsync(
        WorkflowInstanceRecord instance,
        CancellationToken cancellationToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var workflowDispatcher =
            scope.ServiceProvider.GetRequiredService<IWorkflowDispatcher>();

        var context = CreateContext(instance);
        var command = CreateWorkerCommand(instance, context.Items);
        using var activity =
            workflowTracing?.StartWorkflowCommandHandle(command);
        var stopwatch = Stopwatch.StartNew();
        workflowMetrics?.RecordCommandReceived(command);

        try
        {
            await workflowDispatcher.DispatchAsync(
                instance.WorkflowId,
                context,
                cancellationToken);

            workflowMetrics?.RecordCommandCompleted(
                command,
                stopwatch.Elapsed);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (IsTerminal(context.State))
            {
                workflowTracing?.RecordException(activity, ex);
                workflowMetrics?.RecordCommandFailed(
                    command,
                    stopwatch.Elapsed,
                    ex);
            }

            await HandleExecutionFailureAsync(
                instance,
                context,
                ex,
                cancellationToken);
        }
    }

    private async Task HandleExecutionFailureAsync(
        WorkflowInstanceRecord instance,
        WorkflowContext context,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (IsTerminal(context.State))
        {
            return;
        }

        var version = await PersistContextPayloadAsync(
            instance,
            context,
            cancellationToken);

        if (!await workflowInstanceStore.TryUpdateStateAsync(
                instance.Id,
                WorkflowState.Failed,
                version,
                lastError: exception.Message,
                completedAtUtc: DateTimeOffset.UtcNow,
                nextAttemptAtUtc: null,
                cancellationToken))
        {
            logger.LogWarning(
                "Workflow instance {WorkflowInstanceId} terminal failure update lost optimistic concurrency.",
                instance.Id);
        }
    }

    private async Task<int> PersistContextPayloadAsync(
        WorkflowInstanceRecord instance,
        WorkflowContext context,
        CancellationToken cancellationToken)
    {
        var payloadJson = JsonSerializer.Serialize(
            context.Items,
            SerializerOptions);

        if (await workflowInstanceStore.TryUpdatePayloadAsync(
                instance.Id,
                context.WorkflowInstanceVersion > 0
                    ? context.WorkflowInstanceVersion
                    : instance.Version,
                payloadJson,
                cancellationToken))
        {
            context.WorkflowInstanceVersion =
                context.WorkflowInstanceVersion > 0
                    ? context.WorkflowInstanceVersion + 1
                    : instance.Version + 1;

            return context.WorkflowInstanceVersion;
        }

        logger.LogWarning(
            "Workflow instance {WorkflowInstanceId} context payload update lost optimistic concurrency.",
            instance.Id);

        return context.WorkflowInstanceVersion > 0
            ? context.WorkflowInstanceVersion
            : instance.Version;
    }

    private static WorkflowContext CreateContext(
        WorkflowInstanceRecord instance)
    {
        var context = new WorkflowContext(
            instance.WorkflowId,
            instance.CorrelationId)
        {
            WorkflowInstanceId = instance.Id,
            WorkflowInstanceVersion = instance.Version,
            WorkflowAttempt = instance.Attempt,
            WorkflowLeaseOwner = instance.LeaseOwner,
            CommandId = instance.CommandId,
            State = instance.State
        };

        if (!string.IsNullOrWhiteSpace(instance.PayloadJson))
        {
            using var document = JsonDocument.Parse(instance.PayloadJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException(
                    "Workflow instance payload must be a JSON object.");
            }

            foreach (var property in document.RootElement.EnumerateObject())
            {
                context.Set(
                    property.Name,
                    property.Value.Clone());
            }
        }

        context.Set(CommandIdContextKey, instance.CommandId);
        context.Set(AttemptContextKey, instance.Attempt);
        context.Set(EnqueuedAtContextKey, instance.CreatedAtUtc);

        if (!string.IsNullOrWhiteSpace(instance.IdempotencyKey))
        {
            context.Set(
                IdempotencyKeyContextKey,
                instance.IdempotencyKey);
        }

        if (!string.IsNullOrWhiteSpace(instance.RequestedBy))
        {
            context.Set(
                SourceContextKey,
                instance.RequestedBy);
        }

        return context;
    }

    private static WorkflowWorkerCommand CreateWorkerCommand(
        WorkflowInstanceRecord instance,
        IReadOnlyDictionary<string, object?> items) =>
        new(
            instance.CommandId,
            instance.WorkflowId,
            instance.CorrelationId,
            items,
            instance.Attempt,
            instance.CreatedAtUtc,
            instance.IdempotencyKey,
            instance.RequestedBy);

    private static bool IsTerminal(WorkflowState state) =>
        state is WorkflowState.Succeeded
            or WorkflowState.Failed
            or WorkflowState.Compensated
            or WorkflowState.CompensationFailed
            or WorkflowState.Cancelled;

    private static async Task DelayAsync(
        CancellationToken cancellationToken)
    {
        if (PollInterval > TimeSpan.Zero)
        {
            await Task.Delay(PollInterval, cancellationToken);
        }
    }
}
