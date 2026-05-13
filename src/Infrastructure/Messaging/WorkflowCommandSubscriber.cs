using System.Diagnostics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Application.Workflows.Execution;
using Hemi.Infrastructure.Monitoring;

namespace Hemi.Infrastructure.Messaging;

public sealed class WorkflowCommandSubscriber(
    IWorkflowDispatcher workflowDispatcher,
    WorkflowMetrics? workflowMetrics = null,
    WorkflowTracing? workflowTracing = null)
{
    private const string CommandIdContextKey = "workflow.commandId";
    private const string AttemptContextKey = "workflow.commandAttempt";
    private const string EnqueuedAtContextKey = "workflow.commandEnqueuedAtUtc";
    private const string IdempotencyKeyContextKey = "workflow.idempotencyKey";
    private const string SourceContextKey = "workflow.commandSource";

    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    public Task HandleAsync(
        string payloadJson,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            throw new ArgumentException(
                "Workflow command payload is required.",
                nameof(payloadJson));
        }

        var command = JsonSerializer.Deserialize<WorkflowWorkerCommand>(
            payloadJson,
            SerializerOptions)
            ?? throw new InvalidOperationException(
                "Failed to deserialize workflow worker command.");

        return HandleAsync(command, cancellationToken);
    }

    public async Task HandleAsync(
        WorkflowWorkerCommand command,
        CancellationToken cancellationToken = default)
    {
        Validate(command);

        using var activity =
            workflowTracing?.StartWorkflowCommandHandle(command);
        var stopwatch = Stopwatch.StartNew();
        workflowMetrics?.RecordCommandReceived(command);

        var context = new WorkflowContext(
            command.WorkflowId,
            command.CorrelationId);

        foreach (var item in command.Items)
        {
            context.Set(item.Key, item.Value);
        }

        context.Set(CommandIdContextKey, command.CommandId);
        context.Set(AttemptContextKey, command.Attempt);
        context.Set(EnqueuedAtContextKey, command.EnqueuedAtUtc);

        if (!string.IsNullOrWhiteSpace(command.IdempotencyKey))
        {
            context.Set(
                IdempotencyKeyContextKey,
                command.IdempotencyKey);
        }

        if (!string.IsNullOrWhiteSpace(command.Source))
        {
            context.Set(SourceContextKey, command.Source);
        }

        try
        {
            await workflowDispatcher.DispatchAsync(
                command.WorkflowId,
                context,
                cancellationToken);

            workflowMetrics?.RecordCommandCompleted(
                command,
                stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            workflowTracing?.RecordException(activity, ex);
            workflowMetrics?.RecordCommandFailed(
                command,
                stopwatch.Elapsed,
                ex);

            throw;
        }
    }

    private static void Validate(WorkflowWorkerCommand command)
    {
        if (command.CommandId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow command id is required.",
                nameof(command));
        }

        if (string.IsNullOrWhiteSpace(command.WorkflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(command));
        }

        if (string.IsNullOrWhiteSpace(command.CorrelationId))
        {
            throw new ArgumentException(
                "Correlation id is required.",
                nameof(command));
        }

        if (command.Attempt <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(command),
                "Workflow command attempt must be greater than zero.");
        }
    }
}
