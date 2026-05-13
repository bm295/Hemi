using System.Diagnostics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Infrastructure.Monitoring;

namespace Hemi.Infrastructure.Messaging;

public sealed class WorkflowEventPublisher(
    IWorkflowMessagePublisher messagePublisher,
    WorkflowMetrics? workflowMetrics = null,
    WorkflowTracing? workflowTracing = null)
    : IWorkflowEventPublisher
{
    public const string Destination = "workflow.events";

    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    public async Task PublishAsync(
        WorkflowEvent workflowEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workflowEvent);

        using var activity =
            workflowTracing?.StartWorkflowEventPublish(workflowEvent);
        var stopwatch = Stopwatch.StartNew();
        workflowMetrics?.RecordWorkflowEvent(workflowEvent);

        var response = new WorkflowEventResponse(
            workflowEvent.EventName,
            workflowEvent.WorkflowId,
            workflowEvent.WorkflowName,
            workflowEvent.CorrelationId,
            workflowEvent.State,
            workflowEvent.StepName,
            workflowEvent.Error?.Message,
            workflowEvent.OccurredAtUtc);

        var message = new WorkflowMessageEnvelope(
            Guid.NewGuid(),
            workflowEvent.EventName,
            Destination,
            JsonSerializer.Serialize(response, SerializerOptions),
            DateTimeOffset.UtcNow,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["workflow-id"] = workflowEvent.WorkflowId,
                ["workflow-name"] = workflowEvent.WorkflowName,
                ["correlation-id"] = workflowEvent.CorrelationId,
                ["workflow-state"] = workflowEvent.State.ToString()
            });

        try
        {
            await messagePublisher.PublishAsync(
                message,
                cancellationToken);

            workflowMetrics?.RecordMessagePublished(
                message.MessageType,
                message.Destination,
                stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            workflowTracing?.RecordException(activity, ex);
            workflowMetrics?.RecordMessageFailed(
                message.MessageType,
                message.Destination,
                stopwatch.Elapsed,
                ex);

            throw;
        }
    }
}
