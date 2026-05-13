using System.Diagnostics;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Infrastructure.Monitoring;

namespace Hemi.Infrastructure.Messaging;

public sealed class OutboxWorkflowEventPublisher(
    IWorkflowOutboxStore outboxStore,
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

        if (!workflowEvent.WorkflowInstanceId.HasValue)
        {
            throw new InvalidOperationException(
                "Workflow event outbox publishing requires a workflow instance id.");
        }

        using var activity =
            workflowTracing?.StartWorkflowEventPublish(workflowEvent);
        var stopwatch = Stopwatch.StartNew();

        var response = new WorkflowEventResponse(
            workflowEvent.EventName,
            workflowEvent.WorkflowId,
            workflowEvent.WorkflowName,
            workflowEvent.CorrelationId,
            workflowEvent.State,
            workflowEvent.StepName,
            workflowEvent.Error?.Message,
            workflowEvent.OccurredAtUtc);

        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["workflow-instance-id"] = workflowEvent.WorkflowInstanceId.Value.ToString("D"),
            ["workflow-id"] = workflowEvent.WorkflowId,
            ["workflow-name"] = workflowEvent.WorkflowName,
            ["correlation-id"] = workflowEvent.CorrelationId,
            ["workflow-state"] = workflowEvent.State.ToString()
        };

        try
        {
            _ = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    workflowEvent.WorkflowInstanceId.Value,
                    workflowEvent.EventName,
                    Destination,
                    JsonSerializer.Serialize(response, SerializerOptions),
                    JsonSerializer.Serialize(headers, SerializerOptions),
                    workflowEvent.OccurredAtUtc,
                    NextAttemptAtUtc: workflowEvent.OccurredAtUtc),
                cancellationToken);

            workflowMetrics?.RecordWorkflowEvent(workflowEvent);
        }
        catch (Exception ex)
        {
            workflowTracing?.RecordException(activity, ex);
            workflowMetrics?.RecordMessageFailed(
                workflowEvent.EventName,
                Destination,
                stopwatch.Elapsed,
                ex);

            throw;
        }
    }
}
