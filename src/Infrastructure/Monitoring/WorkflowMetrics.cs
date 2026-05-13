using System.Diagnostics;
using System.Diagnostics.Metrics;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;

namespace Hemi.Infrastructure.Monitoring;

public sealed class WorkflowMetrics : IDisposable
{
    public const string MeterName = "Hemi.Workflows";

    private readonly Meter _meter;
    private readonly Counter<long> _workflowEvents;
    private readonly Counter<long> _workflowCommands;
    private readonly Counter<long> _workflowMessages;
    private readonly Histogram<double> _workflowCommandDuration;
    private readonly Histogram<double> _workflowMessageDuration;

    public WorkflowMetrics()
    {
        _meter = new Meter(MeterName);
        _workflowEvents = _meter.CreateCounter<long>(
            "hemi.workflow.events",
            unit: "{event}",
            description: "Workflow events emitted by the workflow engine.");
        _workflowCommands = _meter.CreateCounter<long>(
            "hemi.workflow.commands",
            unit: "{command}",
            description: "Workflow command subscriber outcomes.");
        _workflowMessages = _meter.CreateCounter<long>(
            "hemi.workflow.messages",
            unit: "{message}",
            description: "Workflow messaging publish outcomes.");
        _workflowCommandDuration = _meter.CreateHistogram<double>(
            "hemi.workflow.command.duration",
            unit: "ms",
            description: "Workflow command handling duration.");
        _workflowMessageDuration = _meter.CreateHistogram<double>(
            "hemi.workflow.message.duration",
            unit: "ms",
            description: "Workflow message publishing duration.");
    }

    public void RecordWorkflowEvent(WorkflowEvent workflowEvent)
    {
        var tags = CreateWorkflowTags(
            workflowEvent.WorkflowId,
            workflowEvent.WorkflowName,
            workflowEvent.CorrelationId);
        tags.Add("event.name", workflowEvent.EventName);
        tags.Add("workflow.state", workflowEvent.State.ToString());
        tags.Add("step.name", workflowEvent.StepName);
        tags.Add("error", workflowEvent.Error is not null);

        _workflowEvents.Add(1, tags);
    }

    public void RecordCommandReceived(WorkflowWorkerCommand command)
    {
        var tags = CreateCommandTags(command);
        tags.Add("outcome", "received");

        _workflowCommands.Add(1, tags);
    }

    public void RecordCommandCompleted(
        WorkflowWorkerCommand command,
        TimeSpan duration)
    {
        var tags = CreateCommandTags(command);
        tags.Add("outcome", "completed");

        _workflowCommands.Add(1, tags);
        _workflowCommandDuration.Record(duration.TotalMilliseconds, tags);
    }

    public void RecordCommandFailed(
        WorkflowWorkerCommand command,
        TimeSpan duration,
        Exception exception)
    {
        var tags = CreateCommandTags(command);
        tags.Add("outcome", "failed");
        tags.Add("exception.type", exception.GetType().Name);

        _workflowCommands.Add(1, tags);
        _workflowCommandDuration.Record(duration.TotalMilliseconds, tags);
    }

    public void RecordMessagePublished(
        string messageType,
        string destination,
        TimeSpan duration)
    {
        var tags = CreateMessageTags(messageType, destination);
        tags.Add("outcome", "published");

        _workflowMessages.Add(1, tags);
        _workflowMessageDuration.Record(duration.TotalMilliseconds, tags);
    }

    public void RecordMessageFailed(
        string messageType,
        string destination,
        TimeSpan duration,
        Exception exception)
    {
        var tags = CreateMessageTags(messageType, destination);
        tags.Add("outcome", "failed");
        tags.Add("exception.type", exception.GetType().Name);

        _workflowMessages.Add(1, tags);
        _workflowMessageDuration.Record(duration.TotalMilliseconds, tags);
    }

    public void Dispose() => _meter.Dispose();

    private static TagList CreateWorkflowTags(
        string workflowId,
        string workflowName,
        string correlationId)
    {
        var tags = new TagList
        {
            { "workflow.id", workflowId },
            { "workflow.name", workflowName },
            { "correlation.id", correlationId }
        };

        return tags;
    }

    private static TagList CreateCommandTags(WorkflowWorkerCommand command)
    {
        var tags = new TagList
        {
            { "workflow.id", command.WorkflowId },
            { "correlation.id", command.CorrelationId },
            { "command.id", command.CommandId },
            { "command.attempt", command.Attempt },
            { "command.source", command.Source }
        };

        return tags;
    }

    private static TagList CreateMessageTags(
        string messageType,
        string destination)
    {
        var tags = new TagList
        {
            { "message.type", messageType },
            { "messaging.destination", destination }
        };

        return tags;
    }
}
