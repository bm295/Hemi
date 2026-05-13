using System.Diagnostics;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;

namespace Hemi.Infrastructure.Monitoring;

public sealed class WorkflowTracing : IDisposable
{
    public const string ActivitySourceName = "Hemi.Workflows";

    private readonly ActivitySource _activitySource;

    public WorkflowTracing()
    {
        _activitySource = new ActivitySource(ActivitySourceName);
    }

    public Activity? StartWorkflowEventPublish(WorkflowEvent workflowEvent)
    {
        var activity = _activitySource.StartActivity(
            "workflow.event.publish",
            ActivityKind.Producer);

        if (activity is null)
        {
            return null;
        }

        activity.SetTag("workflow.id", workflowEvent.WorkflowId);
        activity.SetTag("workflow.name", workflowEvent.WorkflowName);
        activity.SetTag("correlation.id", workflowEvent.CorrelationId);
        activity.SetTag("workflow.state", workflowEvent.State.ToString());
        activity.SetTag("workflow.event.name", workflowEvent.EventName);
        activity.SetTag("workflow.step.name", workflowEvent.StepName);
        activity.SetTag("messaging.destination", "workflow.events");

        return activity;
    }

    public Activity? StartWorkflowCommandHandle(WorkflowWorkerCommand command)
    {
        var activity = _activitySource.StartActivity(
            "workflow.command.handle",
            ActivityKind.Consumer);

        if (activity is null)
        {
            return null;
        }

        activity.SetTag("workflow.id", command.WorkflowId);
        activity.SetTag("correlation.id", command.CorrelationId);
        activity.SetTag("workflow.command.id", command.CommandId);
        activity.SetTag("workflow.command.attempt", command.Attempt);
        activity.SetTag("workflow.command.source", command.Source);
        activity.SetTag("workflow.idempotency_key", command.IdempotencyKey);

        return activity;
    }

    public void RecordException(
        Activity? activity,
        Exception exception)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddException(exception);
    }

    public void Dispose() => _activitySource.Dispose();
}
