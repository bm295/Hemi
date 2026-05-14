namespace Hemi.Domain.Workflows;

public static class WorkflowEvents
{
    public const string WorkflowStarted = "workflow.started";

    public const string StepStarted = "workflow.step.started";

    public const string StepCompleted = "workflow.step.completed";

    public const string StepFailed = "workflow.step.failed";

    public const string StepCompensated = "workflow.step.compensated";

    public const string CompensationStarted =
        "workflow.compensation.started";

    public const string CompensationCompleted =
        "workflow.compensation.completed";

    public const string CompensationFailed =
        "workflow.compensation.failed";

    public const string WorkflowSucceeded =
        "workflow.succeeded";

    public const string WorkflowFailed =
        "workflow.failed";

    public const string WorkflowCancelled =
        "workflow.cancelled";
}
