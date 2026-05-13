using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;

namespace Hemi.Presentation.Endpoints;

public static class WorkflowStatusMapper
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    public static async Task<WorkflowStatusResponse> ToStatusResponseAsync(
        WorkflowInstanceRecord instance,
        IWorkflowExecutionLogStore workflowExecutionLogStore,
        CancellationToken cancellationToken = default)
    {
        var attempts = await workflowExecutionLogStore.GetStepAttemptsAsync(
            instance.Id,
            cancellationToken);

        return new WorkflowStatusResponse(
            instance.Id,
            instance.CommandId,
            instance.WorkflowId,
            instance.CorrelationId,
            instance.State,
            instance.CreatedAtUtc,
            instance.UpdatedAtUtc,
            instance.CompletedAtUtc,
            instance.LastError,
            ToStepSummaries(attempts),
            DeserializeItems(instance.PayloadJson),
            instance.IdempotencyKey);
    }

    private static IReadOnlyCollection<WorkflowStepSummaryResponse> ToStepSummaries(
        IEnumerable<WorkflowStepAttemptRecord> attempts) =>
        attempts
            .GroupBy(attempt => attempt.StepOrder)
            .Select(group => group
                .OrderByDescending(attempt => attempt.Attempt)
                .ThenByDescending(attempt => attempt.CompletedAtUtc)
                .ThenByDescending(attempt => attempt.StartedAtUtc)
                .First())
            .OrderBy(attempt => attempt.StepOrder)
            .Select(attempt => new WorkflowStepSummaryResponse(
                attempt.StepOrder,
                attempt.StepName,
                attempt.Status,
                attempt.Attempt,
                attempt.ErrorMessage,
                attempt.StartedAtUtc,
                attempt.CompletedAtUtc,
                attempt.CompensatedAtUtc))
            .ToArray();

    private static IReadOnlyDictionary<string, object?> DeserializeItems(
        string payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }

        return JsonSerializer.Deserialize<Dictionary<string, object?>>(
                payloadJson,
                SerializerOptions) ??
            new Dictionary<string, object?>(StringComparer.Ordinal);
    }
}
