using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;

namespace Hemi.Presentation.BackgroundWorkers;

public sealed class WorkflowCommandQueue(
    IWorkflowInstanceStore workflowInstanceStore,
    IWorkflowRegistry workflowRegistry)
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    private readonly Channel<WorkflowWorkerCommand> _commands =
        Channel.CreateUnbounded<WorkflowWorkerCommand>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

    public async Task<WorkflowAcceptedResponse> EnqueueAsync(
        StartWorkflowCommand request,
        CancellationToken cancellationToken = default)
    {
        Validate(request);
        var idempotencyKey = request.IdempotencyKey
            ?? throw new InvalidOperationException(
                "Idempotency key is required for workflow start requests.");

        if (!workflowRegistry.TryGet(
            request.WorkflowId,
            out var workflowDefinition) ||
            workflowDefinition is null)
        {
            throw new InvalidOperationException(
                $"Workflow '{request.WorkflowId}' is not registered.");
        }

        var requestHash = Hash(request);
        var requestedAtUtc = request.RequestedAtUtc ?? DateTimeOffset.UtcNow;
        var startResult = await workflowInstanceStore.StartWorkflowAsync(
            new WorkflowStartRequest(
                request.WorkflowId,
                workflowDefinition.Name,
                request.CorrelationId,
                SerializeItems(request.Items),
                idempotencyKey,
                requestHash,
                request.RequestedBy,
                requestedAtUtc,
                NextAttemptAtUtc: requestedAtUtc),
            cancellationToken);

        if (startResult.Status == WorkflowStartStatus.IdempotencyConflict)
        {
            throw new WorkflowStartConflictException(
                "Idempotency key was already used with a different workflow request.",
                "workflow.idempotency_conflict");
        }

        if (startResult.Status == WorkflowStartStatus.CorrelationConflict)
        {
            throw new WorkflowStartConflictException(
                "Workflow correlation was already used with a different workflow request.",
                "workflow.correlation_conflict");
        }

        var instance = startResult.Instance
            ?? throw new InvalidOperationException(
                "Workflow start did not return an instance.");

        var response = ToAcceptedResponse(instance);

        if (startResult.Status == WorkflowStartStatus.Created)
        {
            var workerCommand = new WorkflowWorkerCommand(
                instance.CommandId,
                request.WorkflowId,
                request.CorrelationId,
                request.Items,
                Attempt: instance.Attempt + 1,
                instance.CreatedAtUtc,
                idempotencyKey,
                Source: request.RequestedBy ?? "api");

            await _commands.Writer.WriteAsync(
                workerCommand,
                cancellationToken);
        }

        return response;
    }

    public IAsyncEnumerable<WorkflowWorkerCommand> ReadAllAsync(
        CancellationToken cancellationToken = default) =>
        _commands.Reader.ReadAllAsync(cancellationToken);

    private static void Validate(StartWorkflowCommand request)
    {
        if (string.IsNullOrWhiteSpace(request.WorkflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.CorrelationId))
        {
            throw new ArgumentException(
                "Correlation id is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.IdempotencyKey))
        {
            throw new ArgumentException(
                "Idempotency key is required for workflow start requests.",
                nameof(request));
        }

        ArgumentNullException.ThrowIfNull(request.Items);
    }

    private static WorkflowAcceptedResponse ToAcceptedResponse(
        WorkflowInstanceRecord instance) =>
        new(
            instance.CommandId,
            instance.WorkflowId,
            instance.CorrelationId,
            instance.State,
            instance.CreatedAtUtc,
            instance.IdempotencyKey);

    private static string Hash(StartWorkflowCommand request)
    {
        var payload = JsonSerializer.Serialize(
            request with { RequestedAtUtc = null },
            SerializerOptions);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(bytes);
    }

    private static string SerializeItems(
        IReadOnlyDictionary<string, object?> items) =>
        JsonSerializer.Serialize(items, SerializerOptions);
}

public sealed class WorkflowStartConflictException(
    string message,
    string code)
    : InvalidOperationException(message)
{
    public string Code { get; } = code;
}
