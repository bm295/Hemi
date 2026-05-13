using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Hemi.Application.Workflows.Contracts;
using Hemi.Domain.Workflows;

namespace Hemi.Presentation.BackgroundWorkers;

public sealed class WorkflowCommandQueue
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

    private readonly ConcurrentDictionary<string, AcceptedWorkflowCommand> _acceptedByIdempotencyKey =
        new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, WorkflowAcceptedResponse> _acceptedByCorrelation =
        new(StringComparer.Ordinal);

    public async Task<WorkflowAcceptedResponse> EnqueueAsync(
        StartWorkflowCommand request,
        CancellationToken cancellationToken = default)
    {
        Validate(request);
        var idempotencyKey = request.IdempotencyKey
            ?? throw new InvalidOperationException(
                "Idempotency key is required for workflow start requests.");

        var correlationKey = CreateCorrelationKey(
            request.WorkflowId,
            request.CorrelationId);

        if (_acceptedByCorrelation.TryGetValue(
                correlationKey,
                out var existingCorrelationResponse))
        {
            return existingCorrelationResponse;
        }

        var requestHash = Hash(request);
        if (_acceptedByIdempotencyKey.TryGetValue(
                idempotencyKey,
                out var existing))
        {
            if (!string.Equals(
                    existing.RequestHash,
                    requestHash,
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Idempotency key was already used with a different workflow request.");
            }

            return existing.Response;
        }

        var now = DateTimeOffset.UtcNow;
        var response = new WorkflowAcceptedResponse(
            Guid.NewGuid(),
            request.WorkflowId,
            request.CorrelationId,
            WorkflowState.Pending,
            now,
            idempotencyKey);

        var accepted = new AcceptedWorkflowCommand(
            requestHash,
            response);

        if (!_acceptedByIdempotencyKey.TryAdd(
                idempotencyKey,
                accepted))
        {
            return await EnqueueAsync(request, cancellationToken);
        }

        if (!_acceptedByCorrelation.TryAdd(correlationKey, response))
        {
            _acceptedByIdempotencyKey.TryRemove(
                idempotencyKey,
                out _);

            return _acceptedByCorrelation[correlationKey];
        }

        var workerCommand = new WorkflowWorkerCommand(
            response.CommandId,
            request.WorkflowId,
            request.CorrelationId,
            request.Items,
            Attempt: 1,
            now,
            idempotencyKey,
            Source: request.RequestedBy ?? "api");

        await _commands.Writer.WriteAsync(
            workerCommand,
            cancellationToken);

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

    private static string CreateCorrelationKey(
        string workflowId,
        string correlationId) =>
        $"{workflowId}:{correlationId}";

    private static string Hash(StartWorkflowCommand request)
    {
        var payload = JsonSerializer.Serialize(
            request with { RequestedAtUtc = null },
            SerializerOptions);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(payload));

        return Convert.ToHexString(bytes);
    }

    private sealed record AcceptedWorkflowCommand(
        string RequestHash,
        WorkflowAcceptedResponse Response);
}
