namespace Hemi.Infrastructure.Messaging;

public sealed record WorkflowMessageEnvelope(
    Guid MessageId,
    string MessageType,
    string Destination,
    string PayloadJson,
    DateTimeOffset CreatedAtUtc,
    IReadOnlyDictionary<string, string> Headers);

/// <summary>
/// Final delivery boundary for messages claimed from the durable workflow outbox.
/// </summary>
public interface IWorkflowMessagePublisher
{
    Task PublishAsync(
        WorkflowMessageEnvelope message,
        CancellationToken cancellationToken = default);
}
