namespace Hemi.Infrastructure.Messaging;

public sealed record WorkflowMessageEnvelope(
    Guid MessageId,
    string MessageType,
    string Destination,
    string PayloadJson,
    DateTimeOffset CreatedAtUtc,
    IReadOnlyDictionary<string, string> Headers);

public interface IWorkflowMessagePublisher
{
    Task PublishAsync(
        WorkflowMessageEnvelope message,
        CancellationToken cancellationToken = default);
}
