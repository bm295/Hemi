using System.Collections.Concurrent;

namespace Hemi.Infrastructure.Messaging;

public sealed class InMemoryWorkflowMessagePublisher : IWorkflowMessagePublisher
{
    private readonly ConcurrentQueue<WorkflowMessageEnvelope> _messages = new();

    public IReadOnlyCollection<WorkflowMessageEnvelope> PublishedMessages =>
        _messages.ToArray();

    public Task PublishAsync(
        WorkflowMessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();

        _messages.Enqueue(message);
        return Task.CompletedTask;
    }
}
