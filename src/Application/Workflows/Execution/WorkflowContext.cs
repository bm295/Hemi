using Hemi.Domain.Workflows;
using System.Collections.Concurrent;

namespace Hemi.Application.Workflows.Execution;

public sealed class WorkflowContext
{
    private readonly ConcurrentDictionary<string, object?> _items = new();

    public WorkflowContext(
        string workflowId,
        string correlationId)
    {
        WorkflowId = workflowId;
        CorrelationId = correlationId;
        State = WorkflowState.Pending;
    }

    public string WorkflowId { get; }

    public string CorrelationId { get; }

    public Guid? WorkflowInstanceId { get; set; }

    public int WorkflowInstanceVersion { get; set; }

    public int WorkflowAttempt { get; set; }

    public Guid? CommandId { get; set; }

    public WorkflowState State { get; set; }

    public Exception? LastError { get; set; }

    public IReadOnlyDictionary<string, object?> Items => _items;

    public void Set<T>(string key, T value)
    {
        _items[key] = value;
    }

    public T GetRequired<T>(string key)
    {
        if (!_items.TryGetValue(key, out var value))
        {
            throw new InvalidOperationException(
                $"Workflow context item '{key}' was not found.");
        }

        if (value is not T typedValue)
        {
            throw new InvalidOperationException(
                $"Workflow context item '{key}' is not of type '{typeof(T).Name}'.");
        }

        return typedValue;
    }

    public bool TryGet<T>(string key, out T? value)
    {
        if (_items.TryGetValue(key, out var rawValue) &&
            rawValue is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }
}
