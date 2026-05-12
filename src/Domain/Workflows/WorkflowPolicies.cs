namespace Hemi.Domain.Workflows;

public sealed record WorkflowPolicies
{
    public int MaxRetryAttempts { get; }

    public TimeSpan RetryDelay { get; }

    public bool EnableCompensation { get; }

    public bool StopOnFirstFailure { get; }

    public TimeSpan Timeout { get; }

    public WorkflowPolicies(
        int maxRetryAttempts,
        TimeSpan retryDelay,
        bool enableCompensation,
        bool stopOnFirstFailure,
        TimeSpan timeout)
    {
        if (maxRetryAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxRetryAttempts),
                "Max retry attempts cannot be negative.");
        }

        if (maxRetryAttempts > 0 &&
            retryDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retryDelay),
                "Retry delay must be greater than zero when retries are enabled.");
        }

        if (maxRetryAttempts == 0 &&
            retryDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retryDelay),
                "Retry delay cannot be negative.");
        }

        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                "Timeout must be greater than zero.");
        }

        MaxRetryAttempts = maxRetryAttempts;
        RetryDelay = retryDelay;
        EnableCompensation = enableCompensation;
        StopOnFirstFailure = stopOnFirstFailure;
        Timeout = timeout;
    }

    public static WorkflowPolicies Default =>
        new(
            maxRetryAttempts: 3,
            retryDelay: TimeSpan.FromSeconds(2),
            enableCompensation: true,
            stopOnFirstFailure: true,
            timeout: TimeSpan.FromMinutes(5));

    public static WorkflowPolicies NoRetry =>
        new(
            maxRetryAttempts: 0,
            retryDelay: TimeSpan.Zero,
            enableCompensation: true,
            stopOnFirstFailure: true,
            timeout: TimeSpan.FromMinutes(5));
}