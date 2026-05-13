using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Execution;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain.Workflows;
using Microsoft.Extensions.DependencyInjection;

namespace Hemi.Tests.Workflows.Abstractions;

public sealed class WorkflowEngineTests
{
    [Fact]
    public async Task ExecuteAsync_runs_registered_steps_in_order()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new StepRecorder());
        services.AddTransient<FirstStep>();
        services.AddTransient<SecondStep>();

        var publisher = new RecordingWorkflowEventPublisher();
        var engine = CreateEngine(services, publisher);
        var context = new WorkflowContext("test-workflow", "correlation-1");
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(FirstStep),
            typeof(SecondStep));

        await engine.ExecuteAsync("test-workflow", definition, context);

        var recorder = services.BuildServiceProvider()
            .GetRequiredService<StepRecorder>();

        Assert.Equal(WorkflowState.Succeeded, context.State);
        Assert.Equal(["first", "second"], recorder.Executed);
        Assert.Contains(
            publisher.Events,
            workflowEvent => workflowEvent.EventName == WorkflowEvents.WorkflowSucceeded);
    }

    [Fact]
    public async Task ExecuteAsync_compensates_completed_steps_when_a_later_step_fails()
    {
        var services = new ServiceCollection();
        var recorder = new StepRecorder();
        services.AddSingleton(recorder);
        services.AddTransient<CompensableStep>();
        services.AddTransient<FailingStep>();

        var engine = CreateEngine(services, new RecordingWorkflowEventPublisher());
        var context = new WorkflowContext("test-workflow", "correlation-2");
        var definition = WorkflowDefinition.Create(
            "Test Workflow",
            typeof(CompensableStep),
            typeof(FailingStep));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync("test-workflow", definition, context));

        Assert.Equal("Step failed.", exception.Message);
        Assert.Equal(WorkflowState.Compensated, context.State);
        Assert.Equal(["compensable", "compensated"], recorder.Executed);
    }

    private static WorkflowEngine CreateEngine(
        IServiceCollection services,
        IWorkflowEventPublisher eventPublisher)
    {
        var provider = services.BuildServiceProvider();
        var retryPolicyProvider = new RetryPolicyProvider(
        [
            new WorkflowPolicyRegistration(
                "test-workflow",
                WorkflowPolicies.NoRetry)
        ]);

        return new WorkflowEngine(
            provider,
            retryPolicyProvider,
            eventPublisher);
    }

    private sealed class StepRecorder
    {
        public List<string> Executed { get; } = [];
    }

    private sealed class FirstStep(StepRecorder recorder)
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("first");
            return Task.CompletedTask;
        }
    }

    private sealed class SecondStep(StepRecorder recorder)
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("second");
            return Task.CompletedTask;
        }
    }

    private sealed class CompensableStep(StepRecorder recorder)
        : ICompensableWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("compensable");
            return Task.CompletedTask;
        }

        public Task CompensateAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            recorder.Executed.Add("compensated");
            return Task.CompletedTask;
        }
    }

    private sealed class FailingStep
        : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Step failed.");
    }

    private sealed class RecordingWorkflowEventPublisher
        : IWorkflowEventPublisher
    {
        public List<WorkflowEvent> Events { get; } = [];

        public Task PublishAsync(
            WorkflowEvent workflowEvent,
            CancellationToken cancellationToken = default)
        {
            Events.Add(workflowEvent);
            return Task.CompletedTask;
        }
    }
}
