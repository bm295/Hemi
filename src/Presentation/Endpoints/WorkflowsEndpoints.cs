using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Presentation.BackgroundWorkers;

namespace Hemi.Presentation.Endpoints;

public static class WorkflowsEndpoints
{
    public static IEndpointRouteBuilder MapWorkflowsEndpoints(
        this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/workflows");

        group.MapGet("/", (IWorkflowRegistry workflowRegistry) =>
        {
            var workflows = workflowRegistry.GetAll()
                .OrderBy(workflow => workflow.Name, StringComparer.Ordinal)
                .Select(ToResponse)
                .ToArray();

            return Results.Ok(workflows);
        });

        group.MapGet("/{workflowId}", (
            string workflowId,
            IWorkflowRegistry workflowRegistry) =>
        {
            if (!workflowRegistry.TryGet(
                    workflowId,
                    out var workflowDefinition) ||
                workflowDefinition is null)
            {
                return Results.NotFound(new WorkflowErrorResponse(
                    $"Workflow '{workflowId}' is not registered.",
                    Code: "workflow.not_found",
                    WorkflowId: workflowId));
            }

            return Results.Ok(ToResponse(workflowDefinition));
        });

        group.MapGet("/{workflowId}/instances/{correlationId}", async (
            string workflowId,
            string correlationId,
            IWorkflowInstanceStore workflowInstanceStore,
            IWorkflowExecutionLogStore workflowExecutionLogStore,
            CancellationToken cancellationToken) =>
        {
            var instance = await workflowInstanceStore.GetInstanceByCorrelationAsync(
                workflowId,
                correlationId,
                cancellationToken);

            if (instance is null)
            {
                return Results.NotFound(new WorkflowErrorResponse(
                    $"Workflow instance '{workflowId}/{correlationId}' was not found.",
                    Code: "workflow.instance_not_found",
                    WorkflowId: workflowId,
                    CorrelationId: correlationId));
            }

            var response = await WorkflowStatusMapper.ToStatusResponseAsync(
                instance,
                workflowExecutionLogStore,
                cancellationToken);

            return Results.Ok(response);
        });

        group.MapPost("/", async (
            StartWorkflowCommand request,
            WorkflowCommandQueue commandQueue,
            IWorkflowRegistry workflowRegistry,
            CancellationToken cancellationToken) =>
        {
            if (!workflowRegistry.TryGet(
                    request.WorkflowId,
                    out _))
            {
                return Results.NotFound(new WorkflowErrorResponse(
                    $"Workflow '{request.WorkflowId}' is not registered.",
                    Code: "workflow.not_found",
                    WorkflowId: request.WorkflowId,
                    CorrelationId: request.CorrelationId));
            }

            try
            {
                var response = await commandQueue.EnqueueAsync(
                    request,
                    cancellationToken);

                return Results.Accepted(
                    $"/workflows/{response.WorkflowId}/instances/{response.CorrelationId}",
                    response);
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(new WorkflowErrorResponse(
                    ex.Message,
                    Code: "workflow.request_invalid",
                    WorkflowId: request.WorkflowId,
                    CorrelationId: request.CorrelationId));
            }
            catch (WorkflowStartConflictException ex)
            {
                return Results.Conflict(new WorkflowErrorResponse(
                    ex.Message,
                    Code: ex.Code,
                    WorkflowId: request.WorkflowId,
                    CorrelationId: request.CorrelationId));
            }
            catch (InvalidOperationException ex)
            {
                return Results.Conflict(new WorkflowErrorResponse(
                    ex.Message,
                    Code: "workflow.idempotency_conflict",
                    WorkflowId: request.WorkflowId,
                    CorrelationId: request.CorrelationId));
            }
        });

        return app;
    }

    private static WorkflowDefinitionResponse ToResponse(
        IWorkflowDefinition workflowDefinition) =>
        new(
            workflowDefinition.Name,
            workflowDefinition.Steps
                .Select((step, index) => new WorkflowStepResponse(
                    index + 1,
                    step.Name))
                .ToArray());
}
