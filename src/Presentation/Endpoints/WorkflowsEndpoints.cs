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
                    $"/workflows/{response.WorkflowId}",
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
