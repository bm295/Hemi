using Hemi.Application;
using Hemi.Application.Sagas.Legacy;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Compensations;
using Hemi.Application.Workflows.Definitions.OrderFulfillment.Steps;
using Hemi.Application.Workflows.Execution;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure;
using Hemi.Infrastructure.Messaging;
using Hemi.Infrastructure.Monitoring;
using Hemi.Infrastructure.WorkflowPersistence.Repositories;
using Hemi.Presentation.BackgroundWorkers;
using Hemi.Presentation.Endpoints;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<InMemoryFnbStore>();
builder.Services.AddSingleton<InMemoryRestaurantAdapter>();
builder.Services.AddSingleton<InMemoryTableAdapter>();
builder.Services.AddSingleton<InMemoryMenuAdapter>();
builder.Services.AddSingleton<InMemoryOrderAdapter>();
builder.Services.AddSingleton<InMemoryReservationAdapter>();
builder.Services.AddSingleton<InMemoryPaymentAdapter>();
builder.Services.AddSingleton<InMemoryInventoryAdapter>();
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? "Server=localhost;Database=HemiFnb;Trusted_Connection=True;TrustServerCertificate=True;";
builder.Services.AddSingleton(new SqlServerSagaStateAdapter(connectionString));

builder.Services.AddSingleton<IRestaurantQueryPort>(sp => sp.GetRequiredService<InMemoryRestaurantAdapter>());
builder.Services.AddSingleton<ITableQueryPort>(sp => sp.GetRequiredService<InMemoryTableAdapter>());
builder.Services.AddSingleton<IMenuQueryPort>(sp => sp.GetRequiredService<InMemoryMenuAdapter>());
builder.Services.AddSingleton<IOrderQueryPort>(sp => sp.GetRequiredService<InMemoryOrderAdapter>());
builder.Services.AddSingleton<IOrderCommandPort>(sp => sp.GetRequiredService<InMemoryOrderAdapter>());
builder.Services.AddSingleton<IReservationQueryPort>(sp => sp.GetRequiredService<InMemoryReservationAdapter>());
builder.Services.AddSingleton<IReservationCommandPort>(sp => sp.GetRequiredService<InMemoryReservationAdapter>());
builder.Services.AddSingleton<IPaymentQueryPort>(sp => sp.GetRequiredService<InMemoryPaymentAdapter>());
builder.Services.AddSingleton<IPaymentCommandPort>(sp => sp.GetRequiredService<InMemoryPaymentAdapter>());
builder.Services.AddSingleton<IInventoryQueryPort>(sp => sp.GetRequiredService<InMemoryInventoryAdapter>());
builder.Services.AddSingleton<IInventoryCommandPort>(sp => sp.GetRequiredService<InMemoryInventoryAdapter>());
builder.Services.AddSingleton<ISagaStateQueryPort>(sp => sp.GetRequiredService<SqlServerSagaStateAdapter>());

builder.Services.AddSingleton<LegacyOrderFulfillmentSagaQueryService>();
builder.Services.AddSingleton<FnbManagementService>();

builder.Services.AddSingleton(_ => new WorkflowInstanceRepository(connectionString));
builder.Services.AddSingleton<IWorkflowInstanceStore>(sp =>
    sp.GetRequiredService<WorkflowInstanceRepository>());
builder.Services.AddSingleton(_ => new WorkflowExecutionLogRepository(connectionString));
builder.Services.AddSingleton<IWorkflowExecutionLogStore>(sp =>
    sp.GetRequiredService<WorkflowExecutionLogRepository>());
builder.Services.AddSingleton<IWorkflowOutboxStore>(sp =>
    sp.GetRequiredService<WorkflowExecutionLogRepository>());
builder.Services.AddSingleton<IWorkflowJournal>(sp =>
    new SqlServerWorkflowJournal(
        connectionString,
        sp.GetService<WorkflowMetrics>()));
builder.Services.AddSingleton<WorkflowMetrics>();
builder.Services.AddSingleton<WorkflowTracing>();

builder.Services.AddScoped<IWorkflowEngine, WorkflowEngine>();
builder.Services.AddScoped<IWorkflowDispatcher, WorkflowDispatcher>();
builder.Services.AddSingleton<OrderFulfillmentWorkflow>();
builder.Services.AddSingleton<IWorkflowDefinition>(sp =>
    sp.GetRequiredService<OrderFulfillmentWorkflow>());
builder.Services.AddScoped<ReopenKitchenOrderCompensation>();
builder.Services.AddScoped<RefundOrderPaymentCompensation>();
builder.Services.AddScoped<RestoreOrderInventoryCompensation>();
builder.Services.AddScoped<SendOrderToKitchenStep>();
builder.Services.AddScoped<CaptureOrderPaymentStep>();
builder.Services.AddScoped<DeductOrderInventoryStep>();
builder.Services.AddScoped<CloseOrderStep>();
builder.Services.AddSingleton<IWorkflowRegistry>(sp =>
    new WorkflowRegistry(sp.GetRequiredService<IEnumerable<IWorkflowDefinition>>()));
builder.Services.AddSingleton(new WorkflowPolicyRegistration(
    WorkflowIds.OrderFulfillment,
    WorkflowPolicies.Default));
builder.Services.AddSingleton(new WorkflowPolicyRegistration(
    WorkflowIds.OrderCancellation,
    WorkflowPolicies.NoRetry));
builder.Services.AddSingleton(new WorkflowPolicyRegistration(
    WorkflowIds.InventoryReconciliation,
    WorkflowPolicies.Default));
builder.Services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();
builder.Services.AddSingleton<IWorkflowMessagePublisher, InMemoryWorkflowMessagePublisher>();
builder.Services.AddSingleton<WorkflowOutboxPublisher>();
builder.Services.AddSingleton<IWorkflowEventPublisher, OutboxWorkflowEventPublisher>();
builder.Services.AddSingleton<WorkflowCommandQueue>();
builder.Services.AddScoped<WorkflowCommandSubscriber>();
builder.Services.AddHostedService<WorkflowWorkerService>();
builder.Services.AddHostedService<WorkflowOutboxPublisherService>();

var app = builder.Build();

/*app.MapGet("/", async (FnbManagementService service, CancellationToken cancellationToken) =>
{
    var profile = await service.GetProfileAsync(cancellationToken);
    return Results.Ok(new
    {
        profile.Name,
        profile.Location,
        CapacityRange = $"{profile.SeatCapacityMinimum}-{profile.SeatCapacityMaximum} seats",
        Endpoints =
        [
            "/tables",
            "/menu",
            "/orders/open",
            "/orders/{orderId}/items",
            "/orders/{orderId}/send-to-kitchen",
            "/orders/{orderId}/payments",
            "/orders/{orderId}/close",
            "/orders/{orderId}/fulfillment-saga",
            "/reservations/upcoming",
            "/inventory",
            "/reports/sales",
            "/integrations/food-app/orders"
        ]
    });
});*/

app.MapGet("/tables", async (FnbManagementService service, CancellationToken cancellationToken) =>
    Results.Ok(await service.GetTablesAsync(cancellationToken)));

app.MapGet("/menu", async (FnbManagementService service, CancellationToken cancellationToken) =>
    Results.Ok(await service.GetAvailableMenuItemsAsync(cancellationToken)));

app.MapGet("/orders/open", async (FnbManagementService service, CancellationToken cancellationToken) =>
    Results.Ok(await service.GetOpenOrdersAsync(cancellationToken)));

app.MapGet("/reservations/upcoming", async (FnbManagementService service, CancellationToken cancellationToken) =>
    Results.Ok(await service.GetUpcomingReservationsAsync(DateTimeOffset.UtcNow, cancellationToken)));

app.MapGet("/inventory", async (FnbManagementService service, CancellationToken cancellationToken) =>
    Results.Ok(await service.GetInventoryAsync(cancellationToken)));

app.MapGet("/reports/sales", async (DateTimeOffset? from, DateTimeOffset? to, FnbManagementService service, CancellationToken cancellationToken) =>
{
    var now = DateTimeOffset.UtcNow;
    var report = await service.GetSalesReportAsync(from ?? now.AddDays(-1), to ?? now, cancellationToken);
    return Results.Ok(report);
});

app.MapPost("/orders", async (CreateOrderRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var lines = request.Items.Select(line => new CreateOrderLineInput(line.MenuItemId, line.Quantity)).ToArray();
        var order = await service.CreateOrderAsync(request.TableId, lines, cancellationToken);
        return Results.Created($"/orders/{order.Id}", order);
    }
    catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/orders/{orderId:guid}/items", async (Guid orderId, AddOrderItemRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var order = await service.AddOrderItemAsync(orderId, request.MenuItemId, request.Quantity, cancellationToken);
        return Results.Ok(order);
    }
    catch (Exception ex) when (ex is ArgumentOutOfRangeException or InvalidOperationException)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapDelete("/orders/{orderId:guid}/items", async (Guid orderId, [FromBody] RemoveOrderItemRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var order = await service.RemoveOrderItemAsync(orderId, request.MenuItemId, request.Quantity, cancellationToken);
        return Results.Ok(order);
    }
    catch (Exception ex) when (ex is ArgumentOutOfRangeException or InvalidOperationException or ArgumentException)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/orders/{orderId:guid}/send-to-kitchen", async (Guid orderId, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var order = await service.SendOrderToKitchenAsync(orderId, cancellationToken);
        return Results.Ok(order);
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/orders/{orderId:guid}/payments", async (Guid orderId, ProcessPaymentRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var payment = await service.ProcessPaymentAsync(orderId, request.Amount, request.Method, cancellationToken);
        return Results.Ok(payment);
    }
    catch (Exception ex) when (ex is ArgumentOutOfRangeException or InvalidOperationException)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/orders/{orderId:guid}/close", async (Guid orderId, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var order = await service.CloseOrderAsync(orderId, cancellationToken);
        return Results.Ok(order);
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/orders/{orderId:guid}/fulfillment-saga", async (
    Guid orderId,
    FulfillOrderWorkflowRequest request,
    WorkflowCommandQueue commandQueue,
    IWorkflowRegistry workflowRegistry,
    HttpRequest httpRequest,
    CancellationToken cancellationToken) =>
{
    if (!workflowRegistry.TryGet(WorkflowIds.OrderFulfillment, out _))
    {
        return Results.NotFound(new WorkflowErrorResponse(
            $"Workflow '{WorkflowIds.OrderFulfillment}' is not registered.",
            Code: "workflow.not_found",
            WorkflowId: WorkflowIds.OrderFulfillment,
            CorrelationId: orderId.ToString("D")));
    }

    var items = new Dictionary<string, object?>
    {
        [OrderFulfillmentWorkflowContext.OrderId] = orderId.ToString("D"),
        [OrderFulfillmentWorkflowContext.PaymentMethod] = request.Method.ToString()
    };

    if (request.Amount.HasValue)
    {
        items[OrderFulfillmentWorkflowContext.PaymentAmount] = request.Amount.Value;
    }

    var workflowCommand = new StartWorkflowCommand(
        WorkflowIds.OrderFulfillment,
        orderId.ToString("D"),
        items,
        ResolveIdempotencyKey(orderId, request, httpRequest),
        request.RequestedBy ?? "orders-api",
        DateTimeOffset.UtcNow);

    try
    {
        var response = await commandQueue.EnqueueAsync(
            workflowCommand,
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
            WorkflowId: workflowCommand.WorkflowId,
            CorrelationId: workflowCommand.CorrelationId));
    }
    catch (WorkflowStartConflictException ex)
    {
        return Results.Conflict(new WorkflowErrorResponse(
            ex.Message,
            Code: ex.Code,
            WorkflowId: workflowCommand.WorkflowId,
            CorrelationId: workflowCommand.CorrelationId));
    }
    catch (InvalidOperationException ex)
    {
        return Results.Conflict(new WorkflowErrorResponse(
            ex.Message,
            Code: "workflow.idempotency_conflict",
            WorkflowId: workflowCommand.WorkflowId,
            CorrelationId: workflowCommand.CorrelationId));
    }
});

app.MapGet("/orders/{orderId:guid}/fulfillment-saga", async (
    Guid orderId,
    IWorkflowInstanceStore workflowInstanceStore,
    IWorkflowExecutionLogStore workflowExecutionLogStore,
    LegacyOrderFulfillmentSagaQueryService legacySagaQueryService,
    CancellationToken cancellationToken) =>
{
    var correlationId = orderId.ToString("D");
    var instance = await workflowInstanceStore.GetInstanceByCorrelationAsync(
        WorkflowIds.OrderFulfillment,
        correlationId,
        cancellationToken);

    if (instance is not null)
    {
        var response = await WorkflowStatusMapper.ToStatusResponseAsync(
            instance,
            workflowExecutionLogStore,
            cancellationToken);

        return Results.Ok(response);
    }

    var saga = await legacySagaQueryService.GetSagaStateAsync(orderId, cancellationToken);
    return saga is null
        ? Results.NotFound(new { error = "Saga state not found for this order." })
        : Results.Ok(saga);
});

app.MapPost("/reservations", async (CreateReservationRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var reservation = await service.CreateReservationAsync(
            request.GuestName,
            request.PartySize,
            request.ReservedFor,
            request.ContactPhone,
            request.Notes,
            cancellationToken);

        return Results.Created($"/reservations/{reservation.Id}", reservation);
    }
    catch (ArgumentOutOfRangeException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapPost("/integrations/food-app/orders", async (IntegrateFoodAppOrderApiRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var result = await service.IntegrateFoodAppOrderAsync(new FoodAppOrderRequest(
            request.SourceApp,
            request.ExternalOrderId,
            request.TableCode,
            request.Items.Select(x => new FoodAppOrderItemRequest(x.MenuItemId, x.Quantity)).ToArray()),
            cancellationToken);

        return Results.Created($"/orders/{result.InternalOrderId}", result);
    }
    catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException or InvalidOperationException)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapWorkflowsEndpoints();

app.Run();

static string ResolveIdempotencyKey(
    Guid orderId,
    FulfillOrderWorkflowRequest request,
    HttpRequest httpRequest)
{
    if (!string.IsNullOrWhiteSpace(request.IdempotencyKey))
    {
        return request.IdempotencyKey;
    }

    if (httpRequest.Headers.TryGetValue("Idempotency-Key", out var headerValue) &&
        !string.IsNullOrWhiteSpace(headerValue.ToString()))
    {
        return headerValue.ToString();
    }

    return $"{WorkflowIds.OrderFulfillment}:{orderId:D}";
}

public sealed record CreateOrderRequest(Guid TableId, IReadOnlyCollection<CreateOrderItemRequest> Items);

public sealed record CreateOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record AddOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record RemoveOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record ProcessPaymentRequest(decimal Amount, PaymentMethod Method);

public sealed record FulfillOrderWorkflowRequest(
    PaymentMethod Method,
    decimal? Amount,
    string? IdempotencyKey = null,
    string? RequestedBy = null);

public sealed record CreateReservationRequest(
    string GuestName,
    int PartySize,
    DateTimeOffset ReservedFor,
    string ContactPhone,
    string? Notes);

public sealed record IntegrateFoodAppOrderApiRequest(
    string SourceApp,
    string ExternalOrderId,
    string TableCode,
    IReadOnlyCollection<IntegrateFoodAppOrderItemApiRequest> Items);

public sealed record IntegrateFoodAppOrderItemApiRequest(Guid MenuItemId, int Quantity);

public partial class Program;
