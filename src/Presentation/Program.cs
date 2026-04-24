using Hemi.Application;
using Hemi.Domain;
using Hemi.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<InMemoryFnbStore>();
builder.Services.AddSingleton<InMemoryRestaurantAdapter>();
builder.Services.AddSingleton<InMemoryTableAdapter>();
builder.Services.AddSingleton<InMemoryMenuAdapter>();
builder.Services.AddSingleton<InMemoryOrderAdapter>();
builder.Services.AddSingleton<InMemoryReservationAdapter>();
builder.Services.AddSingleton<InMemoryPaymentAdapter>();
builder.Services.AddSingleton<InMemoryInventoryAdapter>();
builder.Services.AddSingleton<InMemorySagaStateAdapter>();

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
builder.Services.AddSingleton<ISagaStateQueryPort>(sp => sp.GetRequiredService<InMemorySagaStateAdapter>());
builder.Services.AddSingleton<ISagaStateCommandPort>(sp => sp.GetRequiredService<InMemorySagaStateAdapter>());

builder.Services.AddSingleton<OrderFulfillmentSagaOrchestrator>();
builder.Services.AddSingleton<FnbManagementService>();

var app = builder.Build();

app.MapGet("/", async (FnbManagementService service, CancellationToken cancellationToken) =>
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
});

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

app.MapDelete("/orders/{orderId:guid}/items", async (Guid orderId, RemoveOrderItemRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
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

app.MapPost("/orders/{orderId:guid}/fulfillment-saga", async (Guid orderId, FulfillOrderSagaRequest request, FnbManagementService service, CancellationToken cancellationToken) =>
{
    try
    {
        var saga = await service.ExecuteOrderFulfillmentSagaAsync(orderId, request.Method, request.Amount, cancellationToken);
        return Results.Ok(saga);
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

app.MapGet("/orders/{orderId:guid}/fulfillment-saga", async (Guid orderId, FnbManagementService service, CancellationToken cancellationToken) =>
{
    var saga = await service.GetOrderFulfillmentSagaStateAsync(orderId, cancellationToken);
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

app.Run();

public sealed record CreateOrderRequest(Guid TableId, IReadOnlyCollection<CreateOrderItemRequest> Items);

public sealed record CreateOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record AddOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record RemoveOrderItemRequest(Guid MenuItemId, int Quantity);

public sealed record ProcessPaymentRequest(decimal Amount, PaymentMethod Method);
public sealed record FulfillOrderSagaRequest(PaymentMethod Method, decimal? Amount);

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
