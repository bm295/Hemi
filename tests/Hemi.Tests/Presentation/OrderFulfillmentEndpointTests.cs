using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Hemi.Domain;
using Hemi.Domain.Workflows;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Hemi.Tests.Presentation;

public sealed class OrderFulfillmentEndpointTests(
    OrderFulfillmentEndpointTests.HemiApiFactory factory)
    : IClassFixture<OrderFulfillmentEndpointTests.HemiApiFactory>
{
    [Fact]
    public async Task Post_fulfillment_saga_accepts_order_fulfillment_workflow_request()
    {
        var client = factory.CreateClient();
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"order-fulfillment-test-{Guid.NewGuid():N}";

        using var response = await client.PostAsJsonAsync(
            $"/orders/{orderId:D}/fulfillment-saga",
            new
            {
                method = PaymentMethod.Card,
                amount = 42.75m,
                idempotencyKey,
                requestedBy = "endpoint-tests"
            });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        using var payload = await ReadJsonAsync(response);
        var root = payload.RootElement;

        Assert.Equal(WorkflowIds.OrderFulfillment, root.GetProperty("workflowId").GetString());
        Assert.Equal(orderId.ToString("D"), root.GetProperty("correlationId").GetString());
        Assert.Equal(idempotencyKey, root.GetProperty("idempotencyKey").GetString());
        Assert.Equal((int)WorkflowState.Pending, root.GetProperty("state").GetInt32());
    }

    [Fact]
    public async Task Post_fulfillment_saga_reuses_idempotency_key_response()
    {
        var client = factory.CreateClient();
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"order-fulfillment-test-{Guid.NewGuid():N}";
        var request = new
        {
            method = PaymentMethod.EWallet,
            amount = 51.00m
        };

        using var firstResponse = await SendWithIdempotencyKeyAsync(
            client,
            orderId,
            request,
            idempotencyKey);

        using var secondResponse = await SendWithIdempotencyKeyAsync(
            client,
            orderId,
            request,
            idempotencyKey);

        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, secondResponse.StatusCode);

        using var firstPayload = await ReadJsonAsync(firstResponse);
        using var secondPayload = await ReadJsonAsync(secondResponse);

        Assert.Equal(
            firstPayload.RootElement.GetProperty("commandId").GetString(),
            secondPayload.RootElement.GetProperty("commandId").GetString());
    }

    [Fact]
    public async Task Get_workflows_lists_order_fulfillment_workflow()
    {
        var client = factory.CreateClient();

        using var response = await client.GetAsync("/workflows/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        using var payload = await ReadJsonAsync(response);

        Assert.Contains(
            payload.RootElement.EnumerateArray(),
            workflow => workflow.GetProperty("workflowId").GetString() == WorkflowIds.OrderFulfillment);
    }

    private static async Task<HttpResponseMessage> SendWithIdempotencyKeyAsync(
        HttpClient client,
        Guid orderId,
        object body,
        string idempotencyKey)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"/orders/{orderId:D}/fulfillment-saga")
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("Idempotency-Key", idempotencyKey);

        return await client.SendAsync(request);
    }

    private static async Task<JsonDocument> ReadJsonAsync(
        HttpResponseMessage response)
    {
        await using var stream = await response.Content.ReadAsStreamAsync();
        return await JsonDocument.ParseAsync(stream);
    }

    public sealed class HemiApiFactory : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IHostedService>();
            });
        }
    }
}
