using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Domain;
using Hemi.Domain.Workflows;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Hemi.Tests.Presentation;

public sealed class OrderFulfillmentEndpointTests(
    OrderFulfillmentEndpointTests.HemiApiFactory factory)
    : IClassFixture<OrderFulfillmentEndpointTests.HemiApiFactory>
{
    [Fact]
    public async Task Post_workflows_persists_pending_instance_before_returning_accepted()
    {
        var client = factory.CreateClient();
        var store = factory.WorkflowInstanceStore;
        var correlationId = Guid.NewGuid().ToString("D");
        var idempotencyKey = $"workflow-test-{Guid.NewGuid():N}";

        using var response = await client.PostAsJsonAsync(
            "/workflows/",
            new StartWorkflowCommand(
                WorkflowIds.OrderFulfillment,
                correlationId,
                new Dictionary<string, object?>
                {
                    ["marker"] = "generic-workflow"
                },
                idempotencyKey,
                "endpoint-tests"));

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        var instance = await store.GetInstanceByCorrelationAsync(
            WorkflowIds.OrderFulfillment,
            correlationId);

        Assert.NotNull(instance);
        Assert.Equal(WorkflowState.Pending, instance.State);
        Assert.Equal(idempotencyKey, instance.IdempotencyKey);
        Assert.Contains("generic-workflow", instance.PayloadJson, StringComparison.Ordinal);
        Assert.Equal(1, instance.Version);
    }

    [Fact]
    public async Task Post_fulfillment_saga_accepts_order_fulfillment_workflow_request()
    {
        var client = factory.CreateClient();
        var store = factory.WorkflowInstanceStore;
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

        var instance = await store.GetInstanceByCorrelationAsync(
            WorkflowIds.OrderFulfillment,
            orderId.ToString("D"));

        Assert.NotNull(instance);
        Assert.Equal(WorkflowState.Pending, instance.State);
        Assert.Equal(idempotencyKey, instance.IdempotencyKey);
        Assert.Contains(
            OrderFulfillmentWorkflowContext.OrderId,
            instance.PayloadJson,
            StringComparison.Ordinal);
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
    public async Task Post_fulfillment_saga_rejects_same_idempotency_key_with_different_request()
    {
        var client = factory.CreateClient();
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"order-fulfillment-test-{Guid.NewGuid():N}";

        using var firstResponse = await SendWithIdempotencyKeyAsync(
            client,
            orderId,
            new
            {
                method = PaymentMethod.Card,
                amount = 51.00m
            },
            idempotencyKey);

        using var secondResponse = await SendWithIdempotencyKeyAsync(
            client,
            orderId,
            new
            {
                method = PaymentMethod.Card,
                amount = 52.00m
            },
            idempotencyKey);

        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, secondResponse.StatusCode);

        using var payload = await ReadJsonAsync(secondResponse);

        Assert.Equal(
            "workflow.idempotency_conflict",
            payload.RootElement.GetProperty("code").GetString());
    }

    [Fact]
    public async Task Post_workflows_rejects_same_workflow_correlation_with_different_request()
    {
        var client = factory.CreateClient();
        var correlationId = Guid.NewGuid().ToString("D");

        using var firstResponse = await client.PostAsJsonAsync(
            "/workflows/",
            new StartWorkflowCommand(
                WorkflowIds.OrderFulfillment,
                correlationId,
                new Dictionary<string, object?>
                {
                    ["marker"] = "first"
                },
                $"workflow-test-{Guid.NewGuid():N}",
                "endpoint-tests"));

        using var secondResponse = await client.PostAsJsonAsync(
            "/workflows/",
            new StartWorkflowCommand(
                WorkflowIds.OrderFulfillment,
                correlationId,
                new Dictionary<string, object?>
                {
                    ["marker"] = "second"
                },
                $"workflow-test-{Guid.NewGuid():N}",
                "endpoint-tests"));

        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, secondResponse.StatusCode);

        using var payload = await ReadJsonAsync(secondResponse);

        Assert.Equal(
            "workflow.correlation_conflict",
            payload.RootElement.GetProperty("code").GetString());
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

    [Fact]
    public void Workflow_persistence_abstraction_resolves_for_endpoint_runtime()
    {
        using var scope = factory.Services.CreateScope();
        var services = scope.ServiceProvider;

        Assert.Same(
            factory.WorkflowInstanceStore,
            services.GetRequiredService<IWorkflowInstanceStore>());
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
        public RecordingWorkflowInstanceStore WorkflowInstanceStore =>
            Services.GetRequiredService<RecordingWorkflowInstanceStore>();

        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IHostedService>();
                services.RemoveAll<IWorkflowInstanceStore>();
                services.AddSingleton<RecordingWorkflowInstanceStore>();
                services.AddSingleton<IWorkflowInstanceStore>(sp =>
                    sp.GetRequiredService<RecordingWorkflowInstanceStore>());
            });
        }
    }

    public sealed class RecordingWorkflowInstanceStore : IWorkflowInstanceStore
    {
        private readonly object _sync = new();
        private readonly Dictionary<string, WorkflowInstanceRecord> _instancesByIdempotencyKey =
            new(StringComparer.Ordinal);
        private readonly Dictionary<string, WorkflowInstanceRecord> _instancesByCorrelation =
            new(StringComparer.Ordinal);

        public Task<WorkflowStartResult> StartWorkflowAsync(
            WorkflowStartRequest request,
            CancellationToken cancellationToken = default)
        {
            lock (_sync)
            {
                if (_instancesByIdempotencyKey.TryGetValue(
                        request.IdempotencyKey,
                        out var existingByIdempotency))
                {
                    return Task.FromResult(SameRequest(
                            existingByIdempotency,
                            request.RequestHash)
                        ? new WorkflowStartResult(
                            WorkflowStartStatus.Existing,
                            existingByIdempotency,
                            existingByIdempotency.RequestHash)
                        : new WorkflowStartResult(
                            WorkflowStartStatus.IdempotencyConflict,
                            existingByIdempotency,
                            existingByIdempotency.RequestHash));
                }

                var correlationKey = CreateCorrelationKey(
                    request.WorkflowId,
                    request.CorrelationId);

                if (_instancesByCorrelation.TryGetValue(
                        correlationKey,
                        out var existingByCorrelation))
                {
                    return Task.FromResult(SameRequest(
                            existingByCorrelation,
                            request.RequestHash)
                        ? new WorkflowStartResult(
                            WorkflowStartStatus.Existing,
                            existingByCorrelation,
                            existingByCorrelation.RequestHash)
                        : new WorkflowStartResult(
                            WorkflowStartStatus.CorrelationConflict,
                            existingByCorrelation,
                            existingByCorrelation.RequestHash));
                }

                var instance = new WorkflowInstanceRecord(
                    Guid.NewGuid(),
                    Guid.NewGuid(),
                    request.WorkflowId,
                    request.WorkflowName,
                    request.CorrelationId,
                    WorkflowState.Pending,
                    request.PayloadJson,
                    LastError: null,
                    Version: 1,
                    request.RequestedAtUtc,
                    request.RequestedAtUtc,
                    CompletedAtUtc: null,
                    request.IdempotencyKey,
                    request.RequestHash,
                    request.RequestedBy,
                    Attempt: 0,
                    request.NextAttemptAtUtc,
                    LeaseOwner: null,
                    LeaseUntilUtc: null);

                _instancesByIdempotencyKey.Add(
                    request.IdempotencyKey,
                    instance);
                _instancesByCorrelation.Add(correlationKey, instance);

                return Task.FromResult(new WorkflowStartResult(
                    WorkflowStartStatus.Created,
                    instance,
                    instance.RequestHash));
            }
        }

        public Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
            Guid id,
            CancellationToken cancellationToken = default)
        {
            lock (_sync)
            {
                return Task.FromResult(
                    _instancesByCorrelation.Values.FirstOrDefault(instance => instance.Id == id));
            }
        }

        public Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
            string workflowId,
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            lock (_sync)
            {
                _instancesByCorrelation.TryGetValue(
                    CreateCorrelationKey(workflowId, correlationId),
                    out var instance);

                return Task.FromResult(instance);
            }
        }

        public Task<IReadOnlyCollection<WorkflowInstanceRecord>> ClaimDueInstancesAsync(
            DateTimeOffset nowUtc,
            string leaseOwner,
            TimeSpan leaseDuration,
            int batchSize = 10,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyCollection<WorkflowInstanceRecord>>([]);

        public Task<bool> TryUpdateStateAsync(
            Guid id,
            WorkflowState state,
            int expectedVersion,
            string? lastError = null,
            DateTimeOffset? completedAtUtc = null,
            DateTimeOffset? nextAttemptAtUtc = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public Task<bool> TryUpdatePayloadAsync(
            Guid id,
            int expectedVersion,
            string payloadJson,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        public Task<bool> TryReleaseLeaseAsync(
            Guid id,
            string leaseOwner,
            int expectedVersion,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(false);

        private static string CreateCorrelationKey(
            string workflowId,
            string correlationId) =>
            $"{workflowId}:{correlationId}";

        private static bool SameRequest(
            WorkflowInstanceRecord instance,
            string requestHash) =>
            string.Equals(
                instance.RequestHash,
                requestHash,
                StringComparison.Ordinal);
    }
}
