using System.Data;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Hemi.Application;
using Hemi.Application.Sagas.Legacy;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Application.Workflows.Execution;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Hemi.Infrastructure.WorkflowPersistence.Repositories;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Hemi.Tests.Infrastructure.WorkflowPersistence;

public sealed class SqlServerWorkflowPersistenceTests
{
    private static readonly Guid CompensationWorkflowTableId =
        Guid.Parse("20000000-0000-0000-0000-000000000002");

    private static readonly Guid CompensationWorkflowMenuItemId =
        Guid.Parse("30000000-0000-0000-0000-000000000002");

    private static readonly Guid CompensationWorkflowInventoryItemId =
        Guid.Parse("40000000-0000-0000-0000-000000000002");

    private static readonly Guid RestartWorkflowTableId =
        Guid.Parse("20000000-0000-0000-0000-000000000005");

    private static readonly Guid RestartWorkflowMenuItemId =
        Guid.Parse("30000000-0000-0000-0000-000000000003");

    private static readonly Guid RestartWorkflowInventoryItemId =
        Guid.Parse("40000000-0000-0000-0000-000000000003");

    [SqlServerFact]
    public async Task Workflow_schema_applies_idempotently_and_creates_durable_columns_indexes()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();

        await database.ApplySchemaAsync();
        await database.ApplySchemaAsync();

        var instanceColumns = await GetColumnNamesAsync(
            database.ConnectionString,
            "WorkflowInstance");
        Assert.Contains("CommandId", instanceColumns);
        Assert.Contains("IdempotencyKey", instanceColumns);
        Assert.Contains("RequestHash", instanceColumns);
        Assert.Contains("RequestedBy", instanceColumns);
        Assert.Contains("Attempt", instanceColumns);
        Assert.Contains("NextAttemptAtUtc", instanceColumns);
        Assert.Contains("LeaseOwner", instanceColumns);
        Assert.Contains("LeaseUntilUtc", instanceColumns);

        var outboxColumns = await GetColumnNamesAsync(
            database.ConnectionString,
            "WorkflowOutboxMessage");
        Assert.Contains("HeadersJson", outboxColumns);
        Assert.Contains("NextAttemptAtUtc", outboxColumns);
        Assert.Contains("PublishedAtUtc", outboxColumns);
        Assert.Contains("LeaseOwner", outboxColumns);
        Assert.Contains("LeaseUntilUtc", outboxColumns);

        var indexes = await GetIndexNamesAsync(database.ConnectionString);
        Assert.Contains("UX_WorkflowInstance_Workflow_Correlation", indexes);
        Assert.Contains("UX_WorkflowInstance_IdempotencyKey", indexes);
        Assert.Contains("IX_WorkflowInstance_State_NextAttempt_Lease", indexes);
        Assert.Contains("UX_WorkflowStepExecution_Instance_Order_Attempt", indexes);
        Assert.Contains("IX_WorkflowOutboxMessage_Status_NextAttempt_Lease", indexes);
        Assert.Contains("IX_WorkflowOutboxMessage_WorkflowInstanceId", indexes);

        var outboxLeaseIndexColumns = await GetIndexKeyColumnNamesAsync(
            database.ConnectionString,
            "WorkflowOutboxMessage",
            "IX_WorkflowOutboxMessage_Status_NextAttempt_Lease");
        Assert.Equal(
            ["Status", "NextAttemptAtUtc", "LeaseUntilUtc", "CreatedAtUtc"],
            outboxLeaseIndexColumns);
    }

    [SqlServerFact]
    public async Task Post_fulfillment_saga_returns_accepted_after_sql_persistence()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        using var factory = new SqlWorkflowApiFactory(database.ConnectionString);
        var client = factory.CreateClient();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        WorkflowInstanceRecord? instance = null;
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"endpoint-sql-{Guid.NewGuid():N}";

        try
        {
            using var response = await PostFulfillmentSagaAsync(
                client,
                orderId,
                idempotencyKey);

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

            instance = await repository.GetInstanceByCorrelationAsync(
                WorkflowIds.OrderFulfillment,
                orderId.ToString("D"));

            var persisted = instance
                ?? throw new InvalidOperationException("Expected persisted workflow instance.");

            using var payload = await ReadJsonAsync(response);
            var root = payload.RootElement;

            Assert.Equal(persisted.Id, root.GetProperty("workflowInstanceId").GetGuid());
            Assert.Equal(persisted.CommandId, root.GetProperty("commandId").GetGuid());
            Assert.Equal(WorkflowIds.OrderFulfillment, persisted.WorkflowId);
            Assert.Equal(orderId.ToString("D"), persisted.CorrelationId);
            Assert.Equal(WorkflowState.Pending, persisted.State);
            Assert.Equal(idempotencyKey, persisted.IdempotencyKey);
            Assert.Contains(
                OrderFulfillmentWorkflowContext.OrderId,
                persisted.PayloadJson,
                StringComparison.Ordinal);
        }
        finally
        {
            if (instance is not null)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Post_fulfillment_saga_repeating_same_request_returns_same_accepted_workflow()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        using var factory = new SqlWorkflowApiFactory(database.ConnectionString);
        var client = factory.CreateClient();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        WorkflowInstanceRecord? instance = null;
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"endpoint-sql-{Guid.NewGuid():N}";

        try
        {
            using var firstResponse = await PostFulfillmentSagaAsync(
                client,
                orderId,
                idempotencyKey,
                PaymentMethod.EWallet,
                51.00m);
            using var secondResponse = await PostFulfillmentSagaAsync(
                client,
                orderId,
                idempotencyKey,
                PaymentMethod.EWallet,
                51.00m);

            Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);
            Assert.Equal(HttpStatusCode.Accepted, secondResponse.StatusCode);

            using var firstPayload = await ReadJsonAsync(firstResponse);
            using var secondPayload = await ReadJsonAsync(secondResponse);
            var firstRoot = firstPayload.RootElement;
            var secondRoot = secondPayload.RootElement;

            Assert.Equal(
                firstRoot.GetProperty("workflowInstanceId").GetGuid(),
                secondRoot.GetProperty("workflowInstanceId").GetGuid());
            Assert.Equal(
                firstRoot.GetProperty("commandId").GetGuid(),
                secondRoot.GetProperty("commandId").GetGuid());
            Assert.Equal(
                firstRoot.GetProperty("acceptedAtUtc").GetDateTimeOffset(),
                secondRoot.GetProperty("acceptedAtUtc").GetDateTimeOffset());

            instance = await repository.GetInstanceByCorrelationAsync(
                WorkflowIds.OrderFulfillment,
                orderId.ToString("D"));

            var persisted = instance
                ?? throw new InvalidOperationException("Expected persisted workflow instance.");

            Assert.Equal(
                firstRoot.GetProperty("workflowInstanceId").GetGuid(),
                persisted.Id);
        }
        finally
        {
            if (instance is not null)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Post_fulfillment_saga_conflicting_idempotency_and_correlation_requests_return_conflict()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        using var factory = new SqlWorkflowApiFactory(database.ConnectionString);
        var client = factory.CreateClient();
        var createdInstanceIds = new List<Guid>();

        try
        {
            var idempotencyKey = $"endpoint-sql-{Guid.NewGuid():N}";
            using var idempotencySource = await PostFulfillmentSagaAsync(
                client,
                Guid.NewGuid(),
                idempotencyKey);
            Assert.Equal(HttpStatusCode.Accepted, idempotencySource.StatusCode);
            createdInstanceIds.Add(await ReadWorkflowInstanceIdAsync(idempotencySource));

            using var idempotencyConflict = await PostFulfillmentSagaAsync(
                client,
                Guid.NewGuid(),
                idempotencyKey);

            Assert.Equal(HttpStatusCode.Conflict, idempotencyConflict.StatusCode);
            using var idempotencyPayload = await ReadJsonAsync(idempotencyConflict);
            Assert.Equal(
                "workflow.idempotency_conflict",
                idempotencyPayload.RootElement.GetProperty("code").GetString());

            var correlationOrderId = Guid.NewGuid();
            using var correlationSource = await PostFulfillmentSagaAsync(
                client,
                correlationOrderId,
                $"endpoint-sql-{Guid.NewGuid():N}");
            Assert.Equal(HttpStatusCode.Accepted, correlationSource.StatusCode);
            createdInstanceIds.Add(await ReadWorkflowInstanceIdAsync(correlationSource));

            using var correlationConflict = await PostFulfillmentSagaAsync(
                client,
                correlationOrderId,
                $"endpoint-sql-{Guid.NewGuid():N}");

            Assert.Equal(HttpStatusCode.Conflict, correlationConflict.StatusCode);
            using var correlationPayload = await ReadJsonAsync(correlationConflict);
            Assert.Equal(
                "workflow.correlation_conflict",
                correlationPayload.RootElement.GetProperty("code").GetString());
        }
        finally
        {
            foreach (var instanceId in createdInstanceIds)
            {
                await database.DeleteWorkflowInstanceAsync(instanceId);
            }
        }
    }

    [SqlServerFact]
    public async Task Get_fulfillment_saga_returns_sql_persisted_workflow_status()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        using var factory = new SqlWorkflowApiFactory(database.ConnectionString);
        var client = factory.CreateClient();
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        Guid? instanceId = null;
        var orderId = Guid.NewGuid();
        var idempotencyKey = $"endpoint-sql-{Guid.NewGuid():N}";

        try
        {
            using var startResponse = await PostFulfillmentSagaAsync(
                client,
                orderId,
                idempotencyKey,
                PaymentMethod.Card,
                42.75m);

            Assert.Equal(HttpStatusCode.Accepted, startResponse.StatusCode);
            using var startPayload = await ReadJsonAsync(startResponse);
            var startRoot = startPayload.RootElement;
            instanceId = startRoot.GetProperty("workflowInstanceId").GetGuid();
            var commandId = startRoot.GetProperty("commandId").GetGuid();

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instanceId.Value,
                    "SendOrderToKitchenStep",
                    StepOrder: 1,
                    Attempt: 1,
                    commandId,
                    DateTimeOffset.UtcNow));

            using var statusResponse = await client.GetAsync(
                $"/orders/{orderId:D}/fulfillment-saga");

            Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);

            using var statusPayload = await ReadJsonAsync(statusResponse);
            var root = statusPayload.RootElement;

            Assert.Equal(instanceId.Value, root.GetProperty("workflowInstanceId").GetGuid());
            Assert.Equal(commandId, root.GetProperty("commandId").GetGuid());
            Assert.Equal(WorkflowIds.OrderFulfillment, root.GetProperty("workflowId").GetString());
            Assert.Equal(orderId.ToString("D"), root.GetProperty("correlationId").GetString());
            Assert.Equal((int)WorkflowState.Pending, root.GetProperty("state").GetInt32());
            Assert.Equal(idempotencyKey, root.GetProperty("idempotencyKey").GetString());
            Assert.Equal(
                orderId.ToString("D"),
                root.GetProperty("items")
                    .GetProperty(OrderFulfillmentWorkflowContext.OrderId)
                    .GetString());

            var step = Assert.Single(root.GetProperty("steps").EnumerateArray());
            Assert.Equal(1, step.GetProperty("order").GetInt32());
            Assert.Equal("SendOrderToKitchenStep", step.GetProperty("name").GetString());
            Assert.Equal((int)WorkflowStepAttemptStatus.Running, step.GetProperty("status").GetInt32());
        }
        finally
        {
            if (instanceId.HasValue)
            {
                await database.DeleteWorkflowInstanceAsync(instanceId.Value);
            }
        }
    }

    [SqlServerFact]
    public async Task Get_workflow_instance_returns_sql_step_summaries()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        using var factory = new SqlWorkflowApiFactory(database.ConnectionString);
        var client = factory.CreateClient();
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        Guid? instanceId = null;
        var orderId = Guid.NewGuid();

        try
        {
            using var startResponse = await PostFulfillmentSagaAsync(
                client,
                orderId,
                $"endpoint-sql-{Guid.NewGuid():N}");

            Assert.Equal(HttpStatusCode.Accepted, startResponse.StatusCode);
            using var startPayload = await ReadJsonAsync(startResponse);
            var startRoot = startPayload.RootElement;
            instanceId = startRoot.GetProperty("workflowInstanceId").GetGuid();
            var commandId = startRoot.GetProperty("commandId").GetGuid();

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instanceId.Value,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 1,
                    commandId,
                    DateTimeOffset.UtcNow.AddSeconds(-4)));
            _ = await logRepository.MarkStepFailedAsync(
                instanceId.Value,
                stepOrder: 2,
                attempt: 1,
                "Transient card gateway error.",
                DateTimeOffset.UtcNow.AddSeconds(-3));
            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instanceId.Value,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 2,
                    commandId,
                    DateTimeOffset.UtcNow.AddSeconds(-2)));
            _ = await logRepository.MarkStepSucceededAsync(
                instanceId.Value,
                stepOrder: 2,
                attempt: 2,
                DateTimeOffset.UtcNow.AddSeconds(-1));
            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instanceId.Value,
                    "DeductOrderInventoryStep",
                    StepOrder: 3,
                    Attempt: 1,
                    commandId,
                    DateTimeOffset.UtcNow));

            using var statusResponse = await client.GetAsync(
                $"/workflows/{WorkflowIds.OrderFulfillment}/instances/{orderId:D}");

            Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);

            using var statusPayload = await ReadJsonAsync(statusResponse);
            var steps = statusPayload.RootElement.GetProperty("steps")
                .EnumerateArray()
                .ToArray();

            Assert.Equal(2, steps.Length);
            Assert.Equal(2, steps[0].GetProperty("order").GetInt32());
            Assert.Equal("CaptureOrderPaymentStep", steps[0].GetProperty("name").GetString());
            Assert.Equal((int)WorkflowStepAttemptStatus.Succeeded, steps[0].GetProperty("status").GetInt32());
            Assert.Equal(2, steps[0].GetProperty("attempt").GetInt32());
            Assert.Equal(3, steps[1].GetProperty("order").GetInt32());
            Assert.Equal("DeductOrderInventoryStep", steps[1].GetProperty("name").GetString());
            Assert.Equal((int)WorkflowStepAttemptStatus.Running, steps[1].GetProperty("status").GetInt32());
            Assert.Equal(1, steps[1].GetProperty("attempt").GetInt32());
        }
        finally
        {
            if (instanceId.HasValue)
            {
                await database.DeleteWorkflowInstanceAsync(instanceId.Value);
            }
        }
    }

    [SqlServerFact]
    public async Task Full_fulfillment_workflow_uses_sql_fnb_journal_claim_compensation_and_status_query()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        await database.ApplyFnbSchemaAsync();

        using var factory = new SqlWorkflowApiFactory(
            database.ConnectionString,
            useSqlFnb: true);
        var client = factory.CreateClient();
        var fnbRepository = new SqlServerFnbRepository(database.ConnectionString);
        var workflowRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var startingStock = await database.GetFnbInventoryQuantityAsync(
            CompensationWorkflowInventoryItemId);
        ServiceOrder? order = null;
        Guid? instanceId = null;

        try
        {
            await database.SetFnbTableStatusAsync(
                CompensationWorkflowTableId,
                TableStatus.Available);
            await database.SetFnbInventoryQuantityAsync(
                CompensationWorkflowInventoryItemId,
                1m);

            var menuItem = (await fnbRepository.GetMenuItemsAsync())
                .Single(item => item.Id == CompensationWorkflowMenuItemId);
            order = await fnbRepository.AddOrderAsync(
                CompensationWorkflowTableId,
                [new OrderLine(menuItem.Id, 2, menuItem.Price)]);

            using var startResponse = await PostFulfillmentSagaAsync(
                client,
                order.Id,
                $"full-sql-compensation-{Guid.NewGuid():N}",
                PaymentMethod.Card,
                order.TotalAmount);

            Assert.Equal(HttpStatusCode.Accepted, startResponse.StatusCode);
            instanceId = await ReadWorkflowInstanceIdAsync(startResponse);

            var claimed = Assert.Single(
                await workflowRepository.ClaimDueInstancesAsync(
                    DateTimeOffset.UtcNow,
                    "sql-full-compensation-worker",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => DispatchClaimedAsync(factory.Services, claimed));

            Assert.Equal(
                "Insufficient inventory for 'Grilled Lobster Tail'.",
                exception.Message);

            var persisted = await workflowRepository.GetInstanceByIdAsync(claimed.Id);

            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Compensated, persisted.State);
            Assert.Null(persisted.LeaseOwner);
            Assert.Null(persisted.LeaseUntilUtc);

            var attempts = await logRepository.GetStepAttemptsAsync(claimed.Id);
            Assert.Contains(
                attempts,
                attempt =>
                    attempt.StepName == "SendOrderToKitchenStep" &&
                    attempt.Status == WorkflowStepAttemptStatus.Compensated);
            Assert.Contains(
                attempts,
                attempt =>
                    attempt.StepName == "CaptureOrderPaymentStep" &&
                    attempt.Status == WorkflowStepAttemptStatus.Compensated);
            Assert.Contains(
                attempts,
                attempt =>
                    attempt.StepName == "DeductOrderInventoryStep" &&
                    attempt.Status == WorkflowStepAttemptStatus.Failed);

            var payment = Assert.Single(
                await fnbRepository.GetPaymentsAsync(),
                item => item.OrderId == order.Id);
            Assert.Equal(PaymentStatus.Refunded, payment.Status);
            Assert.Equal(
                OrderStatus.Open,
                (await fnbRepository.GetOrdersAsync())
                .Single(item => item.Id == order.Id)
                .Status);
            Assert.Equal(
                1m,
                await database.GetFnbInventoryQuantityAsync(
                    CompensationWorkflowInventoryItemId));

            using var statusResponse = await client.GetAsync(
                $"/orders/{order.Id:D}/fulfillment-saga");

            Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
            using var statusPayload = await ReadJsonAsync(statusResponse);
            var root = statusPayload.RootElement;

            Assert.Equal(instanceId.Value, root.GetProperty("workflowInstanceId").GetGuid());
            Assert.Equal(WorkflowIds.OrderFulfillment, root.GetProperty("workflowId").GetString());
            Assert.Equal((int)WorkflowState.Compensated, root.GetProperty("state").GetInt32());
            Assert.NotEmpty(root.GetProperty("steps").EnumerateArray());
        }
        finally
        {
            await database.SetFnbTableStatusAsync(
                CompensationWorkflowTableId,
                TableStatus.Available);
            await database.SetFnbInventoryQuantityAsync(
                CompensationWorkflowInventoryItemId,
                startingStock);

            if (instanceId.HasValue)
            {
                await database.DeleteWorkflowInstanceAsync(instanceId.Value);
            }

            if (order is not null)
            {
                await database.DeleteFnbOrderAsync(order.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Process_restart_reclaims_due_work_and_completes_from_sql_state()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        await database.ApplyFnbSchemaAsync();

        var fnbRepository = new SqlServerFnbRepository(database.ConnectionString);
        var startingStock = await database.GetFnbInventoryQuantityAsync(
            RestartWorkflowInventoryItemId);
        ServiceOrder? order = null;
        Guid? instanceId = null;

        try
        {
            await database.SetFnbTableStatusAsync(
                RestartWorkflowTableId,
                TableStatus.Available);
            await database.SetFnbInventoryQuantityAsync(
                RestartWorkflowInventoryItemId,
                10m);

            var menuItem = (await fnbRepository.GetMenuItemsAsync())
                .Single(item => item.Id == RestartWorkflowMenuItemId);
            order = await fnbRepository.AddOrderAsync(
                RestartWorkflowTableId,
                [new OrderLine(menuItem.Id, 2, menuItem.Price)]);

            using (var initialFactory = new SqlWorkflowApiFactory(
                       database.ConnectionString,
                       useSqlFnb: true))
            {
                var initialClient = initialFactory.CreateClient();
                using var startResponse = await PostFulfillmentSagaAsync(
                    initialClient,
                    order.Id,
                    $"restart-sql-{Guid.NewGuid():N}",
                    PaymentMethod.EWallet,
                    order.TotalAmount);

                Assert.Equal(HttpStatusCode.Accepted, startResponse.StatusCode);
                instanceId = await ReadWorkflowInstanceIdAsync(startResponse);
            }

            using var restartedFactory = new SqlWorkflowApiFactory(
                database.ConnectionString,
                useSqlFnb: true);
            var restartedStore =
                restartedFactory.Services.GetRequiredService<IWorkflowInstanceStore>();

            var claimed = Assert.Single(
                await restartedStore.ClaimDueInstancesAsync(
                    DateTimeOffset.UtcNow,
                    "sql-restarted-worker",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1));

            Assert.Equal(instanceId.Value, claimed.Id);

            await DispatchClaimedAsync(restartedFactory.Services, claimed);

            var persisted = await restartedStore.GetInstanceByIdAsync(instanceId.Value);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Succeeded, persisted.State);
            Assert.Null(persisted.LeaseOwner);
            Assert.Null(persisted.LeaseUntilUtc);

            var restartedFnb = new SqlServerFnbRepository(database.ConnectionString);
            var completedOrder = (await restartedFnb.GetOrdersAsync())
                .Single(item => item.Id == order.Id);

            Assert.Equal(OrderStatus.Completed, completedOrder.Status);
            Assert.Equal(
                8m,
                await database.GetFnbInventoryQuantityAsync(
                    RestartWorkflowInventoryItemId));
            Assert.Single(
                await restartedFnb.GetPaymentsAsync(),
                payment =>
                    payment.OrderId == order.Id &&
                    payment.Status == PaymentStatus.Settled);

            var attempts = await new WorkflowExecutionLogRepository(database.ConnectionString)
                .GetStepAttemptsAsync(instanceId.Value);

            Assert.Equal(4, attempts.Count(attempt => attempt.Status == WorkflowStepAttemptStatus.Succeeded));
        }
        finally
        {
            await database.SetFnbTableStatusAsync(
                RestartWorkflowTableId,
                TableStatus.Available);
            await database.SetFnbInventoryQuantityAsync(
                RestartWorkflowInventoryItemId,
                startingStock);

            if (instanceId.HasValue)
            {
                await database.DeleteWorkflowInstanceAsync(instanceId.Value);
            }

            if (order is not null)
            {
                await database.DeleteFnbOrderAsync(order.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Legacy_saga_is_query_only_and_workflow_status_wins_when_both_records_exist()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        await database.ApplyLegacySagaSchemaAsync();

        using var factory = new SqlWorkflowApiFactory(
            database.ConnectionString,
            useSqlLegacySaga: true);
        var client = factory.CreateClient();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        var orderId = Guid.NewGuid();
        WorkflowInstanceRecord? instance = null;
        Guid? sagaId = null;

        try
        {
            var started = await repository.StartWorkflowAsync(
                CreateStartRequest(
                    orderId.ToString("D"),
                    $"workflow-wins-{Guid.NewGuid():N}",
                    new string('d', 64)));

            instance = started.Instance
                ?? throw new InvalidOperationException("Expected workflow instance.");
            sagaId = await database.InsertLegacySagaAsync(orderId);

            Assert.Contains(
                typeof(ISagaStateQueryPort),
                typeof(SqlServerSagaStateAdapter).GetInterfaces());
            Assert.Null(
                typeof(SqlServerSagaStateAdapter).GetMethod(
                    "SaveOrderFulfillmentSagaAsync"));

            using var statusResponse = await client.GetAsync(
                $"/orders/{orderId:D}/fulfillment-saga");

            Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
            using var payload = await ReadJsonAsync(statusResponse);
            var root = payload.RootElement;

            Assert.Equal(instance.Id, root.GetProperty("workflowInstanceId").GetGuid());
            Assert.Equal(WorkflowIds.OrderFulfillment, root.GetProperty("workflowId").GetString());
            Assert.Equal(orderId.ToString("D"), root.GetProperty("correlationId").GetString());
            Assert.False(root.TryGetProperty("sagaId", out _));
        }
        finally
        {
            if (instance is not null)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }

            if (sagaId.HasValue)
            {
                await database.DeleteLegacySagaAsync(sagaId.Value);
            }
        }
    }

    [SqlServerFact]
    public async Task WorkflowInstanceStore_starts_idempotently_claims_and_updates_instances()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        IWorkflowInstanceStore store = new WorkflowInstanceRepository(database.ConnectionString);
        WorkflowInstanceRecord? instance = null;
        var requestedAtUtc = DateTimeOffset.UtcNow.AddSeconds(-5);
        var requestHash = new string('a', 64);
        var request = new WorkflowStartRequest(
            WorkflowIds.OrderFulfillment,
            "Order Fulfillment",
            Guid.NewGuid().ToString("D"),
            """{"source":"store-test"}""",
            $"workflow-store-test-{Guid.NewGuid():N}",
            requestHash,
            "sql-tests",
            requestedAtUtc);

        try
        {
            var started = await store.StartWorkflowAsync(request);

            Assert.Equal(WorkflowStartStatus.Created, started.Status);
            Assert.NotNull(started.Instance);
            instance = started.Instance;
            Assert.Equal(WorkflowState.Pending, instance.State);
            Assert.Equal(0, instance.Attempt);
            Assert.Equal(request.IdempotencyKey, instance.IdempotencyKey);
            Assert.Equal(requestHash, instance.RequestHash);

            var repeated = await store.StartWorkflowAsync(request);

            Assert.Equal(WorkflowStartStatus.Existing, repeated.Status);
            Assert.NotNull(repeated.Instance);
            Assert.Equal(instance.CommandId, repeated.Instance.CommandId);

            var conflict = await store.StartWorkflowAsync(
                request with { RequestHash = new string('b', 64) });

            Assert.Equal(WorkflowStartStatus.IdempotencyConflict, conflict.Status);
            Assert.Equal(requestHash, conflict.ExistingRequestHash);

            var claimed = await store.ClaimDueInstancesAsync(
                DateTimeOffset.UtcNow,
                "sql-test-worker",
                TimeSpan.FromMinutes(5),
                batchSize: 1);

            var claimedInstance = Assert.Single(claimed);
            Assert.Equal(instance.Id, claimedInstance.Id);
            Assert.Equal(WorkflowState.Running, claimedInstance.State);
            Assert.Equal(1, claimedInstance.Attempt);
            Assert.Equal("sql-test-worker", claimedInstance.LeaseOwner);
            Assert.NotNull(claimedInstance.LeaseUntilUtc);

            var payloadUpdated = await store.TryUpdatePayloadAsync(
                claimedInstance.Id,
                claimedInstance.Version,
                """{"source":"store-test","progress":1}""");

            Assert.True(payloadUpdated);

            var withPayload = await store.GetInstanceByIdAsync(claimedInstance.Id);

            Assert.NotNull(withPayload);
            Assert.Contains(
                @"""progress"":1",
                withPayload.PayloadJson,
                StringComparison.Ordinal);

            var released = await store.TryReleaseLeaseAsync(
                withPayload.Id,
                "sql-test-worker",
                withPayload.Version);

            Assert.True(released);

            var afterRelease = await store.GetInstanceByIdAsync(claimedInstance.Id);

            Assert.NotNull(afterRelease);
            Assert.Null(afterRelease.LeaseOwner);
            Assert.Null(afterRelease.LeaseUntilUtc);
        }
        finally
        {
            if (instance is not null)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Pending_workflow_can_be_claimed_by_only_one_worker()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        IWorkflowInstanceStore store = new WorkflowInstanceRepository(database.ConnectionString);
        WorkflowInstanceRecord? instance = null;

        try
        {
            var started = await store.StartWorkflowAsync(CreateStartRequest(
                correlationId: Guid.NewGuid().ToString("D"),
                idempotencyKey: $"claim-once-{Guid.NewGuid():N}",
                requestHash: new string('c', 64)));

            Assert.NotNull(started.Instance);
            instance = started.Instance;
            var nowUtc = DateTimeOffset.UtcNow;

            var claimResults = await Task.WhenAll(
                store.ClaimDueInstancesAsync(
                    nowUtc,
                    "sql-worker-one",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1),
                store.ClaimDueInstancesAsync(
                    nowUtc,
                    "sql-worker-two",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1));

            var claimedInstances = claimResults.SelectMany(result => result).ToArray();
            var claimed = Assert.Single(claimedInstances);

            Assert.Equal(instance.Id, claimed.Id);
            Assert.Equal(WorkflowState.Running, claimed.State);
            Assert.True(claimed.LeaseOwner is "sql-worker-one" or "sql-worker-two");

            var notClaimedAgain = await store.ClaimDueInstancesAsync(
                nowUtc,
                "sql-worker-three",
                TimeSpan.FromMinutes(5),
                batchSize: 1);

            Assert.Empty(notClaimedAgain);
        }
        finally
        {
            if (instance is not null)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Expired_running_lease_can_be_reclaimed()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"expired-lease-test"}""",
            Attempt = 1,
            LeaseOwner = "expired-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(-5)
        };

        try
        {
            await repository.SaveAsync(instance);

            var claimed = await repository.ClaimDueInstancesAsync(
                DateTimeOffset.UtcNow,
                "replacement-worker",
                TimeSpan.FromMinutes(5),
                batchSize: 1);

            var reclaimed = Assert.Single(claimed);
            Assert.Equal(instance.Id, reclaimed.Id);
            Assert.Equal(WorkflowState.Running, reclaimed.State);
            Assert.Equal(2, reclaimed.Attempt);
            Assert.Equal("replacement-worker", reclaimed.LeaseOwner);
            Assert.NotNull(reclaimed.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task Workflow_instance_updates_can_be_fenced_by_lease_owner()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"lease-fence-test"}""",
            Attempt = 1,
            LeaseOwner = "current-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await repository.SaveAsync(instance);

            var stalePayloadUpdate = await repository.TryUpdatePayloadAsync(
                instance.Id,
                instance.Version,
                """{"source":"stale-worker"}""",
                expectedLeaseOwner: "stale-worker");

            Assert.False(stalePayloadUpdate);

            var afterStalePayload = await repository.GetByIdAsync(instance.Id);

            Assert.NotNull(afterStalePayload);
            Assert.Equal(instance.Version, afterStalePayload.Version);
            Assert.Contains("lease-fence-test", afterStalePayload.PayloadJson, StringComparison.Ordinal);

            var currentPayloadUpdate = await repository.TryUpdatePayloadAsync(
                instance.Id,
                afterStalePayload.Version,
                """{"source":"current-worker"}""",
                expectedLeaseOwner: "current-worker");

            Assert.True(currentPayloadUpdate);

            var afterCurrentPayload = await repository.GetByIdAsync(instance.Id);

            Assert.NotNull(afterCurrentPayload);
            Assert.Equal(afterStalePayload.Version + 1, afterCurrentPayload.Version);
            Assert.Contains("current-worker", afterCurrentPayload.PayloadJson, StringComparison.Ordinal);

            var staleStateUpdate = await repository.TryUpdateStateAsync(
                instance.Id,
                WorkflowState.Failed,
                afterCurrentPayload.Version,
                lastError: "stale worker failed",
                completedAtUtc: DateTimeOffset.UtcNow,
                expectedLeaseOwner: "stale-worker");

            Assert.False(staleStateUpdate);

            var afterStaleState = await repository.GetByIdAsync(instance.Id);

            Assert.NotNull(afterStaleState);
            Assert.Equal(WorkflowState.Running, afterStaleState.State);
            Assert.Equal("current-worker", afterStaleState.LeaseOwner);

            var failedAtUtc = DateTimeOffset.UtcNow;
            var currentStateUpdate = await repository.TryUpdateStateAsync(
                instance.Id,
                WorkflowState.Failed,
                afterStaleState.Version,
                lastError: "current worker failed",
                completedAtUtc: failedAtUtc,
                expectedLeaseOwner: "current-worker");

            Assert.True(currentStateUpdate);

            var afterCurrentState = await repository.GetByIdAsync(instance.Id);

            Assert.NotNull(afterCurrentState);
            Assert.Equal(WorkflowState.Failed, afterCurrentState.State);
            Assert.Equal("current worker failed", afterCurrentState.LastError);
            Assert.Null(afterCurrentState.LeaseOwner);
            Assert.Null(afterCurrentState.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task Terminal_workflows_cannot_be_reset_to_pending_or_claimed()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        var instances = new[]
        {
            CreateTerminalInstance(WorkflowState.Succeeded),
            CreateTerminalInstance(WorkflowState.Failed),
            CreateTerminalInstance(WorkflowState.Compensated),
            CreateTerminalInstance(WorkflowState.CompensationFailed),
            CreateTerminalInstance(WorkflowState.Cancelled)
        };

        try
        {
            foreach (var instance in instances)
            {
                await repository.SaveAsync(instance);

                var reset = await repository.TryUpdateStateAsync(
                    instance.Id,
                    WorkflowState.Pending,
                    instance.Version,
                    nextAttemptAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1));

                Assert.False(reset);

                var persisted = await repository.GetByIdAsync(instance.Id);

                Assert.NotNull(persisted);
                Assert.Equal(instance.State, persisted.State);
                Assert.Equal(instance.Version, persisted.Version);
                Assert.Null(persisted.NextAttemptAtUtc);
                Assert.Null(persisted.LeaseOwner);
                Assert.Null(persisted.LeaseUntilUtc);
                Assert.NotNull(persisted.CompletedAtUtc);
            }

            var claimed = await repository.ClaimDueInstancesAsync(
                DateTimeOffset.UtcNow,
                "terminal-state-worker",
                TimeSpan.FromMinutes(5),
                batchSize: instances.Length);

            Assert.DoesNotContain(
                claimed,
                claimedInstance => instances.Any(instance => instance.Id == claimedInstance.Id));
        }
        finally
        {
            foreach (var instance in instances)
            {
                await database.DeleteWorkflowInstanceAsync(instance.Id);
            }
        }
    }

    [SqlServerFact]
    public async Task Workflow_context_payload_is_persisted_after_each_engine_step()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = "sql-payload-workflow",
            WorkflowName = "SQL Payload Workflow",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"payload-test"}""",
            Attempt = 1,
            LeaseOwner = "sql-payload-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var services = new ServiceCollection();
            services.AddSingleton<IWorkflowInstanceStore>(instanceRepository);
            services.AddSingleton<IWorkflowExecutionLogStore>(logRepository);
            services.AddSingleton<IWorkflowJournal>(
                new SqlServerWorkflowJournal(database.ConnectionString));
            services.AddTransient<SqlFirstPayloadStep>();
            services.AddTransient<SqlSecondPayloadStep>();

            var engine = CreateSqlTestEngine(services, instance.WorkflowId);
            var context = new WorkflowContext(
                instance.WorkflowId,
                instance.CorrelationId)
            {
                WorkflowInstanceId = instance.Id,
                WorkflowInstanceVersion = instance.Version,
                WorkflowAttempt = instance.Attempt,
                WorkflowLeaseOwner = instance.LeaseOwner,
                CommandId = instance.CommandId
            };
            context.Set("source", "payload-test");

            await engine.ExecuteAsync(
                instance.WorkflowId,
                WorkflowDefinition.Create(
                    instance.WorkflowName,
                    typeof(SqlFirstPayloadStep),
                    typeof(SqlSecondPayloadStep)),
                context);

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);

            Assert.NotNull(persisted);
            Assert.Contains(@"""stepOne"":true", persisted.PayloadJson, StringComparison.Ordinal);
            Assert.Contains(@"""stepTwo"":true", persisted.PayloadJson, StringComparison.Ordinal);
            Assert.Equal(5, persisted.Version);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task Workflow_engine_does_not_duplicate_started_event_when_recovering_existing_attempts()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        IWorkflowOutboxStore outboxStore = logRepository;
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = "sql-start-recovery-workflow",
            WorkflowName = "SQL Start Recovery Workflow",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"start-recovery-test"}""",
            Attempt = 2,
            LeaseOwner = "sql-start-recovery-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            _ = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    WorkflowEvents.WorkflowStarted,
                    "workflow.events",
                    """{"eventName":"workflow.started"}""",
                    CreatedAtUtc: DateTimeOffset.UtcNow.AddMinutes(-2),
                    NextAttemptAtUtc: DateTimeOffset.UtcNow.AddMinutes(-2)));

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instance.Id,
                    nameof(SqlFirstPayloadStep),
                    StepOrder: 1,
                    Attempt: 1,
                    instance.CommandId,
                    DateTimeOffset.UtcNow.AddMinutes(-1)));
            _ = await logRepository.MarkStepSucceededAsync(
                instance.Id,
                stepOrder: 1,
                attempt: 1,
                DateTimeOffset.UtcNow.AddSeconds(-30));

            var services = new ServiceCollection();
            services.AddSingleton<IWorkflowInstanceStore>(instanceRepository);
            services.AddSingleton<IWorkflowExecutionLogStore>(logRepository);
            services.AddSingleton<IWorkflowJournal>(
                new SqlServerWorkflowJournal(database.ConnectionString));
            services.AddTransient<SqlFirstPayloadStep>();
            services.AddTransient<SqlSecondPayloadStep>();

            var saved = await instanceRepository.GetByIdAsync(instance.Id)
                ?? throw new InvalidOperationException("Expected saved workflow instance.");
            var engine = CreateSqlTestEngine(services, instance.WorkflowId);
            var context = new WorkflowContext(
                instance.WorkflowId,
                instance.CorrelationId)
            {
                WorkflowInstanceId = instance.Id,
                WorkflowInstanceVersion = saved.Version,
                WorkflowAttempt = saved.Attempt,
                WorkflowLeaseOwner = saved.LeaseOwner,
                CommandId = saved.CommandId
            };
            context.Set("source", "start-recovery-test");

            await engine.ExecuteAsync(
                instance.WorkflowId,
                WorkflowDefinition.Create(
                    instance.WorkflowName,
                    typeof(SqlFirstPayloadStep),
                    typeof(SqlSecondPayloadStep)),
                context);

            var messages = await logRepository.GetMessagesForWorkflowAsync(instance.Id);

            Assert.Single(
                messages,
                message => message.MessageType == WorkflowEvents.WorkflowStarted);
            Assert.Contains(
                messages,
                message => message.MessageType == WorkflowEvents.WorkflowSucceeded);

            var attempts = await logRepository.GetStepAttemptsAsync(instance.Id);
            Assert.Contains(
                attempts,
                attempt =>
                    attempt.StepName == nameof(SqlFirstPayloadStep) &&
                    attempt.Status == WorkflowStepAttemptStatus.Succeeded);
            Assert.Contains(
                attempts,
                attempt =>
                    attempt.StepName == nameof(SqlSecondPayloadStep) &&
                    attempt.Status == WorkflowStepAttemptStatus.Succeeded);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowJournal_commits_state_step_and_outbox_in_one_transaction()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var journal = new SqlServerWorkflowJournal(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"journal-test"}""",
            Attempt = 1,
            LeaseOwner = "sql-journal-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);
            var saved = await instanceRepository.GetByIdAsync(instance.Id)
                ?? throw new InvalidOperationException("Expected saved instance.");

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instance.Id,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 1,
                    instance.CommandId,
                    DateTimeOffset.UtcNow.AddSeconds(-1)));

            var completedAtUtc = DateTimeOffset.UtcNow;
            _ = await journal.AppendStepAttemptTransitionAsync(
                new WorkflowStepAttemptTransitionJournalEntry(
                    instance.Id,
                    saved.Version,
                    "sql-journal-worker",
                    Step: new WorkflowStepJournalEntry(
                        WorkflowStepJournalAction.Succeeded,
                        "CaptureOrderPaymentStep",
                        StepOrder: 2,
                        Attempt: 1,
                        CompletedAtUtc: completedAtUtc),
                    Event: new WorkflowEvent(
                        WorkflowEvents.WorkflowSucceeded,
                        instance.WorkflowId,
                        instance.WorkflowName,
                        instance.CorrelationId,
                        WorkflowState.Succeeded,
                        StepName: null,
                        Error: null,
                        completedAtUtc)
                    {
                        WorkflowInstanceId = instance.Id
                    },
                    PayloadJson: """{"source":"journal-test","completed":true}""",
                    State: new WorkflowStateJournalEntry(
                        WorkflowState.Succeeded,
                        CompletedAtUtc: completedAtUtc,
                        ClearLease: true)));

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Succeeded, persisted.State);
            Assert.Contains(@"""completed"":true", persisted.PayloadJson, StringComparison.Ordinal);
            Assert.Equal(saved.Version + 2, persisted.Version);
            Assert.NotNull(persisted.CompletedAtUtc);
            Assert.Null(persisted.LeaseOwner);
            Assert.Null(persisted.LeaseUntilUtc);

            var attempt = Assert.Single(
                await logRepository.GetStepAttemptsAsync(instance.Id));
            Assert.Equal(WorkflowStepAttemptStatus.Succeeded, attempt.Status);
            Assert.NotNull(attempt.CompletedAtUtc);

            var message = Assert.Single(
                await logRepository.GetMessagesForWorkflowAsync(instance.Id));
            Assert.Equal(WorkflowEvents.WorkflowSucceeded, message.MessageType);
            Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowJournal_rolls_back_step_when_state_update_loses_concurrency()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var journal = new SqlServerWorkflowJournal(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"journal-rollback-test"}""",
            Attempt = 1,
            LeaseOwner = "sql-journal-rollback-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);
            var saved = await instanceRepository.GetByIdAsync(instance.Id)
                ?? throw new InvalidOperationException("Expected saved instance.");

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instance.Id,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 1,
                    instance.CommandId,
                    DateTimeOffset.UtcNow.AddSeconds(-1)));

            var completedAtUtc = DateTimeOffset.UtcNow;
            _ = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                journal.AppendStepAttemptTransitionAsync(
                    new WorkflowStepAttemptTransitionJournalEntry(
                        instance.Id,
                        saved.Version + 100,
                        "sql-journal-rollback-worker",
                        Step: new WorkflowStepJournalEntry(
                            WorkflowStepJournalAction.Succeeded,
                            "CaptureOrderPaymentStep",
                            StepOrder: 2,
                            Attempt: 1,
                            CompletedAtUtc: completedAtUtc),
                        Event: new WorkflowEvent(
                            WorkflowEvents.WorkflowSucceeded,
                            instance.WorkflowId,
                            instance.WorkflowName,
                            instance.CorrelationId,
                            WorkflowState.Succeeded,
                            StepName: null,
                            Error: null,
                            completedAtUtc)
                        {
                            WorkflowInstanceId = instance.Id
                        },
                        State: new WorkflowStateJournalEntry(
                            WorkflowState.Succeeded,
                            CompletedAtUtc: completedAtUtc))));

            var attempt = Assert.Single(
                await logRepository.GetStepAttemptsAsync(instance.Id));
            Assert.Equal(WorkflowStepAttemptStatus.Running, attempt.Status);

            Assert.Empty(await logRepository.GetMessagesForWorkflowAsync(instance.Id));

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Running, persisted.State);
            Assert.Equal(saved.Version, persisted.Version);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowJournal_rejects_wrong_lease_owner_and_rolls_back_step_and_outbox_changes()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var journal = new SqlServerWorkflowJournal(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"journal-lease-test"}""",
            Attempt = 1,
            LeaseOwner = "current-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);
            var saved = await instanceRepository.GetByIdAsync(instance.Id)
                ?? throw new InvalidOperationException("Expected saved instance.");

            _ = await logRepository.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instance.Id,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 1,
                    instance.CommandId,
                    DateTimeOffset.UtcNow.AddSeconds(-1)));

            var completedAtUtc = DateTimeOffset.UtcNow;
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                journal.AppendStepAttemptTransitionAsync(
                    new WorkflowStepAttemptTransitionJournalEntry(
                        instance.Id,
                        saved.Version,
                        "stale-worker",
                        Step: new WorkflowStepJournalEntry(
                            WorkflowStepJournalAction.Succeeded,
                            "CaptureOrderPaymentStep",
                            StepOrder: 2,
                            Attempt: 1,
                            CompletedAtUtc: completedAtUtc),
                        Event: new WorkflowEvent(
                            WorkflowEvents.WorkflowSucceeded,
                            instance.WorkflowId,
                            instance.WorkflowName,
                            instance.CorrelationId,
                            WorkflowState.Succeeded,
                            StepName: null,
                            Error: null,
                            completedAtUtc)
                        {
                            WorkflowInstanceId = instance.Id
                        },
                        State: new WorkflowStateJournalEntry(
                            WorkflowState.Succeeded,
                            CompletedAtUtc: completedAtUtc,
                            ClearLease: true))));

            Assert.Equal(
                "Workflow journal append failed due to stale workflow version or lease owner.",
                exception.Message);

            var attempt = Assert.Single(
                await logRepository.GetStepAttemptsAsync(instance.Id));
            Assert.Equal(WorkflowStepAttemptStatus.Running, attempt.Status);

            Assert.Empty(await logRepository.GetMessagesForWorkflowAsync(instance.Id));

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Running, persisted.State);
            Assert.Equal(saved.Version, persisted.Version);
            Assert.Equal("current-worker", persisted.LeaseOwner);
            Assert.NotNull(persisted.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowJournal_retry_scheduling_clears_workflow_lease_and_writes_event()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var journal = new SqlServerWorkflowJournal(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"retry-schedule-test"}""",
            Attempt = 1,
            LeaseOwner = "retry-schedule-worker",
            LeaseUntilUtc = DateTimeOffset.UtcNow.AddMinutes(5)
        };

        try
        {
            await instanceRepository.SaveAsync(instance);
            var saved = await instanceRepository.GetByIdAsync(instance.Id)
                ?? throw new InvalidOperationException("Expected saved instance.");

            var occurredAtUtc = DateTimeOffset.UtcNow;
            var nextAttemptAtUtc = occurredAtUtc.AddSeconds(2);
            var result = await journal.AppendWorkflowStateTransitionAsync(
                new WorkflowStateTransitionJournalEntry(
                    instance.Id,
                    saved.Version,
                    "retry-schedule-worker",
                    State: new WorkflowStateJournalEntry(
                        WorkflowState.Pending,
                        LastError: "temporary dispatch failure",
                        CompletedAtUtc: null,
                        NextAttemptAtUtc: nextAttemptAtUtc,
                        ClearLease: true),
                    Event: new WorkflowEvent(
                        WorkflowEvents.RetryScheduled,
                        instance.WorkflowId,
                        instance.WorkflowName,
                        instance.CorrelationId,
                        WorkflowState.Pending,
                        StepName: null,
                        Error: new InvalidOperationException("temporary dispatch failure"),
                        occurredAtUtc)
                    {
                        WorkflowInstanceId = instance.Id
                    },
                    PayloadJson: """{"source":"retry-schedule-test","retry":true}"""));

            Assert.Equal(saved.Version + 2, result.WorkflowInstanceVersion);

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Pending, persisted.State);
            Assert.Equal("temporary dispatch failure", persisted.LastError);
            Assert.Equal(nextAttemptAtUtc, persisted.NextAttemptAtUtc);
            Assert.Null(persisted.CompletedAtUtc);
            Assert.Contains(@"""retry"":true", persisted.PayloadJson, StringComparison.Ordinal);
            Assert.Null(persisted.LeaseOwner);
            Assert.Null(persisted.LeaseUntilUtc);

            var message = Assert.Single(
                await logRepository.GetMessagesForWorkflowAsync(instance.Id));
            Assert.Equal(WorkflowEvents.RetryScheduled, message.MessageType);
            Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
            Assert.Equal(occurredAtUtc, message.NextAttemptAtUtc);
            Assert.Null(message.LeaseOwner);
            Assert.Null(message.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowOutboxStore_saves_retries_and_marks_messages_published()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        IWorkflowOutboxStore outboxStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"outbox-test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var message = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.lifecycle",
                    "workflow.events",
                    """{"eventName":"workflow.started"}""",
                    """{"workflow-id":"order-fulfillment"}""",
                    CreatedAtUtc: DateTimeOffset.UtcNow.AddMinutes(-1),
                    NextAttemptAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1)));

            Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
            Assert.Equal(0, message.RetryCount);

            var pending = await outboxStore.ClaimPendingMessagesAsync(
                DateTimeOffset.UtcNow,
                "outbox-store-test",
                TimeSpan.FromMinutes(5),
                batchSize: 10);

            Assert.Contains(pending, pendingMessage => pendingMessage.Id == message.Id);
            var claimed = Assert.Single(
                pending,
                pendingMessage => pendingMessage.Id == message.Id);
            Assert.Equal("outbox-store-test", claimed.LeaseOwner);
            Assert.NotNull(claimed.LeaseUntilUtc);

            var stalePublished = await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                "stale-outbox-store-test",
                DateTimeOffset.UtcNow);
            Assert.False(stalePublished);

            var retryAtUtc = DateTimeOffset.UtcNow.AddMinutes(1);
            var markedFailed = await outboxStore.MarkMessageFailedAsync(
                message.Id,
                "outbox-store-test",
                "transport unavailable",
                DateTimeOffset.UtcNow,
                retryAtUtc);
            Assert.True(markedFailed);

            var retried = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Pending, retried.Status);
            Assert.Equal(1, retried.RetryCount);
            Assert.Equal("transport unavailable", retried.ErrorMessage);
            Assert.NotNull(retried.LastAttemptAtUtc);
            Assert.NotNull(retried.NextAttemptAtUtc);
            Assert.Null(retried.LeaseOwner);
            Assert.Null(retried.LeaseUntilUtc);

            var retryClaim = await outboxStore.ClaimPendingMessagesAsync(
                retryAtUtc.AddMilliseconds(1),
                "outbox-store-test",
                TimeSpan.FromMinutes(5),
                batchSize: 10);
            Assert.Contains(retryClaim, pendingMessage => pendingMessage.Id == message.Id);

            var markedPublished = await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                "outbox-store-test",
                DateTimeOffset.UtcNow);
            Assert.True(markedPublished);

            var published = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Published, published.Status);
            Assert.Equal(1, published.RetryCount);
            Assert.Null(published.ErrorMessage);
            Assert.Null(published.NextAttemptAtUtc);
            Assert.NotNull(published.PublishedAtUtc);
            Assert.Null(published.LeaseOwner);
            Assert.Null(published.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowExecutionLogStore_tracks_attempt_lifecycle_and_outbox_messages()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        IWorkflowExecutionLogStore logStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        IWorkflowOutboxStore outboxStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"store-test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var running = await logStore.MarkStepRunningAsync(
                new WorkflowStepAttemptStart(
                    instance.Id,
                    "CaptureOrderPaymentStep",
                    StepOrder: 2,
                    Attempt: 1,
                    CommandId: Guid.NewGuid(),
                    StartedAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1)));

            Assert.Equal(WorkflowStepAttemptStatus.Running, running.Status);

            var succeeded = await logStore.MarkStepSucceededAsync(
                instance.Id,
                stepOrder: 2,
                attempt: 1,
                DateTimeOffset.UtcNow);

            Assert.True(succeeded);

            var attempts = await logStore.GetStepAttemptsAsync(instance.Id);
            var attempt = Assert.Single(attempts);
            Assert.Equal(WorkflowStepAttemptStatus.Succeeded, attempt.Status);
            Assert.NotNull(attempt.CompletedAtUtc);

            var compensated = await logStore.MarkStepCompensatedAsync(
                instance.Id,
                stepOrder: 2,
                attempt: 1,
                DateTimeOffset.UtcNow);

            Assert.True(compensated);

            var compensatedAttempts = await logStore.GetStepAttemptsAsync(instance.Id);
            var compensatedAttempt = Assert.Single(compensatedAttempts);
            Assert.Equal(WorkflowStepAttemptStatus.Compensated, compensatedAttempt.Status);
            Assert.NotNull(compensatedAttempt.CompletedAtUtc);
            Assert.NotNull(compensatedAttempt.CompensatedAtUtc);

            var message = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.step.succeeded",
                    "workflow-events",
                    """{"step":"CaptureOrderPaymentStep"}""",
                    """{"traceId":"sql-tests"}""",
                    CreatedAtUtc: DateTimeOffset.UtcNow.AddMinutes(-1),
                    NextAttemptAtUtc: DateTimeOffset.UtcNow.AddSeconds(-1)));

            Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
            Assert.Equal("""{"traceId":"sql-tests"}""", message.HeadersJson);

            var pending = await outboxStore.ClaimPendingMessagesAsync(
                DateTimeOffset.UtcNow,
                "execution-log-test",
                TimeSpan.FromMinutes(5),
                batchSize: 10);

            Assert.Contains(pending, pendingMessage => pendingMessage.Id == message.Id);

            var nextAttemptAtUtc = DateTimeOffset.UtcNow.AddMinutes(5);
            var markedFailed = await outboxStore.MarkMessageFailedAsync(
                message.Id,
                "execution-log-test",
                "transient failure",
                DateTimeOffset.UtcNow,
                nextAttemptAtUtc);
            Assert.True(markedFailed);

            var afterFailure = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Pending, afterFailure.Status);
            Assert.Equal(1, afterFailure.RetryCount);
            Assert.Null(afterFailure.LeaseOwner);
            Assert.Null(afterFailure.LeaseUntilUtc);

            var publishClaim = await outboxStore.ClaimPendingMessagesAsync(
                nextAttemptAtUtc.AddMilliseconds(1),
                "execution-log-test",
                TimeSpan.FromMinutes(5),
                batchSize: 10);
            Assert.Contains(publishClaim, pendingMessage => pendingMessage.Id == message.Id);

            var markedPublished = await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                "execution-log-test",
                DateTimeOffset.UtcNow);
            Assert.True(markedPublished);

            var afterPublish = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Published, afterPublish.Status);
            Assert.NotNull(afterPublish.PublishedAtUtc);
            Assert.Null(afterPublish.ErrorMessage);
            Assert.Null(afterPublish.LeaseOwner);
            Assert.Null(afterPublish.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowOutboxStore_pending_message_can_be_claimed_by_only_one_publisher()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        IWorkflowOutboxStore outboxStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        var nowUtc = DateTimeOffset.UtcNow;
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"outbox-concurrency-test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var message = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.concurrent-claim",
                    "workflow.events",
                    """{"state":"pending"}""",
                    CreatedAtUtc: nowUtc.AddMinutes(-1),
                    NextAttemptAtUtc: nowUtc.AddSeconds(-1)));

            var claimResults = await Task.WhenAll(
                outboxStore.ClaimPendingMessagesAsync(
                    nowUtc,
                    "publisher-one",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1),
                outboxStore.ClaimPendingMessagesAsync(
                    nowUtc,
                    "publisher-two",
                    TimeSpan.FromMinutes(5),
                    batchSize: 1));

            var claimedMessages = claimResults.SelectMany(result => result).ToArray();
            var claimed = Assert.Single(claimedMessages);

            Assert.Equal(message.Id, claimed.Id);
            Assert.True(claimed.LeaseOwner is "publisher-one" or "publisher-two");
            Assert.NotNull(claimed.LeaseUntilUtc);

            var notClaimedAgain = await outboxStore.ClaimPendingMessagesAsync(
                nowUtc,
                "publisher-three",
                TimeSpan.FromMinutes(5),
                batchSize: 1);

            Assert.Empty(notClaimedAgain);

            var persisted = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));
            Assert.Equal(message.Id, persisted.Id);
            Assert.Equal(WorkflowOutboxStatus.Pending, persisted.Status);
            Assert.True(persisted.LeaseOwner is "publisher-one" or "publisher-two");
            Assert.NotNull(persisted.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowOutboxStore_publish_and_fail_require_current_claiming_lease_owner()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        IWorkflowOutboxStore outboxStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        var nowUtc = DateTimeOffset.UtcNow;
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"outbox-completion-lease-test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var publishMessage = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.publish-fenced",
                    "workflow.events",
                    """{"state":"pending"}""",
                    CreatedAtUtc: nowUtc.AddMinutes(-10),
                    NextAttemptAtUtc: nowUtc.AddMinutes(-10)));
            var failMessage = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.fail-fenced",
                    "workflow.events",
                    """{"state":"pending"}""",
                    CreatedAtUtc: nowUtc.AddMinutes(-9),
                    NextAttemptAtUtc: nowUtc.AddMinutes(-9)));
            var messageIds = new HashSet<Guid>
            {
                publishMessage.Id,
                failMessage.Id
            };

            var firstClaim = await outboxStore.ClaimPendingMessagesAsync(
                nowUtc,
                "stale-publisher",
                TimeSpan.FromSeconds(1),
                batchSize: 50);

            Assert.Contains(firstClaim, message => message.Id == publishMessage.Id);
            Assert.Contains(firstClaim, message => message.Id == failMessage.Id);

            var secondClaim = await outboxStore.ClaimPendingMessagesAsync(
                nowUtc.AddSeconds(2),
                "current-publisher",
                TimeSpan.FromMinutes(5),
                batchSize: 50);

            Assert.Contains(secondClaim, message => message.Id == publishMessage.Id);
            Assert.Contains(secondClaim, message => message.Id == failMessage.Id);

            var stalePublished = await outboxStore.MarkMessagePublishedAsync(
                publishMessage.Id,
                "stale-publisher",
                nowUtc.AddSeconds(3));
            var staleFailed = await outboxStore.MarkMessageFailedAsync(
                failMessage.Id,
                "stale-publisher",
                "stale publisher should not win",
                nowUtc.AddSeconds(3),
                nowUtc.AddMinutes(5));

            Assert.False(stalePublished);
            Assert.False(staleFailed);

            var afterStaleCompletion = (await outboxStore.GetMessagesForWorkflowAsync(instance.Id))
                .Where(message => messageIds.Contains(message.Id))
                .ToArray();

            Assert.Collection(
                afterStaleCompletion.OrderBy(message => message.MessageType),
                message =>
                {
                    Assert.Equal(failMessage.Id, message.Id);
                    Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
                    Assert.Equal(0, message.RetryCount);
                    Assert.Null(message.ErrorMessage);
                    Assert.Null(message.PublishedAtUtc);
                    Assert.Equal("current-publisher", message.LeaseOwner);
                    Assert.NotNull(message.LeaseUntilUtc);
                },
                message =>
                {
                    Assert.Equal(publishMessage.Id, message.Id);
                    Assert.Equal(WorkflowOutboxStatus.Pending, message.Status);
                    Assert.Equal(0, message.RetryCount);
                    Assert.Null(message.ErrorMessage);
                    Assert.Null(message.PublishedAtUtc);
                    Assert.Equal("current-publisher", message.LeaseOwner);
                    Assert.NotNull(message.LeaseUntilUtc);
                });

            var currentPublished = await outboxStore.MarkMessagePublishedAsync(
                publishMessage.Id,
                "current-publisher",
                nowUtc.AddSeconds(4));
            var currentFailed = await outboxStore.MarkMessageFailedAsync(
                failMessage.Id,
                "current-publisher",
                "current publisher failure",
                nowUtc.AddSeconds(4),
                nextAttemptAtUtc: null);

            Assert.True(currentPublished);
            Assert.True(currentFailed);

            var afterCurrentCompletion = (await outboxStore.GetMessagesForWorkflowAsync(instance.Id))
                .Where(message => messageIds.Contains(message.Id))
                .ToArray();

            var published = Assert.Single(
                afterCurrentCompletion,
                message => message.Id == publishMessage.Id);
            Assert.Equal(WorkflowOutboxStatus.Published, published.Status);
            Assert.NotNull(published.PublishedAtUtc);
            Assert.Null(published.LeaseOwner);
            Assert.Null(published.LeaseUntilUtc);

            var failed = Assert.Single(
                afterCurrentCompletion,
                message => message.Id == failMessage.Id);
            Assert.Equal(WorkflowOutboxStatus.Failed, failed.Status);
            Assert.Equal(1, failed.RetryCount);
            Assert.Equal("current publisher failure", failed.ErrorMessage);
            Assert.Null(failed.LeaseOwner);
            Assert.Null(failed.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowOutboxStore_claim_skips_active_leases_and_reclaims_expired_leases()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var outboxStore = new WorkflowExecutionLogRepository(database.ConnectionString);
        var nowUtc = DateTimeOffset.UtcNow;
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"outbox-lease-test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var activeLeaseMessage = await outboxStore.SaveMessageAsync(
                new WorkflowOutboxMessageDraft(
                    instance.Id,
                    "workflow.active-lease",
                    "workflow.events",
                    """{"state":"pending"}""",
                    CreatedAtUtc: nowUtc.AddMinutes(-2),
                    NextAttemptAtUtc: nowUtc.AddMinutes(-2)));

            var activeClaim = await outboxStore.ClaimPendingMessagesAsync(
                nowUtc,
                "active-owner",
                TimeSpan.FromMinutes(5),
                batchSize: 10);

            _ = Assert.Single(
                activeClaim,
                message => message.Id == activeLeaseMessage.Id);

            var expiredLeaseMessage = new WorkflowOutboxMessageEntity
            {
                Id = Guid.NewGuid(),
                WorkflowInstanceId = instance.Id,
                MessageType = "workflow.expired-lease",
                Destination = "workflow.events",
                PayloadJson = """{"state":"pending"}""",
                Status = WorkflowOutboxMessageStatus.Pending,
                CreatedAtUtc = nowUtc.AddMinutes(-1),
                NextAttemptAtUtc = nowUtc.AddMinutes(-1),
                LeaseOwner = "expired-owner",
                LeaseUntilUtc = nowUtc.AddMinutes(-1)
            };

            await outboxStore.SaveOutboxMessageAsync(expiredLeaseMessage);

            var secondClaim = await outboxStore.ClaimPendingMessagesAsync(
                nowUtc,
                "replacement-owner",
                TimeSpan.FromMinutes(5),
                batchSize: 10);

            Assert.DoesNotContain(
                secondClaim,
                message => message.Id == activeLeaseMessage.Id);

            var reclaimed = Assert.Single(
                secondClaim,
                message => message.Id == expiredLeaseMessage.Id);
            Assert.Equal("replacement-owner", reclaimed.LeaseOwner);
            Assert.NotNull(reclaimed.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowInstanceRepository_saves_reads_and_updates_instance_state()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var repository = new WorkflowInstanceRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Pending,
            PayloadJson = """{"source":"test"}"""
        };

        try
        {
            await repository.SaveAsync(instance);

            var saved = await repository.GetByCorrelationAsync(
                instance.WorkflowId,
                instance.CorrelationId);

            Assert.NotNull(saved);
            Assert.Equal(instance.Id, saved.Id);
            Assert.Equal(WorkflowState.Pending, saved.State);
            Assert.Equal(1, saved.Version);

            var completedAtUtc = DateTimeOffset.UtcNow;
            var updated = await repository.TryUpdateStateAsync(
                instance.Id,
                WorkflowState.Succeeded,
                saved.Version,
                completedAtUtc: completedAtUtc);

            Assert.True(updated);

            var updatedInstance = await repository.GetByIdAsync(instance.Id);

            Assert.NotNull(updatedInstance);
            Assert.Equal(WorkflowState.Succeeded, updatedInstance.State);
            Assert.Equal(2, updatedInstance.Version);
            Assert.NotNull(updatedInstance.CompletedAtUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    [SqlServerFact]
    public async Task WorkflowExecutionLogRepository_saves_steps_and_outbox_messages()
    {
        await using var database = await WorkflowSqlTestDatabase.CreateAsync();
        var instanceRepository = new WorkflowInstanceRepository(database.ConnectionString);
        var logRepository = new WorkflowExecutionLogRepository(database.ConnectionString);
        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = WorkflowState.Running,
            PayloadJson = """{"source":"test"}"""
        };

        try
        {
            await instanceRepository.SaveAsync(instance);

            var step = new WorkflowStepExecutionEntity
            {
                Id = Guid.NewGuid(),
                WorkflowInstanceId = instance.Id,
                StepName = "CaptureOrderPaymentStep",
                StepOrder = 2,
                Status = WorkflowStepExecutionStatus.Succeeded,
                Attempt = 1,
                CommandId = Guid.NewGuid(),
                StartedAtUtc = DateTimeOffset.UtcNow.AddSeconds(-1),
                CompletedAtUtc = DateTimeOffset.UtcNow
            };

            await logRepository.SaveStepExecutionAsync(step);

            var steps = await logRepository.GetStepExecutionsAsync(instance.Id);
            var savedStep = Assert.Single(steps);
            Assert.Equal(step.Id, savedStep.Id);
            Assert.Equal(WorkflowStepExecutionStatus.Succeeded, savedStep.Status);

            var outboxMessage = new WorkflowOutboxMessageEntity
            {
                Id = Guid.NewGuid(),
                WorkflowInstanceId = instance.Id,
                MessageType = "workflow.completed",
                Destination = "workflow-events",
                PayloadJson = """{"status":"running"}""",
                Status = WorkflowOutboxMessageStatus.Pending,
                CreatedAtUtc = DateTimeOffset.UtcNow.AddMinutes(-1),
                NextAttemptAtUtc = DateTimeOffset.UtcNow.AddSeconds(-1)
            };

            await logRepository.SaveOutboxMessageAsync(outboxMessage);

            var pendingMessages = await logRepository.ClaimPendingOutboxMessagesAsync(
                DateTimeOffset.UtcNow,
                "log-repository-test",
                TimeSpan.FromMinutes(5),
                batchSize: 50);

            Assert.Contains(
                pendingMessages,
                message => message.Id == outboxMessage.Id);
            var claimedOutboxMessage = Assert.Single(
                pendingMessages,
                message => message.Id == outboxMessage.Id);
            Assert.Equal("log-repository-test", claimedOutboxMessage.LeaseOwner);
            Assert.NotNull(claimedOutboxMessage.LeaseUntilUtc);

            var nextAttemptAtUtc = DateTimeOffset.UtcNow.AddMinutes(5);
            var markedFailed = await logRepository.MarkOutboxMessageFailedAsync(
                outboxMessage.Id,
                "log-repository-test",
                "transient failure",
                DateTimeOffset.UtcNow,
                nextAttemptAtUtc);
            Assert.True(markedFailed);

            var failedMessages = await logRepository.GetOutboxMessagesAsync(instance.Id);
            var failedMessage = Assert.Single(failedMessages);
            Assert.Equal(WorkflowOutboxMessageStatus.Pending, failedMessage.Status);
            Assert.Equal(1, failedMessage.RetryCount);
            Assert.Equal("transient failure", failedMessage.ErrorMessage);
            Assert.Null(failedMessage.LeaseOwner);
            Assert.Null(failedMessage.LeaseUntilUtc);

            var publishClaim = await logRepository.ClaimPendingOutboxMessagesAsync(
                nextAttemptAtUtc.AddMilliseconds(1),
                "log-repository-test",
                TimeSpan.FromMinutes(5),
                batchSize: 50);
            Assert.Contains(publishClaim, message => message.Id == outboxMessage.Id);

            var markedPublished = await logRepository.MarkOutboxMessagePublishedAsync(
                outboxMessage.Id,
                "log-repository-test",
                DateTimeOffset.UtcNow);
            Assert.True(markedPublished);

            var publishedMessages = await logRepository.GetOutboxMessagesAsync(instance.Id);
            var publishedMessage = Assert.Single(publishedMessages);
            Assert.Equal(WorkflowOutboxMessageStatus.Published, publishedMessage.Status);
            Assert.NotNull(publishedMessage.PublishedAtUtc);
            Assert.Null(publishedMessage.ErrorMessage);
            Assert.Null(publishedMessage.LeaseOwner);
            Assert.Null(publishedMessage.LeaseUntilUtc);
        }
        finally
        {
            await database.DeleteWorkflowInstanceAsync(instance.Id);
        }
    }

    private sealed class SqlServerFactAttribute : FactAttribute
    {
        public SqlServerFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(WorkflowSqlTestDatabase.ConnectionStringFromEnvironment))
            {
                Skip = "Set HEMI_TEST_SQLSERVER_CONNECTION_STRING to run SQL Server persistence integration tests.";
            }
        }
    }

    private static WorkflowStartRequest CreateStartRequest(
        string correlationId,
        string idempotencyKey,
        string requestHash) =>
        new(
            WorkflowIds.OrderFulfillment,
            "Order Fulfillment",
            correlationId,
            """{"source":"sql-test"}""",
            idempotencyKey,
            requestHash,
            "sql-tests",
            DateTimeOffset.UtcNow.AddSeconds(-5),
            NextAttemptAtUtc: DateTimeOffset.UtcNow.AddSeconds(-5));

    private static WorkflowInstanceEntity CreateTerminalInstance(
        WorkflowState state) =>
        new()
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = WorkflowIds.OrderFulfillment,
            WorkflowName = "Order Fulfillment",
            CorrelationId = Guid.NewGuid().ToString("D"),
            State = state,
            PayloadJson = """{"source":"terminal-state-test"}""",
            CompletedAtUtc = DateTimeOffset.UtcNow.AddSeconds(-1)
        };

    private static WorkflowEngine CreateSqlTestEngine(
        IServiceCollection services,
        string workflowId)
    {
        var provider = services.BuildServiceProvider();
        var retryPolicyProvider = new RetryPolicyProvider(
        [
            new WorkflowPolicyRegistration(
                workflowId,
                WorkflowPolicies.NoRetry)
        ]);

        return new WorkflowEngine(
            provider,
            retryPolicyProvider);
    }

    private static async Task DispatchClaimedAsync(
        IServiceProvider services,
        WorkflowInstanceRecord instance)
    {
        using var scope = services.CreateScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IWorkflowDispatcher>();

        await dispatcher.DispatchAsync(
            instance.WorkflowId,
            CreateWorkerContext(instance));
    }

    private static WorkflowContext CreateWorkerContext(
        WorkflowInstanceRecord instance)
    {
        var context = new WorkflowContext(
            instance.WorkflowId,
            instance.CorrelationId)
        {
            WorkflowInstanceId = instance.Id,
            WorkflowInstanceVersion = instance.Version,
            WorkflowAttempt = instance.Attempt,
            WorkflowLeaseOwner = instance.LeaseOwner,
            CommandId = instance.CommandId,
            State = instance.State
        };

        if (!string.IsNullOrWhiteSpace(instance.PayloadJson))
        {
            using var document = JsonDocument.Parse(instance.PayloadJson);
            foreach (var property in document.RootElement.EnumerateObject())
            {
                context.Set(property.Name, property.Value.Clone());
            }
        }

        return context;
    }

    private static Task<HttpResponseMessage> PostFulfillmentSagaAsync(
        HttpClient client,
        Guid orderId,
        string idempotencyKey,
        PaymentMethod method = PaymentMethod.Card,
        decimal amount = 42.75m) =>
        client.PostAsJsonAsync(
            $"/orders/{orderId:D}/fulfillment-saga",
            new
            {
                method,
                amount,
                idempotencyKey,
                requestedBy = "sql-endpoint-tests"
            });

    private static async Task<Guid> ReadWorkflowInstanceIdAsync(
        HttpResponseMessage response)
    {
        using var payload = await ReadJsonAsync(response);
        return payload.RootElement.GetProperty("workflowInstanceId").GetGuid();
    }

    private static async Task<JsonDocument> ReadJsonAsync(
        HttpResponseMessage response)
    {
        await using var stream = await response.Content.ReadAsStreamAsync();
        return await JsonDocument.ParseAsync(stream);
    }

    private static async Task<HashSet<string>> GetColumnNamesAsync(
        string connectionString,
        string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string sql = """
            SELECT c.name
            FROM sys.columns c
            INNER JOIN sys.objects o ON o.object_id = c.object_id
            WHERE o.object_id = OBJECT_ID(@TableName)
              AND o.type = 'U';
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@TableName", SqlDbType.NVarChar, 256).Value =
            $"dbo.{tableName}";

        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _ = names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<HashSet<string>> GetIndexNamesAsync(
        string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string sql = """
            SELECT name
            FROM sys.indexes
            WHERE object_id IN (
                OBJECT_ID(N'dbo.WorkflowInstance'),
                OBJECT_ID(N'dbo.WorkflowStepExecution'),
                OBJECT_ID(N'dbo.WorkflowOutboxMessage'));
            """;

        await using var command = new SqlCommand(sql, connection);
        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                _ = names.Add(reader.GetString(0));
            }
        }

        return names;
    }

    private static async Task<string[]> GetIndexKeyColumnNamesAsync(
        string connectionString,
        string tableName,
        string indexName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        const string sql = """
            SELECT c.name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic
                ON ic.object_id = i.object_id
               AND ic.index_id = i.index_id
            INNER JOIN sys.columns c
                ON c.object_id = ic.object_id
               AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(@TableName)
              AND i.name = @IndexName
              AND ic.is_included_column = 0
              AND ic.key_ordinal > 0
            ORDER BY ic.key_ordinal;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@TableName", SqlDbType.NVarChar, 256).Value =
            $"dbo.{tableName}";
        _ = command.Parameters.Add("@IndexName", SqlDbType.NVarChar, 128).Value =
            indexName;

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return [.. names];
    }

    private sealed class SqlWorkflowApiFactory(
        string connectionString,
        bool useSqlFnb = false,
        bool useSqlLegacySaga = false)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IHostedService>();
                services.RemoveAll<WorkflowInstanceRepository>();
                services.RemoveAll<IWorkflowInstanceStore>();
                services.RemoveAll<WorkflowExecutionLogRepository>();
                services.RemoveAll<IWorkflowExecutionLogStore>();
                services.RemoveAll<IWorkflowOutboxStore>();
                services.RemoveAll<IWorkflowJournal>();
                services.RemoveAll<WorkflowPolicyRegistration>();
                services.RemoveAll<IRetryPolicyProvider>();

                services.AddSingleton(_ => new WorkflowInstanceRepository(connectionString));
                services.AddSingleton<IWorkflowInstanceStore>(sp =>
                    sp.GetRequiredService<WorkflowInstanceRepository>());
                services.AddSingleton(_ => new WorkflowExecutionLogRepository(connectionString));
                services.AddSingleton<IWorkflowExecutionLogStore>(sp =>
                    sp.GetRequiredService<WorkflowExecutionLogRepository>());
                services.AddSingleton<IWorkflowOutboxStore>(sp =>
                    sp.GetRequiredService<WorkflowExecutionLogRepository>());
                services.AddSingleton<IWorkflowJournal>(_ =>
                    new SqlServerWorkflowJournal(connectionString));
                services.AddSingleton(new WorkflowPolicyRegistration(
                    WorkflowIds.OrderFulfillment,
                    WorkflowPolicies.NoRetry));
                services.AddSingleton(new WorkflowPolicyRegistration(
                    WorkflowIds.OrderCancellation,
                    WorkflowPolicies.NoRetry));
                services.AddSingleton(new WorkflowPolicyRegistration(
                    WorkflowIds.InventoryReconciliation,
                    WorkflowPolicies.NoRetry));
                services.AddSingleton<IRetryPolicyProvider, RetryPolicyProvider>();

                if (useSqlFnb)
                {
                    AddSqlFnbPersistence(services, connectionString);
                }

                if (useSqlLegacySaga)
                {
                    services.RemoveAll<SqlServerSagaStateAdapter>();
                    services.RemoveAll<ISagaStateQueryPort>();
                    services.AddSingleton(_ => new SqlServerSagaStateAdapter(connectionString));
                    services.AddSingleton<ISagaStateQueryPort>(sp =>
                        sp.GetRequiredService<SqlServerSagaStateAdapter>());
                }
            });
        }

        private static void AddSqlFnbPersistence(
            IServiceCollection services,
            string connectionString)
        {
            services.RemoveAll<SqlServerFnbRepository>();
            services.RemoveAll<IRestaurantQueryPort>();
            services.RemoveAll<ITableQueryPort>();
            services.RemoveAll<IMenuQueryPort>();
            services.RemoveAll<IOrderQueryPort>();
            services.RemoveAll<IOrderCommandPort>();
            services.RemoveAll<IReservationQueryPort>();
            services.RemoveAll<IReservationCommandPort>();
            services.RemoveAll<IPaymentQueryPort>();
            services.RemoveAll<IPaymentCommandPort>();
            services.RemoveAll<IInventoryQueryPort>();
            services.RemoveAll<IInventoryCommandPort>();

            services.AddSingleton(_ => new SqlServerFnbRepository(connectionString));
            services.AddSingleton<IRestaurantQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<ITableQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IMenuQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IOrderQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IOrderCommandPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IReservationQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IReservationCommandPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IPaymentQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IPaymentCommandPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IInventoryQueryPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
            services.AddSingleton<IInventoryCommandPort>(sp =>
                sp.GetRequiredService<SqlServerFnbRepository>());
        }
    }

    private sealed class SqlFirstPayloadStep : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            context.Set("stepOne", true);
            return Task.CompletedTask;
        }
    }

    private sealed class SqlSecondPayloadStep : IWorkflowStep<WorkflowContext>
    {
        public Task ExecuteAsync(
            WorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            context.Set("stepTwo", true);
            return Task.CompletedTask;
        }
    }

    private sealed class WorkflowSqlTestDatabase : IAsyncDisposable
    {
        private WorkflowSqlTestDatabase(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public static string? ConnectionStringFromEnvironment =>
            Environment.GetEnvironmentVariable("HEMI_TEST_SQLSERVER_CONNECTION_STRING");

        public string ConnectionString { get; }

        public static async Task<WorkflowSqlTestDatabase> CreateAsync()
        {
            var connectionString = ConnectionStringFromEnvironment
                ?? throw new InvalidOperationException(
                    "HEMI_TEST_SQLSERVER_CONNECTION_STRING is required.");

            var database = new WorkflowSqlTestDatabase(connectionString);
            await database.ApplySchemaAsync();
            return database;
        }

        public async Task DeleteWorkflowInstanceAsync(Guid workflowInstanceId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var sql in new[]
            {
                "DELETE FROM dbo.WorkflowOutboxMessage WHERE WorkflowInstanceId = @WorkflowInstanceId;",
                "DELETE FROM dbo.WorkflowStepExecution WHERE WorkflowInstanceId = @WorkflowInstanceId;",
                "DELETE FROM dbo.WorkflowInstance WHERE Id = @WorkflowInstanceId;"
            })
            {
                await using var command = new SqlCommand(sql, connection);
                _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
                    workflowInstanceId;
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        public async Task DeleteFnbOrderAsync(Guid orderId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var sql in new[]
            {
                "DELETE FROM dbo.FnbStockMovement WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbPayment WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbOrderLine WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbServiceOrder WHERE Id = @OrderId;"
            })
            {
                await using var command = new SqlCommand(sql, connection);
                _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value =
                    orderId;
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        public async Task DeleteLegacySagaAsync(Guid sagaId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var sql in new[]
            {
                "DELETE FROM dbo.OutboxMessage WHERE SagaInstanceId = @SagaId;",
                "DELETE FROM dbo.SagaStep WHERE SagaInstanceId = @SagaId;",
                "DELETE FROM dbo.SagaInstance WHERE Id = @SagaId;"
            })
            {
                await using var command = new SqlCommand(sql, connection);
                _ = command.Parameters.Add("@SagaId", SqlDbType.UniqueIdentifier).Value =
                    sagaId;
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task ApplySchemaAsync() =>
            ApplySqlScriptAsync(
                "src",
                "Infrastructure",
                "WorkflowPersistence",
                "Sql",
                "WorkflowTables.sql");

        public Task ApplyFnbSchemaAsync() =>
            ApplySqlScriptAsync(
                "src",
                "Infrastructure",
                "FnbPersistence",
                "Sql",
                "FnbTables.sql");

        public Task ApplyLegacySagaSchemaAsync() =>
            ApplySqlScriptAsync(
                "src",
                "Infrastructure",
                "Persistence",
                "SagaCoreTables.sql");

        public async Task<decimal> GetFnbInventoryQuantityAsync(
            Guid inventoryItemId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                SELECT StockQuantity
                FROM dbo.FnbInventoryItem
                WHERE Id = @InventoryItemId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
                inventoryItemId;

            var result = await command.ExecuteScalarAsync()
                ?? throw new InvalidOperationException("Inventory item not found.");

            return (decimal)result;
        }

        public async Task SetFnbInventoryQuantityAsync(
            Guid inventoryItemId,
            decimal stockQuantity)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                UPDATE dbo.FnbInventoryItem
                SET StockQuantity = @StockQuantity
                WHERE Id = @InventoryItemId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
                inventoryItemId;
            var stockParameter = command.Parameters.Add("@StockQuantity", SqlDbType.Decimal);
            stockParameter.Precision = 18;
            stockParameter.Scale = 3;
            stockParameter.Value = stockQuantity;
            _ = await command.ExecuteNonQueryAsync();
        }

        public async Task SetFnbTableStatusAsync(
            Guid tableId,
            TableStatus status)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                UPDATE dbo.FnbDiningTable
                SET Status = @Status
                WHERE Id = @TableId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value =
                tableId;
            _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                status.ToString();
            _ = await command.ExecuteNonQueryAsync();
        }

        public async Task<Guid> InsertLegacySagaAsync(Guid orderId)
        {
            var sagaId = Guid.NewGuid();
            var now = DateTimeOffset.UtcNow.AddMinutes(-5);
            var payloadJson = JsonSerializer.Serialize(
                new SagaMessageEnvelope(
                    orderId,
                    sagaId,
                    OrderFulfillmentSagaStatus.Completed,
                    SagaStepStatus.Completed,
                    SagaStepStatus.Completed,
                    SagaStepStatus.Completed,
                    SagaStepStatus.Completed,
                    LastError: null,
                    UpdatedAt: now,
                    StartedAt: now));

            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                INSERT dbo.SagaInstance (
                    Id, SagaType, CorrelationId, CurrentStep, Status,
                    PayloadJson, Version, CreatedAt, UpdatedAt)
                VALUES (
                    @Id, @SagaType, @CorrelationId, @CurrentStep, @Status,
                    @PayloadJson, @Version, @CreatedAt, @UpdatedAt);
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = sagaId;
            _ = command.Parameters.Add("@SagaType", SqlDbType.NVarChar, 128).Value =
                "OrderFulfillmentSaga";
            _ = command.Parameters.Add("@CorrelationId", SqlDbType.UniqueIdentifier).Value =
                orderId;
            _ = command.Parameters.Add("@CurrentStep", SqlDbType.NVarChar, 128).Value =
                "Done";
            _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                SagaInstanceStatus.Completed.ToString();
            _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value =
                payloadJson;
            _ = command.Parameters.Add("@Version", SqlDbType.Int).Value = 1;
            _ = command.Parameters.Add("@CreatedAt", SqlDbType.DateTimeOffset).Value =
                now;
            _ = command.Parameters.Add("@UpdatedAt", SqlDbType.DateTimeOffset).Value =
                now;
            _ = await command.ExecuteNonQueryAsync();

            return sagaId;
        }

        private async Task ApplySqlScriptAsync(params string[] relativePathParts)
        {
            var pathParts = new string[relativePathParts.Length + 1];
            pathParts[0] = FindRepositoryRoot();
            Array.Copy(
                relativePathParts,
                sourceIndex: 0,
                pathParts,
                destinationIndex: 1,
                relativePathParts.Length);

            var schemaPath = Path.Combine(pathParts);
            var schemaSql = await File.ReadAllTextAsync(schemaPath);

            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var batch in SplitSqlBatches(schemaSql))
            {
                await using var command = new SqlCommand(batch, connection);
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        private static IEnumerable<string> SplitSqlBatches(string sql)
        {
            var batch = new StringBuilder();

            foreach (var line in sql.Replace("\r\n", "\n").Split('\n'))
            {
                if (string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                {
                    var batchText = batch.ToString().Trim();
                    if (!string.IsNullOrWhiteSpace(batchText))
                    {
                        yield return batchText;
                    }

                    batch.Clear();
                    continue;
                }

                batch.AppendLine(line);
            }

            var finalBatch = batch.ToString().Trim();
            if (!string.IsNullOrWhiteSpace(finalBatch))
            {
                yield return finalBatch;
            }
        }

        private static string FindRepositoryRoot()
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);

            while (directory is not null &&
                   !File.Exists(Path.Combine(directory.FullName, "Hemi.sln")))
            {
                directory = directory.Parent;
            }

            return directory?.FullName
                ?? throw new DirectoryNotFoundException(
                    "Could not find repository root containing Hemi.sln.");
        }
    }
}
