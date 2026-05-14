using System.Data;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Definitions.OrderFulfillment;
using Hemi.Application.Workflows.Execution;
using Hemi.Application.Workflows.Registry;
using Hemi.Domain;
using Hemi.Domain.Workflows;
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

        var indexes = await GetIndexNamesAsync(database.ConnectionString);
        Assert.Contains("UX_WorkflowInstance_Workflow_Correlation", indexes);
        Assert.Contains("UX_WorkflowInstance_IdempotencyKey", indexes);
        Assert.Contains("IX_WorkflowInstance_State_NextAttempt_Lease", indexes);
        Assert.Contains("UX_WorkflowStepExecution_Instance_Order_Attempt", indexes);
        Assert.Contains("IX_WorkflowOutboxMessage_Status_NextAttempt_CreatedAt", indexes);
        Assert.Contains("IX_WorkflowOutboxMessage_WorkflowInstanceId", indexes);
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
            Attempt = 1
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
            Assert.Equal(4, persisted.Version);
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
            Attempt = 1
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
            _ = await journal.AppendAsync(
                new WorkflowJournalEntry(
                    instance.Id,
                    saved.Version,
                    PayloadJson: """{"source":"journal-test","completed":true}""",
                    State: new WorkflowStateJournalEntry(
                        WorkflowState.Succeeded,
                        CompletedAtUtc: completedAtUtc),
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
                    }));

            var persisted = await instanceRepository.GetByIdAsync(instance.Id);
            Assert.NotNull(persisted);
            Assert.Equal(WorkflowState.Succeeded, persisted.State);
            Assert.Contains(@"""completed"":true", persisted.PayloadJson, StringComparison.Ordinal);
            Assert.Equal(saved.Version + 2, persisted.Version);
            Assert.NotNull(persisted.CompletedAtUtc);

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
            Attempt = 1
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
                journal.AppendAsync(
                    new WorkflowJournalEntry(
                        instance.Id,
                        saved.Version + 100,
                        State: new WorkflowStateJournalEntry(
                            WorkflowState.Succeeded,
                            CompletedAtUtc: completedAtUtc),
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
                        })));

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

            var pending = await outboxStore.GetPendingMessagesAsync(
                batchSize: 10,
                dueAtUtc: DateTimeOffset.UtcNow);

            Assert.Contains(pending, pendingMessage => pendingMessage.Id == message.Id);

            await outboxStore.MarkMessageFailedAsync(
                message.Id,
                "transport unavailable",
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddMinutes(1));

            var retried = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Pending, retried.Status);
            Assert.Equal(1, retried.RetryCount);
            Assert.Equal("transport unavailable", retried.ErrorMessage);
            Assert.NotNull(retried.LastAttemptAtUtc);
            Assert.NotNull(retried.NextAttemptAtUtc);

            await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                DateTimeOffset.UtcNow);

            var published = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Published, published.Status);
            Assert.Equal(1, published.RetryCount);
            Assert.Null(published.ErrorMessage);
            Assert.Null(published.NextAttemptAtUtc);
            Assert.NotNull(published.PublishedAtUtc);
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

            var pending = await outboxStore.GetPendingMessagesAsync(
                dueAtUtc: DateTimeOffset.UtcNow);

            Assert.Contains(pending, pendingMessage => pendingMessage.Id == message.Id);

            await outboxStore.MarkMessageFailedAsync(
                message.Id,
                "transient failure",
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddMinutes(5));

            var afterFailure = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Pending, afterFailure.Status);
            Assert.Equal(1, afterFailure.RetryCount);

            await outboxStore.MarkMessagePublishedAsync(
                message.Id,
                DateTimeOffset.UtcNow);

            var afterPublish = Assert.Single(
                await outboxStore.GetMessagesForWorkflowAsync(instance.Id));

            Assert.Equal(WorkflowOutboxStatus.Published, afterPublish.Status);
            Assert.NotNull(afterPublish.PublishedAtUtc);
            Assert.Null(afterPublish.ErrorMessage);
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

            var pendingMessages = await logRepository.GetPendingOutboxMessagesAsync(
                dueAtUtc: DateTimeOffset.UtcNow);

            Assert.Contains(
                pendingMessages,
                message => message.Id == outboxMessage.Id);

            await logRepository.MarkOutboxMessageFailedAsync(
                outboxMessage.Id,
                "transient failure",
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddMinutes(5));

            var failedMessages = await logRepository.GetOutboxMessagesAsync(instance.Id);
            var failedMessage = Assert.Single(failedMessages);
            Assert.Equal(WorkflowOutboxMessageStatus.Pending, failedMessage.Status);
            Assert.Equal(1, failedMessage.RetryCount);
            Assert.Equal("transient failure", failedMessage.ErrorMessage);

            await logRepository.MarkOutboxMessagePublishedAsync(
                outboxMessage.Id,
                DateTimeOffset.UtcNow);

            var publishedMessages = await logRepository.GetOutboxMessagesAsync(instance.Id);
            var publishedMessage = Assert.Single(publishedMessages);
            Assert.Equal(WorkflowOutboxMessageStatus.Published, publishedMessage.Status);
            Assert.NotNull(publishedMessage.PublishedAtUtc);
            Assert.Null(publishedMessage.ErrorMessage);
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

    private sealed class SqlWorkflowApiFactory(string connectionString)
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
            });
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

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public async Task ApplySchemaAsync()
        {
            var schemaPath = Path.Combine(
                FindRepositoryRoot(),
                "src",
                "Infrastructure",
                "WorkflowPersistence",
                "Sql",
                "WorkflowTables.sql");

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

            foreach (var line in sql.Split(Environment.NewLine))
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
