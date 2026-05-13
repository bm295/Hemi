using System.Data;
using System.Text;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Hemi.Infrastructure.WorkflowPersistence.Repositories;
using Microsoft.Data.SqlClient;

namespace Hemi.Tests.Infrastructure.WorkflowPersistence;

public sealed class SqlServerWorkflowPersistenceTests
{
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

        private async Task ApplySchemaAsync()
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
