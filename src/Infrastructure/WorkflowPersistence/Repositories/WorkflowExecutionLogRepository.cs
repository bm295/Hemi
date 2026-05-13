using System.Data;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure.WorkflowPersistence.Repositories;

public sealed class WorkflowExecutionLogRepository(string connectionString)
{
    public async Task<IReadOnlyCollection<WorkflowStepExecutionEntity>> GetStepExecutionsAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, WorkflowInstanceId, StepName, StepOrder, Status, Attempt,
                   CommandId, ErrorMessage, StartedAtUtc, CompletedAtUtc, CompensatedAtUtc
            FROM dbo.WorkflowStepExecution
            WHERE WorkflowInstanceId = @WorkflowInstanceId
            ORDER BY StepOrder, Attempt;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;

        var result = new List<WorkflowStepExecutionEntity>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapStepExecution(reader));
        }

        return result;
    }

    public async Task SaveStepExecutionAsync(
        WorkflowStepExecutionEntity entity,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entity);
        Validate(entity);

        if (entity.Id == Guid.Empty)
        {
            entity.Id = Guid.NewGuid();
        }

        if (entity.Attempt <= 0)
        {
            entity.Attempt = 1;
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            MERGE dbo.WorkflowStepExecution AS Target
            USING (SELECT @Id AS Id) AS Source
            ON Target.Id = Source.Id
            WHEN MATCHED THEN
                UPDATE SET
                    WorkflowInstanceId = @WorkflowInstanceId,
                    StepName = @StepName,
                    StepOrder = @StepOrder,
                    Status = @Status,
                    Attempt = @Attempt,
                    CommandId = @CommandId,
                    ErrorMessage = @ErrorMessage,
                    StartedAtUtc = @StartedAtUtc,
                    CompletedAtUtc = @CompletedAtUtc,
                    CompensatedAtUtc = @CompensatedAtUtc
            WHEN NOT MATCHED THEN
                INSERT (Id, WorkflowInstanceId, StepName, StepOrder, Status, Attempt,
                        CommandId, ErrorMessage, StartedAtUtc, CompletedAtUtc, CompensatedAtUtc)
                VALUES (@Id, @WorkflowInstanceId, @StepName, @StepOrder, @Status, @Attempt,
                        @CommandId, @ErrorMessage, @StartedAtUtc, @CompletedAtUtc, @CompensatedAtUtc);
            """;

        await using var command = new SqlCommand(sql, connection);
        AddStepExecutionParameters(command, entity);
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyCollection<WorkflowOutboxMessageEntity>> GetOutboxMessagesAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, WorkflowInstanceId, MessageType, Destination, PayloadJson, Status,
                   RetryCount, ErrorMessage, CreatedAtUtc, LastAttemptAtUtc,
                   NextAttemptAtUtc, PublishedAtUtc
            FROM dbo.WorkflowOutboxMessage
            WHERE WorkflowInstanceId = @WorkflowInstanceId
            ORDER BY CreatedAtUtc;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;

        var result = new List<WorkflowOutboxMessageEntity>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapOutboxMessage(reader));
        }

        return result;
    }

    public async Task<IReadOnlyCollection<WorkflowOutboxMessageEntity>> GetPendingOutboxMessagesAsync(
        int batchSize = 50,
        DateTimeOffset? dueAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                "Batch size must be greater than zero.");
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT TOP (@BatchSize)
                   Id, WorkflowInstanceId, MessageType, Destination, PayloadJson, Status,
                   RetryCount, ErrorMessage, CreatedAtUtc, LastAttemptAtUtc,
                   NextAttemptAtUtc, PublishedAtUtc
            FROM dbo.WorkflowOutboxMessage
            WHERE Status = @Status
              AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= @DueAtUtc)
            ORDER BY CreatedAtUtc;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            WorkflowOutboxMessageStatus.Pending.ToString();
        _ = command.Parameters.Add("@DueAtUtc", SqlDbType.DateTimeOffset).Value =
            dueAtUtc ?? DateTimeOffset.UtcNow;

        var result = new List<WorkflowOutboxMessageEntity>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapOutboxMessage(reader));
        }

        return result;
    }

    public async Task SaveOutboxMessageAsync(
        WorkflowOutboxMessageEntity entity,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entity);
        Validate(entity);

        if (entity.Id == Guid.Empty)
        {
            entity.Id = Guid.NewGuid();
        }

        if (entity.CreatedAtUtc == default)
        {
            entity.CreatedAtUtc = DateTimeOffset.UtcNow;
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            MERGE dbo.WorkflowOutboxMessage AS Target
            USING (SELECT @Id AS Id) AS Source
            ON Target.Id = Source.Id
            WHEN MATCHED THEN
                UPDATE SET
                    WorkflowInstanceId = @WorkflowInstanceId,
                    MessageType = @MessageType,
                    Destination = @Destination,
                    PayloadJson = @PayloadJson,
                    Status = @Status,
                    RetryCount = @RetryCount,
                    ErrorMessage = @ErrorMessage,
                    LastAttemptAtUtc = @LastAttemptAtUtc,
                    NextAttemptAtUtc = @NextAttemptAtUtc,
                    PublishedAtUtc = @PublishedAtUtc
            WHEN NOT MATCHED THEN
                INSERT (Id, WorkflowInstanceId, MessageType, Destination, PayloadJson, Status,
                        RetryCount, ErrorMessage, CreatedAtUtc, LastAttemptAtUtc,
                        NextAttemptAtUtc, PublishedAtUtc)
                VALUES (@Id, @WorkflowInstanceId, @MessageType, @Destination, @PayloadJson, @Status,
                        @RetryCount, @ErrorMessage, @CreatedAtUtc, @LastAttemptAtUtc,
                        @NextAttemptAtUtc, @PublishedAtUtc);
            """;

        await using var command = new SqlCommand(sql, connection);
        AddOutboxParameters(command, entity);
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkOutboxMessagePublishedAsync(
        Guid messageId,
        DateTimeOffset publishedAtUtc,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.WorkflowOutboxMessage
            SET Status = @Status,
                ErrorMessage = NULL,
                LastAttemptAtUtc = @PublishedAtUtc,
                NextAttemptAtUtc = NULL,
                PublishedAtUtc = @PublishedAtUtc
            WHERE Id = @Id;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = messageId;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            WorkflowOutboxMessageStatus.Published.ToString();
        _ = command.Parameters.Add("@PublishedAtUtc", SqlDbType.DateTimeOffset).Value =
            publishedAtUtc;

        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkOutboxMessageFailedAsync(
        Guid messageId,
        string errorMessage,
        DateTimeOffset lastAttemptAtUtc,
        DateTimeOffset? nextAttemptAtUtc,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.WorkflowOutboxMessage
            SET Status = @Status,
                RetryCount = RetryCount + 1,
                ErrorMessage = @ErrorMessage,
                LastAttemptAtUtc = @LastAttemptAtUtc,
                NextAttemptAtUtc = @NextAttemptAtUtc
            WHERE Id = @Id;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = messageId;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            nextAttemptAtUtc.HasValue
                ? WorkflowOutboxMessageStatus.Pending.ToString()
                : WorkflowOutboxMessageStatus.Failed.ToString();
        AddNullable(command, "@ErrorMessage", SqlDbType.NVarChar, 1024, errorMessage);
        _ = command.Parameters.Add("@LastAttemptAtUtc", SqlDbType.DateTimeOffset).Value =
            lastAttemptAtUtc;
        AddNullable(command, "@NextAttemptAtUtc", SqlDbType.DateTimeOffset, nextAttemptAtUtc);

        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void Validate(WorkflowStepExecutionEntity entity)
    {
        if (entity.WorkflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(entity));
        }

        if (string.IsNullOrWhiteSpace(entity.StepName))
        {
            throw new ArgumentException(
                "Step name is required.",
                nameof(entity));
        }

        if (entity.StepOrder <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entity),
                "Step order must be greater than zero.");
        }
    }

    private static void Validate(WorkflowOutboxMessageEntity entity)
    {
        if (entity.WorkflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(entity));
        }

        if (string.IsNullOrWhiteSpace(entity.MessageType))
        {
            throw new ArgumentException(
                "Message type is required.",
                nameof(entity));
        }

        if (string.IsNullOrWhiteSpace(entity.Destination))
        {
            throw new ArgumentException(
                "Destination is required.",
                nameof(entity));
        }
    }

    private static void AddStepExecutionParameters(
        SqlCommand command,
        WorkflowStepExecutionEntity entity)
    {
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = entity.Id;
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            entity.WorkflowInstanceId;
        _ = command.Parameters.Add("@StepName", SqlDbType.NVarChar, 128).Value = entity.StepName;
        _ = command.Parameters.Add("@StepOrder", SqlDbType.Int).Value = entity.StepOrder;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value = entity.Status.ToString();
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value = entity.Attempt;
        AddNullable(command, "@CommandId", SqlDbType.UniqueIdentifier, entity.CommandId);
        AddNullable(command, "@ErrorMessage", SqlDbType.NVarChar, 1024, entity.ErrorMessage);
        AddNullable(command, "@StartedAtUtc", SqlDbType.DateTimeOffset, entity.StartedAtUtc);
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, entity.CompletedAtUtc);
        AddNullable(command, "@CompensatedAtUtc", SqlDbType.DateTimeOffset, entity.CompensatedAtUtc);
    }

    private static void AddOutboxParameters(
        SqlCommand command,
        WorkflowOutboxMessageEntity entity)
    {
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = entity.Id;
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            entity.WorkflowInstanceId;
        _ = command.Parameters.Add("@MessageType", SqlDbType.NVarChar, 128).Value = entity.MessageType;
        _ = command.Parameters.Add("@Destination", SqlDbType.NVarChar, 256).Value = entity.Destination;
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value = entity.PayloadJson;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value = entity.Status.ToString();
        _ = command.Parameters.Add("@RetryCount", SqlDbType.Int).Value = entity.RetryCount;
        AddNullable(command, "@ErrorMessage", SqlDbType.NVarChar, 1024, entity.ErrorMessage);
        _ = command.Parameters.Add("@CreatedAtUtc", SqlDbType.DateTimeOffset).Value = entity.CreatedAtUtc;
        AddNullable(command, "@LastAttemptAtUtc", SqlDbType.DateTimeOffset, entity.LastAttemptAtUtc);
        AddNullable(command, "@NextAttemptAtUtc", SqlDbType.DateTimeOffset, entity.NextAttemptAtUtc);
        AddNullable(command, "@PublishedAtUtc", SqlDbType.DateTimeOffset, entity.PublishedAtUtc);
    }

    private static WorkflowStepExecutionEntity MapStepExecution(SqlDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(0),
            WorkflowInstanceId = reader.GetGuid(1),
            StepName = reader.GetString(2),
            StepOrder = reader.GetInt32(3),
            Status = ParseStepExecutionStatus(reader.GetString(4)),
            Attempt = reader.GetInt32(5),
            CommandId = reader.IsDBNull(6) ? null : reader.GetGuid(6),
            ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
            StartedAtUtc = reader.IsDBNull(8) ? null : reader.GetDateTimeOffset(8),
            CompletedAtUtc = reader.IsDBNull(9) ? null : reader.GetDateTimeOffset(9),
            CompensatedAtUtc = reader.IsDBNull(10) ? null : reader.GetDateTimeOffset(10)
        };

    private static WorkflowOutboxMessageEntity MapOutboxMessage(SqlDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(0),
            WorkflowInstanceId = reader.GetGuid(1),
            MessageType = reader.GetString(2),
            Destination = reader.GetString(3),
            PayloadJson = reader.GetString(4),
            Status = ParseOutboxStatus(reader.GetString(5)),
            RetryCount = reader.GetInt32(6),
            ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
            CreatedAtUtc = reader.GetDateTimeOffset(8),
            LastAttemptAtUtc = reader.IsDBNull(9) ? null : reader.GetDateTimeOffset(9),
            NextAttemptAtUtc = reader.IsDBNull(10) ? null : reader.GetDateTimeOffset(10),
            PublishedAtUtc = reader.IsDBNull(11) ? null : reader.GetDateTimeOffset(11)
        };

    private static WorkflowStepExecutionStatus ParseStepExecutionStatus(string raw) =>
        Enum.TryParse<WorkflowStepExecutionStatus>(raw, out var status)
            ? status
            : WorkflowStepExecutionStatus.Pending;

    private static WorkflowOutboxMessageStatus ParseOutboxStatus(string raw) =>
        Enum.TryParse<WorkflowOutboxMessageStatus>(raw, out var status)
            ? status
            : WorkflowOutboxMessageStatus.Pending;

    private static void AddNullable(
        SqlCommand command,
        string name,
        SqlDbType type,
        object? value)
    {
        _ = command.Parameters.Add(name, type).Value =
            value ?? DBNull.Value;
    }

    private static void AddNullable(
        SqlCommand command,
        string name,
        SqlDbType type,
        int size,
        string? value)
    {
        _ = command.Parameters.Add(name, type, size).Value =
            string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
    }
}
