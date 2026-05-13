using System.Data;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure.WorkflowPersistence.Repositories;

public sealed class WorkflowExecutionLogRepository(string connectionString)
    : IWorkflowExecutionLogStore, IWorkflowOutboxStore
{
    private const string StepColumns = """
        Id, WorkflowInstanceId, StepName, StepOrder, Status, Attempt,
        CommandId, ErrorMessage, StartedAtUtc, CompletedAtUtc, CompensatedAtUtc
        """;

    private const string OutboxColumns = """
        Id, WorkflowInstanceId, MessageType, Destination, PayloadJson,
        HeadersJson, Status, RetryCount, ErrorMessage, CreatedAtUtc,
        LastAttemptAtUtc, NextAttemptAtUtc, PublishedAtUtc
        """;

    public async Task<IReadOnlyCollection<WorkflowStepAttemptRecord>> GetStepAttemptsAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default)
    {
        var stepExecutions = await GetStepExecutionsAsync(
            workflowInstanceId,
            cancellationToken);

        return stepExecutions
            .Select(MapAttemptRecord)
            .ToArray();
    }

    public async Task<WorkflowStepAttemptRecord> MarkStepRunningAsync(
        WorkflowStepAttemptStart request,
        CancellationToken cancellationToken = default)
    {
        Validate(request);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            MERGE dbo.WorkflowStepExecution AS Target
            USING (
                SELECT @WorkflowInstanceId AS WorkflowInstanceId,
                       @StepOrder AS StepOrder,
                       @Attempt AS Attempt
            ) AS Source
            ON Target.WorkflowInstanceId = Source.WorkflowInstanceId
               AND Target.StepOrder = Source.StepOrder
               AND Target.Attempt = Source.Attempt
            WHEN MATCHED THEN
                UPDATE SET
                    StepName = @StepName,
                    Status = @Status,
                    CommandId = @CommandId,
                    ErrorMessage = NULL,
                    StartedAtUtc = @StartedAtUtc,
                    CompletedAtUtc = NULL,
                    CompensatedAtUtc = NULL
            WHEN NOT MATCHED THEN
                INSERT (Id, WorkflowInstanceId, StepName, StepOrder, Status,
                        Attempt, CommandId, ErrorMessage, StartedAtUtc,
                        CompletedAtUtc, CompensatedAtUtc)
                VALUES (@Id, @WorkflowInstanceId, @StepName, @StepOrder,
                        @Status, @Attempt, @CommandId, NULL, @StartedAtUtc,
                        NULL, NULL)
            OUTPUT inserted.Id, inserted.WorkflowInstanceId, inserted.StepName,
                   inserted.StepOrder, inserted.Status, inserted.Attempt,
                   inserted.CommandId, inserted.ErrorMessage,
                   inserted.StartedAtUtc, inserted.CompletedAtUtc,
                   inserted.CompensatedAtUtc;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = Guid.NewGuid();
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            request.WorkflowInstanceId;
        _ = command.Parameters.Add("@StepName", SqlDbType.NVarChar, 128).Value = request.StepName;
        _ = command.Parameters.Add("@StepOrder", SqlDbType.Int).Value = request.StepOrder;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            WorkflowStepExecutionStatus.Running.ToString();
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value = request.Attempt;
        AddNullable(command, "@CommandId", SqlDbType.UniqueIdentifier, request.CommandId);
        _ = command.Parameters.Add("@StartedAtUtc", SqlDbType.DateTimeOffset).Value =
            request.StartedAtUtc ?? DateTimeOffset.UtcNow;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException(
                "Step attempt update did not return a row.");
        }

        return MapAttemptRecord(reader);
    }

    public async Task<bool> MarkStepSucceededAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        DateTimeOffset completedAtUtc,
        CancellationToken cancellationToken = default)
    {
        return await MarkStepTerminalAsync(
            workflowInstanceId,
            stepOrder,
            attempt,
            WorkflowStepExecutionStatus.Succeeded,
            errorMessage: null,
            completedAtUtc,
            compensatedAtUtc: null,
            cancellationToken);
    }

    public async Task<bool> MarkStepFailedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        string errorMessage,
        DateTimeOffset completedAtUtc,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(errorMessage))
        {
            throw new ArgumentException(
                "Error message is required.",
                nameof(errorMessage));
        }

        return await MarkStepTerminalAsync(
            workflowInstanceId,
            stepOrder,
            attempt,
            WorkflowStepExecutionStatus.Failed,
            errorMessage,
            completedAtUtc,
            compensatedAtUtc: null,
            cancellationToken);
    }

    public async Task<bool> MarkStepCompensatedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        DateTimeOffset compensatedAtUtc,
        CancellationToken cancellationToken = default)
    {
        return await MarkStepTerminalAsync(
            workflowInstanceId,
            stepOrder,
            attempt,
            WorkflowStepExecutionStatus.Compensated,
            errorMessage: null,
            completedAtUtc: null,
            compensatedAtUtc,
            cancellationToken);
    }

    public async Task<bool> MarkStepCompensationFailedAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        string errorMessage,
        DateTimeOffset compensatedAtUtc,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(errorMessage))
        {
            throw new ArgumentException(
                "Error message is required.",
                nameof(errorMessage));
        }

        return await MarkStepTerminalAsync(
            workflowInstanceId,
            stepOrder,
            attempt,
            WorkflowStepExecutionStatus.CompensationFailed,
            errorMessage,
            completedAtUtc: null,
            compensatedAtUtc,
            cancellationToken);
    }

    public async Task<WorkflowOutboxMessageRecord> SaveMessageAsync(
        WorkflowOutboxMessageDraft message,
        CancellationToken cancellationToken = default)
    {
        Validate(message);

        var entity = new WorkflowOutboxMessageEntity
        {
            Id = Guid.NewGuid(),
            WorkflowInstanceId = message.WorkflowInstanceId,
            MessageType = message.MessageType,
            Destination = message.Destination,
            PayloadJson = message.PayloadJson,
            HeadersJson = string.IsNullOrWhiteSpace(message.HeadersJson)
                ? "{}"
                : message.HeadersJson,
            Status = WorkflowOutboxMessageStatus.Pending,
            RetryCount = 0,
            CreatedAtUtc = message.CreatedAtUtc ?? DateTimeOffset.UtcNow,
            NextAttemptAtUtc = message.NextAttemptAtUtc
        };

        await SaveOutboxMessageAsync(entity, cancellationToken);
        return MapOutboxRecord(entity);
    }

    public async Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> GetMessagesForWorkflowAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default)
    {
        var messages = await GetOutboxMessagesAsync(
            workflowInstanceId,
            cancellationToken);

        return messages
            .Select(MapOutboxRecord)
            .ToArray();
    }

    public async Task<IReadOnlyCollection<WorkflowOutboxMessageRecord>> GetPendingMessagesAsync(
        int batchSize = 50,
        DateTimeOffset? dueAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        var messages = await GetPendingOutboxMessagesAsync(
            batchSize,
            dueAtUtc,
            cancellationToken);

        return messages
            .Select(MapOutboxRecord)
            .ToArray();
    }

    public Task MarkMessagePublishedAsync(
        Guid messageId,
        DateTimeOffset publishedAtUtc,
        CancellationToken cancellationToken = default) =>
        MarkOutboxMessagePublishedAsync(messageId, publishedAtUtc, cancellationToken);

    public Task MarkMessageFailedAsync(
        Guid messageId,
        string errorMessage,
        DateTimeOffset lastAttemptAtUtc,
        DateTimeOffset? nextAttemptAtUtc,
        CancellationToken cancellationToken = default) =>
        MarkOutboxMessageFailedAsync(
            messageId,
            errorMessage,
            lastAttemptAtUtc,
            nextAttemptAtUtc,
            cancellationToken);

    public async Task<IReadOnlyCollection<WorkflowStepExecutionEntity>> GetStepExecutionsAsync(
        Guid workflowInstanceId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            SELECT {StepColumns}
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

        var sql = $"""
            SELECT {OutboxColumns}
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

        var sql = $"""
            SELECT TOP (@BatchSize) {OutboxColumns}
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

        if (string.IsNullOrWhiteSpace(entity.HeadersJson))
        {
            entity.HeadersJson = "{}";
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
                    HeadersJson = @HeadersJson,
                    Status = @Status,
                    RetryCount = @RetryCount,
                    ErrorMessage = @ErrorMessage,
                    LastAttemptAtUtc = @LastAttemptAtUtc,
                    NextAttemptAtUtc = @NextAttemptAtUtc,
                    PublishedAtUtc = @PublishedAtUtc
            WHEN NOT MATCHED THEN
                INSERT (Id, WorkflowInstanceId, MessageType, Destination,
                        PayloadJson, HeadersJson, Status, RetryCount,
                        ErrorMessage, CreatedAtUtc, LastAttemptAtUtc,
                        NextAttemptAtUtc, PublishedAtUtc)
                VALUES (@Id, @WorkflowInstanceId, @MessageType, @Destination,
                        @PayloadJson, @HeadersJson, @Status, @RetryCount,
                        @ErrorMessage, @CreatedAtUtc, @LastAttemptAtUtc,
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
        if (string.IsNullOrWhiteSpace(errorMessage))
        {
            throw new ArgumentException(
                "Error message is required.",
                nameof(errorMessage));
        }

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

    private async Task<bool> MarkStepTerminalAsync(
        Guid workflowInstanceId,
        int stepOrder,
        int attempt,
        WorkflowStepExecutionStatus status,
        string? errorMessage,
        DateTimeOffset? completedAtUtc,
        DateTimeOffset? compensatedAtUtc,
        CancellationToken cancellationToken)
    {
        if (workflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(workflowInstanceId));
        }

        if (stepOrder <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(stepOrder),
                "Step order must be greater than zero.");
        }

        if (attempt <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(attempt),
                "Attempt must be greater than zero.");
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.WorkflowStepExecution
            SET Status = @Status,
                ErrorMessage = @ErrorMessage,
                CompletedAtUtc = COALESCE(@CompletedAtUtc, CompletedAtUtc),
                CompensatedAtUtc = COALESCE(@CompensatedAtUtc, CompensatedAtUtc)
            WHERE WorkflowInstanceId = @WorkflowInstanceId
              AND StepOrder = @StepOrder
              AND Attempt = @Attempt;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@StepOrder", SqlDbType.Int).Value = stepOrder;
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value = attempt;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value = status.ToString();
        AddNullable(command, "@ErrorMessage", SqlDbType.NVarChar, 1024, errorMessage);
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, completedAtUtc);
        AddNullable(command, "@CompensatedAtUtc", SqlDbType.DateTimeOffset, compensatedAtUtc);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private static void Validate(WorkflowStepAttemptStart request)
    {
        if (request.WorkflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.StepName))
        {
            throw new ArgumentException(
                "Step name is required.",
                nameof(request));
        }

        if (request.StepOrder <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Step order must be greater than zero.");
        }

        if (request.Attempt <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Attempt must be greater than zero.");
        }
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

    private static void Validate(WorkflowOutboxMessageDraft message)
    {
        if (message.WorkflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(message));
        }

        if (string.IsNullOrWhiteSpace(message.MessageType))
        {
            throw new ArgumentException(
                "Message type is required.",
                nameof(message));
        }

        if (string.IsNullOrWhiteSpace(message.Destination))
        {
            throw new ArgumentException(
                "Destination is required.",
                nameof(message));
        }

        if (string.IsNullOrWhiteSpace(message.PayloadJson))
        {
            throw new ArgumentException(
                "Payload JSON is required.",
                nameof(message));
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
        _ = command.Parameters.Add("@HeadersJson", SqlDbType.NVarChar, -1).Value = entity.HeadersJson;
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

    private static WorkflowStepAttemptRecord MapAttemptRecord(SqlDataReader reader) =>
        MapAttemptRecord(MapStepExecution(reader));

    private static WorkflowStepAttemptRecord MapAttemptRecord(
        WorkflowStepExecutionEntity entity) =>
        new(
            entity.Id,
            entity.WorkflowInstanceId,
            entity.StepName,
            entity.StepOrder,
            MapAttemptStatus(entity.Status),
            entity.Attempt,
            entity.CommandId,
            entity.ErrorMessage,
            entity.StartedAtUtc,
            entity.CompletedAtUtc,
            entity.CompensatedAtUtc);

    private static WorkflowOutboxMessageEntity MapOutboxMessage(SqlDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(0),
            WorkflowInstanceId = reader.GetGuid(1),
            MessageType = reader.GetString(2),
            Destination = reader.GetString(3),
            PayloadJson = reader.GetString(4),
            HeadersJson = reader.GetString(5),
            Status = ParseOutboxStatus(reader.GetString(6)),
            RetryCount = reader.GetInt32(7),
            ErrorMessage = reader.IsDBNull(8) ? null : reader.GetString(8),
            CreatedAtUtc = reader.GetDateTimeOffset(9),
            LastAttemptAtUtc = reader.IsDBNull(10) ? null : reader.GetDateTimeOffset(10),
            NextAttemptAtUtc = reader.IsDBNull(11) ? null : reader.GetDateTimeOffset(11),
            PublishedAtUtc = reader.IsDBNull(12) ? null : reader.GetDateTimeOffset(12)
        };

    private static WorkflowOutboxMessageRecord MapOutboxRecord(
        WorkflowOutboxMessageEntity entity) =>
        new(
            entity.Id,
            entity.WorkflowInstanceId,
            entity.MessageType,
            entity.Destination,
            entity.PayloadJson,
            entity.HeadersJson,
            MapOutboxStatus(entity.Status),
            entity.RetryCount,
            entity.ErrorMessage,
            entity.CreatedAtUtc,
            entity.LastAttemptAtUtc,
            entity.NextAttemptAtUtc,
            entity.PublishedAtUtc);

    private static WorkflowStepAttemptStatus MapAttemptStatus(
        WorkflowStepExecutionStatus status) =>
        status switch
        {
            WorkflowStepExecutionStatus.Running => WorkflowStepAttemptStatus.Running,
            WorkflowStepExecutionStatus.Succeeded => WorkflowStepAttemptStatus.Succeeded,
            WorkflowStepExecutionStatus.Failed => WorkflowStepAttemptStatus.Failed,
            WorkflowStepExecutionStatus.Compensated => WorkflowStepAttemptStatus.Compensated,
            WorkflowStepExecutionStatus.CompensationFailed =>
                WorkflowStepAttemptStatus.CompensationFailed,
            _ => WorkflowStepAttemptStatus.Pending
        };

    private static WorkflowOutboxStatus MapOutboxStatus(
        WorkflowOutboxMessageStatus status) =>
        status switch
        {
            WorkflowOutboxMessageStatus.Published => WorkflowOutboxStatus.Published,
            WorkflowOutboxMessageStatus.Failed => WorkflowOutboxStatus.Failed,
            _ => WorkflowOutboxStatus.Pending
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
