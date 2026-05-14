using System.Data;
using System.Text.Json;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Application.Workflows.Contracts;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.Messaging;
using Hemi.Infrastructure.Monitoring;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure.WorkflowPersistence.Repositories;

public sealed class SqlServerWorkflowJournal(
    string connectionString,
    WorkflowMetrics? workflowMetrics = null)
    : IWorkflowJournal
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new(JsonSerializerDefaults.Web);

    public async Task<WorkflowJournalResult> AppendAsync(
        WorkflowJournalEntry entry,
        CancellationToken cancellationToken = default)
    {
        Validate(entry);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.ReadCommitted,
                cancellationToken);

        var workflowInstanceVersion = entry.ExpectedWorkflowVersion;
        WorkflowStepAttemptRecord? stepAttempt = null;

        try
        {
            if (entry.PayloadJson is not null)
            {
                workflowInstanceVersion = await UpdatePayloadAsync(
                    connection,
                    transaction,
                    entry.WorkflowInstanceId,
                    workflowInstanceVersion,
                    entry.PayloadJson,
                    cancellationToken);
            }

            if (entry.Step is not null)
            {
                stepAttempt = await ApplyStepAsync(
                    connection,
                    transaction,
                    entry.WorkflowInstanceId,
                    entry.Step,
                    cancellationToken);
            }

            if (entry.State is not null)
            {
                workflowInstanceVersion = await UpdateStateAsync(
                    connection,
                    transaction,
                    entry.WorkflowInstanceId,
                    workflowInstanceVersion,
                    entry.State,
                    cancellationToken);
            }

            if (entry.Event is not null)
            {
                await InsertOutboxEventAsync(
                    connection,
                    transaction,
                    entry.WorkflowInstanceId,
                    entry.Event,
                    cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            try
            {
                await transaction.RollbackAsync(CancellationToken.None);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }

        if (entry.Event is not null)
        {
            workflowMetrics?.RecordWorkflowEvent(entry.Event);
        }

        return new WorkflowJournalResult(
            workflowInstanceVersion,
            stepAttempt);
    }

    private static async Task<int> UpdatePayloadAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        int expectedVersion,
        string payloadJson,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE dbo.WorkflowInstance
            SET PayloadJson = @PayloadJson,
                Version = Version + 1,
                UpdatedAtUtc = @UpdatedAtUtc
            OUTPUT inserted.Version
            WHERE Id = @Id AND Version = @ExpectedVersion;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@ExpectedVersion", SqlDbType.Int).Value =
            expectedVersion;
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value =
            payloadJson;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;

        var version = await command.ExecuteScalarAsync(cancellationToken);
        if (version is null)
        {
            throw new InvalidOperationException(
                "Workflow context update failed due to optimistic concurrency.");
        }

        return Convert.ToInt32(version);
    }

    private static async Task<int> UpdateStateAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        int expectedVersion,
        WorkflowStateJournalEntry state,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE dbo.WorkflowInstance
            SET State = @State,
                LastError = @LastError,
                Version = Version + 1,
                UpdatedAtUtc = @UpdatedAtUtc,
                CompletedAtUtc = @CompletedAtUtc,
                NextAttemptAtUtc = @NextAttemptAtUtc,
                LeaseOwner = CASE WHEN @ClearLease = 1 THEN NULL ELSE LeaseOwner END,
                LeaseUntilUtc = CASE WHEN @ClearLease = 1 THEN NULL ELSE LeaseUntilUtc END
            OUTPUT inserted.Version
            WHERE Id = @Id
              AND Version = @ExpectedVersion
              AND NOT (
                  @State = @PendingState
                  AND State IN (
                      @SucceededState,
                      @FailedState,
                      @CompensatedState,
                      @CompensationFailedState,
                      @CancelledState
                  )
              );
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@State", SqlDbType.NVarChar, 32).Value =
            state.State.ToString();
        _ = command.Parameters.Add("@PendingState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Pending.ToString();
        _ = command.Parameters.Add("@SucceededState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Succeeded.ToString();
        _ = command.Parameters.Add("@FailedState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Failed.ToString();
        _ = command.Parameters.Add("@CompensatedState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Compensated.ToString();
        _ = command.Parameters.Add("@CompensationFailedState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.CompensationFailed.ToString();
        _ = command.Parameters.Add("@CancelledState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Cancelled.ToString();
        AddNullable(command, "@LastError", SqlDbType.NVarChar, 1024, state.LastError);
        _ = command.Parameters.Add("@ExpectedVersion", SqlDbType.Int).Value =
            expectedVersion;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, state.CompletedAtUtc);
        AddNullable(command, "@NextAttemptAtUtc", SqlDbType.DateTimeOffset, state.NextAttemptAtUtc);
        _ = command.Parameters.Add("@ClearLease", SqlDbType.Bit).Value =
            state.State != WorkflowState.Running;

        var version = await command.ExecuteScalarAsync(cancellationToken);
        if (version is null)
        {
            throw new InvalidOperationException(
                "Workflow state update failed due to optimistic concurrency.");
        }

        return Convert.ToInt32(version);
    }

    private static Task<WorkflowStepAttemptRecord?> ApplyStepAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        WorkflowStepJournalEntry step,
        CancellationToken cancellationToken) =>
        step.Action == WorkflowStepJournalAction.Running
            ? MarkStepRunningAsync(
                connection,
                transaction,
                workflowInstanceId,
                step,
                cancellationToken)
            : MarkStepTerminalAsync(
                connection,
                transaction,
                workflowInstanceId,
                step,
                cancellationToken);

    private static async Task<WorkflowStepAttemptRecord?> MarkStepRunningAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        WorkflowStepJournalEntry step,
        CancellationToken cancellationToken)
    {
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

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value =
            Guid.NewGuid();
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@StepName", SqlDbType.NVarChar, 128).Value =
            step.StepName;
        _ = command.Parameters.Add("@StepOrder", SqlDbType.Int).Value =
            step.StepOrder;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            WorkflowStepExecutionStatus.Running.ToString();
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value =
            step.Attempt;
        AddNullable(command, "@CommandId", SqlDbType.UniqueIdentifier, step.CommandId);
        _ = command.Parameters.Add("@StartedAtUtc", SqlDbType.DateTimeOffset).Value =
            step.StartedAtUtc ?? DateTimeOffset.UtcNow;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException(
                "Step attempt update did not return a row.");
        }

        return MapAttemptRecord(reader);
    }

    private static async Task<WorkflowStepAttemptRecord?> MarkStepTerminalAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        WorkflowStepJournalEntry step,
        CancellationToken cancellationToken)
    {
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

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@StepOrder", SqlDbType.Int).Value =
            step.StepOrder;
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value =
            step.Attempt;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            MapStepStatus(step.Action).ToString();
        AddNullable(command, "@ErrorMessage", SqlDbType.NVarChar, 1024, step.ErrorMessage);
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, step.CompletedAtUtc);
        AddNullable(command, "@CompensatedAtUtc", SqlDbType.DateTimeOffset, step.CompensatedAtUtc);

        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new InvalidOperationException(
                "Workflow step attempt update failed.");
        }

        return null;
    }

    private static async Task InsertOutboxEventAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid workflowInstanceId,
        WorkflowEvent workflowEvent,
        CancellationToken cancellationToken)
    {
        var response = new WorkflowEventResponse(
            workflowEvent.EventName,
            workflowEvent.WorkflowId,
            workflowEvent.WorkflowName,
            workflowEvent.CorrelationId,
            workflowEvent.State,
            workflowEvent.StepName,
            workflowEvent.Error?.Message,
            workflowEvent.OccurredAtUtc);

        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["workflow-instance-id"] = workflowInstanceId.ToString("D"),
            ["workflow-id"] = workflowEvent.WorkflowId,
            ["workflow-name"] = workflowEvent.WorkflowName,
            ["correlation-id"] = workflowEvent.CorrelationId,
            ["workflow-state"] = workflowEvent.State.ToString()
        };

        const string sql = """
            INSERT dbo.WorkflowOutboxMessage (
                Id, WorkflowInstanceId, MessageType, Destination,
                PayloadJson, HeadersJson, Status, RetryCount, ErrorMessage,
                CreatedAtUtc, LastAttemptAtUtc, NextAttemptAtUtc, PublishedAtUtc)
            VALUES (
                @Id, @WorkflowInstanceId, @MessageType, @Destination,
                @PayloadJson, @HeadersJson, @Status, @RetryCount, NULL,
                @CreatedAtUtc, NULL, @NextAttemptAtUtc, NULL);
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value =
            Guid.NewGuid();
        _ = command.Parameters.Add("@WorkflowInstanceId", SqlDbType.UniqueIdentifier).Value =
            workflowInstanceId;
        _ = command.Parameters.Add("@MessageType", SqlDbType.NVarChar, 128).Value =
            workflowEvent.EventName;
        _ = command.Parameters.Add("@Destination", SqlDbType.NVarChar, 256).Value =
            OutboxWorkflowEventPublisher.Destination;
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value =
            JsonSerializer.Serialize(response, SerializerOptions);
        _ = command.Parameters.Add("@HeadersJson", SqlDbType.NVarChar, -1).Value =
            JsonSerializer.Serialize(headers, SerializerOptions);
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            WorkflowOutboxMessageStatus.Pending.ToString();
        _ = command.Parameters.Add("@RetryCount", SqlDbType.Int).Value = 0;
        _ = command.Parameters.Add("@CreatedAtUtc", SqlDbType.DateTimeOffset).Value =
            workflowEvent.OccurredAtUtc;
        _ = command.Parameters.Add("@NextAttemptAtUtc", SqlDbType.DateTimeOffset).Value =
            workflowEvent.OccurredAtUtc;

        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void Validate(WorkflowJournalEntry entry)
    {
        if (entry.WorkflowInstanceId == Guid.Empty)
        {
            throw new ArgumentException(
                "Workflow instance id is required.",
                nameof(entry));
        }

        if (entry.ExpectedWorkflowVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entry),
                "Expected workflow version must be greater than zero.");
        }

        if (entry.PayloadJson is not null &&
            string.IsNullOrWhiteSpace(entry.PayloadJson))
        {
            throw new ArgumentException(
                "Payload JSON cannot be empty.",
                nameof(entry));
        }

        if (entry.Step is not null)
        {
            Validate(entry.Step);
        }
    }

    private static void Validate(WorkflowStepJournalEntry step)
    {
        if (string.IsNullOrWhiteSpace(step.StepName))
        {
            throw new ArgumentException(
                "Step name is required.",
                nameof(step));
        }

        if (step.StepOrder <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(step),
                "Step order must be greater than zero.");
        }

        if (step.Attempt <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(step),
                "Step attempt must be greater than zero.");
        }

        if ((step.Action is WorkflowStepJournalAction.Failed
                or WorkflowStepJournalAction.CompensationFailed) &&
            string.IsNullOrWhiteSpace(step.ErrorMessage))
        {
            throw new ArgumentException(
                "Error message is required for failed step journal entries.",
                nameof(step));
        }
    }

    private static WorkflowStepExecutionStatus MapStepStatus(
        WorkflowStepJournalAction action) =>
        action switch
        {
            WorkflowStepJournalAction.Succeeded => WorkflowStepExecutionStatus.Succeeded,
            WorkflowStepJournalAction.Failed => WorkflowStepExecutionStatus.Failed,
            WorkflowStepJournalAction.Compensated => WorkflowStepExecutionStatus.Compensated,
            WorkflowStepJournalAction.CompensationFailed =>
                WorkflowStepExecutionStatus.CompensationFailed,
            _ => WorkflowStepExecutionStatus.Running
        };

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

    private static WorkflowStepAttemptRecord MapAttemptRecord(
        SqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetGuid(1),
            reader.GetString(2),
            reader.GetInt32(3),
            MapAttemptStatus(
                Enum.TryParse<WorkflowStepExecutionStatus>(
                    reader.GetString(4),
                    out var status)
                    ? status
                    : WorkflowStepExecutionStatus.Pending),
            reader.GetInt32(5),
            reader.IsDBNull(6) ? null : reader.GetGuid(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetDateTimeOffset(8),
            reader.IsDBNull(9) ? null : reader.GetDateTimeOffset(9),
            reader.IsDBNull(10) ? null : reader.GetDateTimeOffset(10));

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
