using System.Data;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure.WorkflowPersistence.Repositories;

public sealed class WorkflowInstanceRepository(string connectionString)
{
    public async Task<WorkflowInstanceEntity?> GetByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, WorkflowId, WorkflowName, CorrelationId, State, PayloadJson,
                   LastError, Version, CreatedAtUtc, UpdatedAtUtc, CompletedAtUtc
            FROM dbo.WorkflowInstance
            WHERE Id = @Id;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapInstance(reader)
            : null;
    }

    public async Task<WorkflowInstanceEntity?> GetByCorrelationAsync(
        string workflowId,
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(workflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(workflowId));
        }

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            throw new ArgumentException(
                "Correlation id is required.",
                nameof(correlationId));
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, WorkflowId, WorkflowName, CorrelationId, State, PayloadJson,
                   LastError, Version, CreatedAtUtc, UpdatedAtUtc, CompletedAtUtc
            FROM dbo.WorkflowInstance
            WHERE WorkflowId = @WorkflowId AND CorrelationId = @CorrelationId;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@WorkflowId", SqlDbType.NVarChar, 128).Value = workflowId;
        _ = command.Parameters.Add("@CorrelationId", SqlDbType.NVarChar, 128).Value = correlationId;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapInstance(reader)
            : null;
    }

    public async Task SaveAsync(
        WorkflowInstanceEntity entity,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entity);
        Validate(entity);

        if (entity.Id == Guid.Empty)
        {
            entity.Id = Guid.NewGuid();
        }

        var now = DateTimeOffset.UtcNow;
        if (entity.CreatedAtUtc == default)
        {
            entity.CreatedAtUtc = now;
        }

        if (entity.UpdatedAtUtc == default)
        {
            entity.UpdatedAtUtc = now;
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            MERGE dbo.WorkflowInstance AS Target
            USING (SELECT @Id AS Id) AS Source
            ON Target.Id = Source.Id
            WHEN MATCHED THEN
                UPDATE SET
                    WorkflowId = @WorkflowId,
                    WorkflowName = @WorkflowName,
                    CorrelationId = @CorrelationId,
                    State = @State,
                    PayloadJson = @PayloadJson,
                    LastError = @LastError,
                    Version = Target.Version + 1,
                    UpdatedAtUtc = @UpdatedAtUtc,
                    CompletedAtUtc = @CompletedAtUtc
            WHEN NOT MATCHED THEN
                INSERT (Id, WorkflowId, WorkflowName, CorrelationId, State, PayloadJson,
                        LastError, Version, CreatedAtUtc, UpdatedAtUtc, CompletedAtUtc)
                VALUES (@Id, @WorkflowId, @WorkflowName, @CorrelationId, @State, @PayloadJson,
                        @LastError, @InitialVersion, @CreatedAtUtc, @UpdatedAtUtc, @CompletedAtUtc)
            OUTPUT inserted.Version;
            """;

        await using var command = new SqlCommand(sql, connection);
        AddInstanceParameters(command, entity);
        _ = command.Parameters.Add("@InitialVersion", SqlDbType.Int).Value =
            entity.Version <= 0 ? 1 : entity.Version;

        var version = await command.ExecuteScalarAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                "Workflow instance save did not return a version.");

        entity.Version = Convert.ToInt32(version);
    }

    public async Task<bool> TryUpdateStateAsync(
        Guid id,
        WorkflowState state,
        int expectedVersion,
        string? lastError = null,
        DateTimeOffset? completedAtUtc = null,
        CancellationToken cancellationToken = default)
    {
        if (expectedVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion),
                "Expected version must be greater than zero.");
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.WorkflowInstance
            SET State = @State,
                LastError = @LastError,
                Version = Version + 1,
                UpdatedAtUtc = @UpdatedAtUtc,
                CompletedAtUtc = @CompletedAtUtc
            WHERE Id = @Id AND Version = @ExpectedVersion;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;
        _ = command.Parameters.Add("@State", SqlDbType.NVarChar, 32).Value = state.ToString();
        AddNullable(command, "@LastError", SqlDbType.NVarChar, 1024, lastError);
        _ = command.Parameters.Add("@ExpectedVersion", SqlDbType.Int).Value = expectedVersion;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, completedAtUtc);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private static void Validate(WorkflowInstanceEntity entity)
    {
        if (string.IsNullOrWhiteSpace(entity.WorkflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(entity));
        }

        if (string.IsNullOrWhiteSpace(entity.WorkflowName))
        {
            throw new ArgumentException(
                "Workflow name is required.",
                nameof(entity));
        }

        if (string.IsNullOrWhiteSpace(entity.CorrelationId))
        {
            throw new ArgumentException(
                "Correlation id is required.",
                nameof(entity));
        }
    }

    private static void AddInstanceParameters(
        SqlCommand command,
        WorkflowInstanceEntity entity)
    {
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = entity.Id;
        _ = command.Parameters.Add("@WorkflowId", SqlDbType.NVarChar, 128).Value = entity.WorkflowId;
        _ = command.Parameters.Add("@WorkflowName", SqlDbType.NVarChar, 128).Value = entity.WorkflowName;
        _ = command.Parameters.Add("@CorrelationId", SqlDbType.NVarChar, 128).Value = entity.CorrelationId;
        _ = command.Parameters.Add("@State", SqlDbType.NVarChar, 32).Value = entity.State.ToString();
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value = entity.PayloadJson;
        AddNullable(command, "@LastError", SqlDbType.NVarChar, 1024, entity.LastError);
        _ = command.Parameters.Add("@CreatedAtUtc", SqlDbType.DateTimeOffset).Value = entity.CreatedAtUtc;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value = entity.UpdatedAtUtc;
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, entity.CompletedAtUtc);
    }

    private static WorkflowInstanceEntity MapInstance(SqlDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(0),
            WorkflowId = reader.GetString(1),
            WorkflowName = reader.GetString(2),
            CorrelationId = reader.GetString(3),
            State = ParseWorkflowState(reader.GetString(4)),
            PayloadJson = reader.GetString(5),
            LastError = reader.IsDBNull(6) ? null : reader.GetString(6),
            Version = reader.GetInt32(7),
            CreatedAtUtc = reader.GetDateTimeOffset(8),
            UpdatedAtUtc = reader.GetDateTimeOffset(9),
            CompletedAtUtc = reader.IsDBNull(10)
                ? null
                : reader.GetDateTimeOffset(10)
        };

    private static WorkflowState ParseWorkflowState(string raw) =>
        Enum.TryParse<WorkflowState>(raw, out var state)
            ? state
            : WorkflowState.Pending;

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
