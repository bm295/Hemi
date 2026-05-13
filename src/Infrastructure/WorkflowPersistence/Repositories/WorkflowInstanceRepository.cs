using System.Data;
using System.Data.Common;
using Hemi.Application.Workflows.Abstractions;
using Hemi.Domain.Workflows;
using Hemi.Infrastructure.WorkflowPersistence.Entities;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure.WorkflowPersistence.Repositories;

public sealed class WorkflowInstanceRepository(string connectionString)
    : IWorkflowInstanceStore
{
    private const string InstanceColumns = """
        Id, CommandId, WorkflowId, WorkflowName, CorrelationId, State, PayloadJson,
        LastError, Version, CreatedAtUtc, UpdatedAtUtc, CompletedAtUtc,
        IdempotencyKey, RequestHash, RequestedBy, Attempt, NextAttemptAtUtc,
        LeaseOwner, LeaseUntilUtc
        """;

    public async Task<WorkflowStartResult> StartWorkflowAsync(
        WorkflowStartRequest request,
        CancellationToken cancellationToken = default)
    {
        Validate(request);

        var requestedAtUtc = request.RequestedAtUtc == default
            ? DateTimeOffset.UtcNow
            : request.RequestedAtUtc;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(
            IsolationLevel.Serializable,
            cancellationToken);

        var existingByIdempotency = await GetByIdempotencyKeyAsync(
            connection,
            transaction,
            request.IdempotencyKey,
            cancellationToken);

        if (existingByIdempotency is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            return SameRequest(existingByIdempotency, request.RequestHash)
                ? new WorkflowStartResult(
                    WorkflowStartStatus.Existing,
                    MapRecord(existingByIdempotency),
                    existingByIdempotency.RequestHash)
                : new WorkflowStartResult(
                    WorkflowStartStatus.IdempotencyConflict,
                    MapRecord(existingByIdempotency),
                    existingByIdempotency.RequestHash);
        }

        var existingByCorrelation = await GetByCorrelationAsync(
            connection,
            transaction,
            request.WorkflowId,
            request.CorrelationId,
            cancellationToken);

        if (existingByCorrelation is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            return SameRequest(existingByCorrelation, request.RequestHash)
                ? new WorkflowStartResult(
                    WorkflowStartStatus.Existing,
                    MapRecord(existingByCorrelation),
                    existingByCorrelation.RequestHash)
                : new WorkflowStartResult(
                    WorkflowStartStatus.CorrelationConflict,
                    MapRecord(existingByCorrelation),
                    existingByCorrelation.RequestHash);
        }

        var instance = new WorkflowInstanceEntity
        {
            Id = Guid.NewGuid(),
            CommandId = Guid.NewGuid(),
            WorkflowId = request.WorkflowId,
            WorkflowName = request.WorkflowName,
            CorrelationId = request.CorrelationId,
            State = WorkflowState.Pending,
            PayloadJson = request.PayloadJson,
            Version = 1,
            CreatedAtUtc = requestedAtUtc,
            UpdatedAtUtc = requestedAtUtc,
            IdempotencyKey = request.IdempotencyKey,
            RequestHash = request.RequestHash,
            RequestedBy = request.RequestedBy,
            Attempt = 0,
            NextAttemptAtUtc = request.NextAttemptAtUtc
        };

        await InsertAsync(connection, transaction, instance, cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        return new WorkflowStartResult(
            WorkflowStartStatus.Created,
            MapRecord(instance),
            instance.RequestHash);
    }

    public async Task<WorkflowInstanceRecord?> GetInstanceByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default)
    {
        var entity = await GetByIdAsync(id, cancellationToken);
        return entity is null ? null : MapRecord(entity);
    }

    public async Task<WorkflowInstanceRecord?> GetInstanceByCorrelationAsync(
        string workflowId,
        string correlationId,
        CancellationToken cancellationToken = default)
    {
        var entity = await GetByCorrelationAsync(
            workflowId,
            correlationId,
            cancellationToken);

        return entity is null ? null : MapRecord(entity);
    }

    public async Task<IReadOnlyCollection<WorkflowInstanceRecord>> ClaimDueInstancesAsync(
        DateTimeOffset nowUtc,
        string leaseOwner,
        TimeSpan leaseDuration,
        int batchSize = 10,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(leaseOwner))
        {
            throw new ArgumentException(
                "Lease owner is required.",
                nameof(leaseOwner));
        }

        if (leaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaseDuration),
                "Lease duration must be greater than zero.");
        }

        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                "Batch size must be greater than zero.");
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            ;WITH DueInstances AS (
                SELECT TOP (@BatchSize) *
                FROM dbo.WorkflowInstance WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE State IN (@PendingState, @RunningState)
                  AND (NextAttemptAtUtc IS NULL OR NextAttemptAtUtc <= @NowUtc)
                  AND (LeaseUntilUtc IS NULL OR LeaseUntilUtc <= @NowUtc)
                ORDER BY CreatedAtUtc
            )
            UPDATE DueInstances
            SET State = @RunningState,
                Attempt = Attempt + 1,
                LeaseOwner = @LeaseOwner,
                LeaseUntilUtc = @LeaseUntilUtc,
                Version = Version + 1,
                UpdatedAtUtc = @NowUtc
            OUTPUT inserted.Id, inserted.CommandId, inserted.WorkflowId,
                   inserted.WorkflowName, inserted.CorrelationId, inserted.State,
                   inserted.PayloadJson, inserted.LastError, inserted.Version,
                   inserted.CreatedAtUtc, inserted.UpdatedAtUtc,
                   inserted.CompletedAtUtc, inserted.IdempotencyKey,
                   inserted.RequestHash, inserted.RequestedBy, inserted.Attempt,
                   inserted.NextAttemptAtUtc, inserted.LeaseOwner,
                   inserted.LeaseUntilUtc;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
        _ = command.Parameters.Add("@PendingState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Pending.ToString();
        _ = command.Parameters.Add("@RunningState", SqlDbType.NVarChar, 32).Value =
            WorkflowState.Running.ToString();
        _ = command.Parameters.Add("@NowUtc", SqlDbType.DateTimeOffset).Value = nowUtc;
        _ = command.Parameters.Add("@LeaseOwner", SqlDbType.NVarChar, 128).Value = leaseOwner;
        _ = command.Parameters.Add("@LeaseUntilUtc", SqlDbType.DateTimeOffset).Value =
            nowUtc.Add(leaseDuration);

        var result = new List<WorkflowInstanceRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapRecord(reader));
        }

        return result;
    }

    public async Task<WorkflowInstanceEntity?> GetByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            SELECT {InstanceColumns}
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
        ValidateCorrelation(workflowId, correlationId);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            SELECT {InstanceColumns}
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

        if (entity.CommandId == Guid.Empty)
        {
            entity.CommandId = Guid.NewGuid();
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
                    CommandId = @CommandId,
                    WorkflowId = @WorkflowId,
                    WorkflowName = @WorkflowName,
                    CorrelationId = @CorrelationId,
                    State = @State,
                    PayloadJson = @PayloadJson,
                    LastError = @LastError,
                    Version = Target.Version + 1,
                    UpdatedAtUtc = @UpdatedAtUtc,
                    CompletedAtUtc = @CompletedAtUtc,
                    IdempotencyKey = @IdempotencyKey,
                    RequestHash = @RequestHash,
                    RequestedBy = @RequestedBy,
                    Attempt = @Attempt,
                    NextAttemptAtUtc = @NextAttemptAtUtc,
                    LeaseOwner = @LeaseOwner,
                    LeaseUntilUtc = @LeaseUntilUtc
            WHEN NOT MATCHED THEN
                INSERT (Id, CommandId, WorkflowId, WorkflowName, CorrelationId,
                        State, PayloadJson, LastError, Version, CreatedAtUtc,
                        UpdatedAtUtc, CompletedAtUtc, IdempotencyKey, RequestHash,
                        RequestedBy, Attempt, NextAttemptAtUtc, LeaseOwner,
                        LeaseUntilUtc)
                VALUES (@Id, @CommandId, @WorkflowId, @WorkflowName,
                        @CorrelationId, @State, @PayloadJson, @LastError,
                        @InitialVersion, @CreatedAtUtc, @UpdatedAtUtc,
                        @CompletedAtUtc, @IdempotencyKey, @RequestHash,
                        @RequestedBy, @Attempt, @NextAttemptAtUtc, @LeaseOwner,
                        @LeaseUntilUtc)
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
                CompletedAtUtc = @CompletedAtUtc,
                LeaseOwner = CASE WHEN @ClearLease = 1 THEN NULL ELSE LeaseOwner END,
                LeaseUntilUtc = CASE WHEN @ClearLease = 1 THEN NULL ELSE LeaseUntilUtc END
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
        _ = command.Parameters.Add("@ClearLease", SqlDbType.Bit).Value = IsTerminal(state);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task<bool> TryUpdatePayloadAsync(
        Guid id,
        int expectedVersion,
        string payloadJson,
        CancellationToken cancellationToken = default)
    {
        if (expectedVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion),
                "Expected version must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            throw new ArgumentException(
                "Payload JSON is required.",
                nameof(payloadJson));
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.WorkflowInstance
            SET PayloadJson = @PayloadJson,
                Version = Version + 1,
                UpdatedAtUtc = @UpdatedAtUtc
            WHERE Id = @Id AND Version = @ExpectedVersion;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;
        _ = command.Parameters.Add("@ExpectedVersion", SqlDbType.Int).Value = expectedVersion;
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value = payloadJson;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task<bool> TryReleaseLeaseAsync(
        Guid id,
        string leaseOwner,
        int expectedVersion,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(leaseOwner))
        {
            throw new ArgumentException(
                "Lease owner is required.",
                nameof(leaseOwner));
        }

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
            SET LeaseOwner = NULL,
                LeaseUntilUtc = NULL,
                Version = Version + 1,
                UpdatedAtUtc = @UpdatedAtUtc
            WHERE Id = @Id
              AND Version = @ExpectedVersion
              AND LeaseOwner = @LeaseOwner;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = id;
        _ = command.Parameters.Add("@ExpectedVersion", SqlDbType.Int).Value = expectedVersion;
        _ = command.Parameters.Add("@LeaseOwner", SqlDbType.NVarChar, 128).Value = leaseOwner;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private static async Task InsertAsync(
        SqlConnection connection,
        DbTransaction transaction,
        WorkflowInstanceEntity entity,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT dbo.WorkflowInstance (
                Id, CommandId, WorkflowId, WorkflowName, CorrelationId, State,
                PayloadJson, LastError, Version, CreatedAtUtc, UpdatedAtUtc,
                CompletedAtUtc, IdempotencyKey, RequestHash, RequestedBy, Attempt,
                NextAttemptAtUtc, LeaseOwner, LeaseUntilUtc)
            VALUES (
                @Id, @CommandId, @WorkflowId, @WorkflowName, @CorrelationId,
                @State, @PayloadJson, @LastError, @Version, @CreatedAtUtc,
                @UpdatedAtUtc, @CompletedAtUtc, @IdempotencyKey, @RequestHash,
                @RequestedBy, @Attempt, @NextAttemptAtUtc, @LeaseOwner,
                @LeaseUntilUtc);
            """;

        await using var command = new SqlCommand(sql, connection, (SqlTransaction)transaction);
        AddInstanceParameters(command, entity);
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<WorkflowInstanceEntity?> GetByIdempotencyKeyAsync(
        SqlConnection connection,
        DbTransaction transaction,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT {InstanceColumns}
            FROM dbo.WorkflowInstance WITH (UPDLOCK, HOLDLOCK)
            WHERE IdempotencyKey = @IdempotencyKey;
            """;

        await using var command = new SqlCommand(sql, connection, (SqlTransaction)transaction);
        _ = command.Parameters.Add("@IdempotencyKey", SqlDbType.NVarChar, 256).Value =
            idempotencyKey;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapInstance(reader)
            : null;
    }

    private static async Task<WorkflowInstanceEntity?> GetByCorrelationAsync(
        SqlConnection connection,
        DbTransaction transaction,
        string workflowId,
        string correlationId,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT {InstanceColumns}
            FROM dbo.WorkflowInstance WITH (UPDLOCK, HOLDLOCK)
            WHERE WorkflowId = @WorkflowId AND CorrelationId = @CorrelationId;
            """;

        await using var command = new SqlCommand(sql, connection, (SqlTransaction)transaction);
        _ = command.Parameters.Add("@WorkflowId", SqlDbType.NVarChar, 128).Value = workflowId;
        _ = command.Parameters.Add("@CorrelationId", SqlDbType.NVarChar, 128).Value = correlationId;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapInstance(reader)
            : null;
    }

    private static void Validate(WorkflowStartRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.WorkflowId))
        {
            throw new ArgumentException(
                "Workflow id is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.WorkflowName))
        {
            throw new ArgumentException(
                "Workflow name is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.CorrelationId))
        {
            throw new ArgumentException(
                "Correlation id is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.PayloadJson))
        {
            throw new ArgumentException(
                "Payload JSON is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.IdempotencyKey))
        {
            throw new ArgumentException(
                "Idempotency key is required.",
                nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.RequestHash))
        {
            throw new ArgumentException(
                "Request hash is required.",
                nameof(request));
        }

        if (request.RequestHash.Length != 64)
        {
            throw new ArgumentException(
                "Request hash must be 64 characters.",
                nameof(request));
        }
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

    private static void ValidateCorrelation(
        string workflowId,
        string correlationId)
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
    }

    private static void AddInstanceParameters(
        SqlCommand command,
        WorkflowInstanceEntity entity)
    {
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = entity.Id;
        _ = command.Parameters.Add("@CommandId", SqlDbType.UniqueIdentifier).Value =
            entity.CommandId;
        _ = command.Parameters.Add("@WorkflowId", SqlDbType.NVarChar, 128).Value = entity.WorkflowId;
        _ = command.Parameters.Add("@WorkflowName", SqlDbType.NVarChar, 128).Value =
            entity.WorkflowName;
        _ = command.Parameters.Add("@CorrelationId", SqlDbType.NVarChar, 128).Value =
            entity.CorrelationId;
        _ = command.Parameters.Add("@State", SqlDbType.NVarChar, 32).Value = entity.State.ToString();
        _ = command.Parameters.Add("@PayloadJson", SqlDbType.NVarChar, -1).Value =
            entity.PayloadJson;
        AddNullable(command, "@LastError", SqlDbType.NVarChar, 1024, entity.LastError);
        _ = command.Parameters.Add("@Version", SqlDbType.Int).Value =
            entity.Version <= 0 ? 1 : entity.Version;
        _ = command.Parameters.Add("@CreatedAtUtc", SqlDbType.DateTimeOffset).Value =
            entity.CreatedAtUtc;
        _ = command.Parameters.Add("@UpdatedAtUtc", SqlDbType.DateTimeOffset).Value =
            entity.UpdatedAtUtc;
        AddNullable(command, "@CompletedAtUtc", SqlDbType.DateTimeOffset, entity.CompletedAtUtc);
        AddNullable(command, "@IdempotencyKey", SqlDbType.NVarChar, 256, entity.IdempotencyKey);
        AddNullable(command, "@RequestHash", SqlDbType.Char, 64, entity.RequestHash);
        AddNullable(command, "@RequestedBy", SqlDbType.NVarChar, 128, entity.RequestedBy);
        _ = command.Parameters.Add("@Attempt", SqlDbType.Int).Value = entity.Attempt;
        AddNullable(command, "@NextAttemptAtUtc", SqlDbType.DateTimeOffset, entity.NextAttemptAtUtc);
        AddNullable(command, "@LeaseOwner", SqlDbType.NVarChar, 128, entity.LeaseOwner);
        AddNullable(command, "@LeaseUntilUtc", SqlDbType.DateTimeOffset, entity.LeaseUntilUtc);
    }

    private static WorkflowInstanceEntity MapInstance(SqlDataReader reader) =>
        new()
        {
            Id = reader.GetGuid(0),
            CommandId = reader.GetGuid(1),
            WorkflowId = reader.GetString(2),
            WorkflowName = reader.GetString(3),
            CorrelationId = reader.GetString(4),
            State = ParseWorkflowState(reader.GetString(5)),
            PayloadJson = reader.GetString(6),
            LastError = reader.IsDBNull(7) ? null : reader.GetString(7),
            Version = reader.GetInt32(8),
            CreatedAtUtc = reader.GetDateTimeOffset(9),
            UpdatedAtUtc = reader.GetDateTimeOffset(10),
            CompletedAtUtc = reader.IsDBNull(11)
                ? null
                : reader.GetDateTimeOffset(11),
            IdempotencyKey = reader.IsDBNull(12) ? null : reader.GetString(12),
            RequestHash = reader.IsDBNull(13) ? null : reader.GetString(13),
            RequestedBy = reader.IsDBNull(14) ? null : reader.GetString(14),
            Attempt = reader.GetInt32(15),
            NextAttemptAtUtc = reader.IsDBNull(16) ? null : reader.GetDateTimeOffset(16),
            LeaseOwner = reader.IsDBNull(17) ? null : reader.GetString(17),
            LeaseUntilUtc = reader.IsDBNull(18) ? null : reader.GetDateTimeOffset(18)
        };

    private static WorkflowInstanceRecord MapRecord(SqlDataReader reader) =>
        MapRecord(MapInstance(reader));

    private static WorkflowInstanceRecord MapRecord(WorkflowInstanceEntity entity) =>
        new(
            entity.Id,
            entity.CommandId,
            entity.WorkflowId,
            entity.WorkflowName,
            entity.CorrelationId,
            entity.State,
            entity.PayloadJson,
            entity.LastError,
            entity.Version,
            entity.CreatedAtUtc,
            entity.UpdatedAtUtc,
            entity.CompletedAtUtc,
            entity.IdempotencyKey,
            entity.RequestHash,
            entity.RequestedBy,
            entity.Attempt,
            entity.NextAttemptAtUtc,
            entity.LeaseOwner,
            entity.LeaseUntilUtc);

    private static bool SameRequest(
        WorkflowInstanceEntity instance,
        string requestHash) =>
        string.Equals(
            instance.RequestHash,
            requestHash,
            StringComparison.Ordinal);

    private static bool IsTerminal(WorkflowState state) =>
        state is WorkflowState.Succeeded
            or WorkflowState.Failed
            or WorkflowState.Compensated
            or WorkflowState.CompensationFailed
            or WorkflowState.Cancelled;

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
