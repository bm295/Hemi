using System.Data;
using System.Text.Json;
using Hemi.Application;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure;

public sealed class SqlServerSagaStateAdapter(string connectionString) : ISagaStateQueryPort, ISagaStateCommandPort
{
    public async Task<OrderFulfillmentSagaState?> GetOrderFulfillmentSagaAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sagaSql = """
            SELECT Id, CorrelationId, Status, PayloadJson, UpdatedAt
            FROM dbo.SagaInstance
            WHERE CorrelationId = @OrderId;
            """;

        await using var command = new SqlCommand(sagaSql, connection);
        _ = command.Parameters.AddWithValue("@OrderId", orderId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var sagaId = reader.GetGuid(0);
        var correlationId = reader.GetGuid(1);
        var instanceStatus = ParseInstanceStatus(reader.GetString(2));
        var payloadJson = reader.GetString(3);
        var updatedAt = reader.GetDateTimeOffset(4);

        await reader.CloseAsync();

        var envelope = JsonSerializer.Deserialize<SagaMessageEnvelope>(payloadJson)
            ?? throw new InvalidOperationException("Failed to deserialize saga payload.");

        var steps = await GetStepStatusMapAsync(connection, sagaId, cancellationToken);

        return new OrderFulfillmentSagaState(
            sagaId,
            correlationId,
            MapInstanceStatus(instanceStatus),
            steps.TryGetValue("Kitchen", out var kitchen) ? MapStepStatus(kitchen) : envelope.KitchenStep,
            steps.TryGetValue("Payment", out var payment) ? MapStepStatus(payment) : envelope.PaymentStep,
            steps.TryGetValue("Inventory", out var inventory) ? MapStepStatus(inventory) : envelope.InventoryStep,
            steps.TryGetValue("CloseOrder", out var closeOrder) ? MapStepStatus(closeOrder) : envelope.CloseOrderStep,
            envelope.StartedAt,
            updatedAt,
            envelope.LastError);
    }

    public async Task SaveOrderFulfillmentSagaAsync(OrderFulfillmentSagaState sagaState, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var now = DateTimeOffset.UtcNow;
            var payloadJson = JsonSerializer.Serialize(new SagaMessageEnvelope(
                sagaState.OrderId,
                sagaState.SagaId,
                sagaState.Status,
                sagaState.KitchenStep,
                sagaState.PaymentStep,
                sagaState.InventoryStep,
                sagaState.CloseOrderStep,
                sagaState.LastError,
                sagaState.UpdatedAt,
                sagaState.StartedAt));

            const string upsertInstanceSql = """
                MERGE dbo.SagaInstance AS Target
                USING (SELECT @Id AS Id) AS Source
                ON Target.Id = Source.Id
                WHEN MATCHED THEN
                    UPDATE SET
                        CorrelationId = @CorrelationId,
                        CurrentStep = @CurrentStep,
                        Status = @Status,
                        PayloadJson = @PayloadJson,
                        Version = Version + 1,
                        UpdatedAt = @UpdatedAt
                WHEN NOT MATCHED THEN
                    INSERT (Id, SagaType, CorrelationId, CurrentStep, Status, PayloadJson, Version, CreatedAt, UpdatedAt)
                    VALUES (@Id, @SagaType, @CorrelationId, @CurrentStep, @Status, @PayloadJson, 1, @CreatedAt, @UpdatedAt);
                """;

            await using (var command = new SqlCommand(upsertInstanceSql, connection, (SqlTransaction)transaction))
            {
                _ = command.Parameters.AddWithValue("@Id", sagaState.SagaId);
                _ = command.Parameters.AddWithValue("@SagaType", "OrderFulfillmentSaga");
                _ = command.Parameters.AddWithValue("@CorrelationId", sagaState.OrderId);
                _ = command.Parameters.AddWithValue("@CurrentStep", ResolveCurrentStep(sagaState));
                _ = command.Parameters.AddWithValue("@Status", MapInstanceStatus(sagaState.Status).ToString());
                _ = command.Parameters.AddWithValue("@PayloadJson", payloadJson);
                _ = command.Parameters.AddWithValue("@CreatedAt", sagaState.StartedAt);
                _ = command.Parameters.AddWithValue("@UpdatedAt", now);
                _ = await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await SaveStepAsync(connection, (SqlTransaction)transaction, sagaState.SagaId, "Kitchen", 1, sagaState.KitchenStep, cancellationToken);
            await SaveStepAsync(connection, (SqlTransaction)transaction, sagaState.SagaId, "Payment", 2, sagaState.PaymentStep, cancellationToken);
            await SaveStepAsync(connection, (SqlTransaction)transaction, sagaState.SagaId, "Inventory", 3, sagaState.InventoryStep, cancellationToken);
            await SaveStepAsync(connection, (SqlTransaction)transaction, sagaState.SagaId, "CloseOrder", 4, sagaState.CloseOrderStep, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private static async Task<Dictionary<string, SagaStepExecutionStatus>> GetStepStatusMapAsync(SqlConnection connection, Guid sagaId, CancellationToken cancellationToken)
    {
        const string stepSql = """
            SELECT StepName, Status
            FROM dbo.SagaStep
            WHERE SagaInstanceId = @SagaInstanceId;
            """;

        await using var command = new SqlCommand(stepSql, connection);
        _ = command.Parameters.AddWithValue("@SagaInstanceId", sagaId);

        var result = new Dictionary<string, SagaStepExecutionStatus>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result[reader.GetString(0)] = ParseStepStatus(reader.GetString(1));
        }

        return result;
    }

    private static async Task SaveStepAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid sagaInstanceId,
        string stepName,
        int stepOrder,
        SagaStepStatus stepStatus,
        CancellationToken cancellationToken)
    {
        var mapped = MapStepStatus(stepStatus);
        var now = DateTimeOffset.UtcNow;

        const string stepUpsertSql = """
            MERGE dbo.SagaStep AS Target
            USING (SELECT @SagaInstanceId AS SagaInstanceId, @StepOrder AS StepOrder) AS Source
            ON Target.SagaInstanceId = Source.SagaInstanceId AND Target.StepOrder = Source.StepOrder
            WHEN MATCHED THEN
                UPDATE SET
                    StepName = @StepName,
                    Status = @Status,
                    StartedAt = @StartedAt,
                    CompletedAt = @CompletedAt
            WHEN NOT MATCHED THEN
                INSERT (Id, SagaInstanceId, StepName, StepOrder, Status, CommandId, ReplyMessageId, ErrorMessage, StartedAt, CompletedAt)
                VALUES (@Id, @SagaInstanceId, @StepName, @StepOrder, @Status, NULL, NULL, NULL, @StartedAt, @CompletedAt);
            """;

        await using var command = new SqlCommand(stepUpsertSql, connection, transaction);
        _ = command.Parameters.AddWithValue("@Id", Guid.NewGuid());
        _ = command.Parameters.AddWithValue("@SagaInstanceId", sagaInstanceId);
        _ = command.Parameters.AddWithValue("@StepName", stepName);
        _ = command.Parameters.AddWithValue("@StepOrder", stepOrder);
        _ = command.Parameters.AddWithValue("@Status", mapped.ToString());
        _ = command.Parameters.Add("@StartedAt", SqlDbType.DateTimeOffset).Value = mapped is SagaStepExecutionStatus.Pending ? DBNull.Value : now;
        _ = command.Parameters.Add("@CompletedAt", SqlDbType.DateTimeOffset).Value = mapped is SagaStepExecutionStatus.Pending ? DBNull.Value : now;
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string ResolveCurrentStep(OrderFulfillmentSagaState state)
    {
        if (state.CloseOrderStep is not SagaStepStatus.Completed and not SagaStepStatus.Compensated)
        {
            return "CloseOrder";
        }

        if (state.InventoryStep is not SagaStepStatus.Completed and not SagaStepStatus.Compensated)
        {
            return "Inventory";
        }

        if (state.PaymentStep is not SagaStepStatus.Completed and not SagaStepStatus.Compensated)
        {
            return "Payment";
        }

        return state.KitchenStep is not SagaStepStatus.Completed and not SagaStepStatus.Compensated
            ? "Kitchen"
            : "Done";
    }

    private static SagaInstanceStatus MapInstanceStatus(OrderFulfillmentSagaStatus status) =>
        status switch
        {
            OrderFulfillmentSagaStatus.Running => SagaInstanceStatus.Waiting,
            OrderFulfillmentSagaStatus.Completed => SagaInstanceStatus.Completed,
            OrderFulfillmentSagaStatus.Failed => SagaInstanceStatus.Failed,
            OrderFulfillmentSagaStatus.Compensated => SagaInstanceStatus.Compensated,
            _ => SagaInstanceStatus.Started
        };

    private static OrderFulfillmentSagaStatus MapInstanceStatus(SagaInstanceStatus status) =>
        status switch
        {
            SagaInstanceStatus.Completed => OrderFulfillmentSagaStatus.Completed,
            SagaInstanceStatus.Failed => OrderFulfillmentSagaStatus.Failed,
            SagaInstanceStatus.Compensated => OrderFulfillmentSagaStatus.Compensated,
            _ => OrderFulfillmentSagaStatus.Running
        };

    private static SagaStepExecutionStatus MapStepStatus(SagaStepStatus stepStatus) =>
        stepStatus switch
        {
            SagaStepStatus.Pending => SagaStepExecutionStatus.Pending,
            SagaStepStatus.Completed => SagaStepExecutionStatus.Succeeded,
            SagaStepStatus.Compensated => SagaStepExecutionStatus.Compensated,
            _ => SagaStepExecutionStatus.Pending
        };

    private static SagaStepStatus MapStepStatus(SagaStepExecutionStatus stepStatus) =>
        stepStatus switch
        {
            SagaStepExecutionStatus.Succeeded => SagaStepStatus.Completed,
            SagaStepExecutionStatus.Compensated => SagaStepStatus.Compensated,
            _ => SagaStepStatus.Pending
        };

    private static SagaInstanceStatus ParseInstanceStatus(string raw) =>
        Enum.TryParse<SagaInstanceStatus>(raw, out var status)
            ? status
            : SagaInstanceStatus.Started;

    private static SagaStepExecutionStatus ParseStepStatus(string raw) =>
        Enum.TryParse<SagaStepExecutionStatus>(raw, out var status)
            ? status
            : SagaStepExecutionStatus.Pending;
}
