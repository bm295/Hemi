using System.Text.Json;
using Hemi.Application;
using Hemi.Application.Sagas.Legacy;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure;

public sealed class SqlServerSagaStateAdapter(string connectionString) : ISagaStateQueryPort
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

    private static OrderFulfillmentSagaStatus MapInstanceStatus(SagaInstanceStatus status) =>
        status switch
        {
            SagaInstanceStatus.Completed => OrderFulfillmentSagaStatus.Completed,
            SagaInstanceStatus.Failed => OrderFulfillmentSagaStatus.Failed,
            SagaInstanceStatus.Compensated => OrderFulfillmentSagaStatus.Compensated,
            _ => OrderFulfillmentSagaStatus.Running
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
