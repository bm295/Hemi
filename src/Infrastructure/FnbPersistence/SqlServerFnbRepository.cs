using System.Data;
using Hemi.Application;
using Hemi.Domain;
using Microsoft.Data.SqlClient;

namespace Hemi.Infrastructure;

public sealed class SqlServerFnbRepository(string connectionString) :
    IRestaurantQueryPort,
    ITableQueryPort,
    IMenuQueryPort,
    IOrderQueryPort,
    IOrderCommandPort,
    IReservationQueryPort,
    IReservationCommandPort,
    IInventoryQueryPort,
    IInventoryCommandPort,
    IPaymentQueryPort,
    IPaymentCommandPort
{
    private const string OrderClosedReason = "Order Closed";
    private const string InventoryRestoredReason = "Saga Compensation: Inventory Restored";

    private const string PaymentColumns = """
        Id, OrderId, Amount, Method, Status, PaidAtUtc
        """;

    private const string StockMovementColumns = """
        Id, InventoryItemId, OrderId, QuantityChanged, OccurredAtUtc, Reason
        """;

    public async Task<RestaurantProfile> GetRestaurantProfileAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT TOP (1) Name, Location, SeatCapacityMinimum, SeatCapacityMaximum
            FROM dbo.FnbRestaurantProfile
            ORDER BY Name;
            """;

        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("Restaurant profile not found.");
        }

        return new RestaurantProfile(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetInt32(2),
            reader.GetInt32(3));
    }

    public async Task<IReadOnlyCollection<DiningTable>> GetTablesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, Code, Capacity, Status
            FROM dbo.FnbDiningTable
            ORDER BY Code;
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = new List<DiningTable>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapTable(reader));
        }

        return result;
    }

    public async Task<IReadOnlyCollection<MenuItem>> GetMenuItemsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, Name, Category, Price, IsAvailable
            FROM dbo.FnbMenuItem
            ORDER BY Category, Name;
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = new List<MenuItem>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new MenuItem(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetDecimal(3),
                reader.GetBoolean(4)));
        }

        return result;
    }

    public async Task<IReadOnlyCollection<ServiceOrder>> GetOrdersAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        return await GetOrdersAsync(connection, transaction: null, cancellationToken);
    }

    public async Task<ServiceOrder> AddOrderAsync(
        Guid tableId,
        IReadOnlyCollection<OrderLine> lines,
        CancellationToken cancellationToken = default)
    {
        var normalizedLines = NormalizeOrderLines(lines);
        var order = new ServiceOrder(
            Guid.NewGuid(),
            tableId,
            DateTimeOffset.UtcNow,
            OrderStatus.Open,
            normalizedLines);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        _ = await GetTableAsync(
            connection,
            transaction,
            tableId,
            lockRow: true,
            cancellationToken)
            ?? throw new InvalidOperationException("Table not found.");

        await MarkTableOccupiedIfAvailableAsync(
            connection,
            transaction,
            tableId,
            cancellationToken);

        const string sql = """
            INSERT dbo.FnbServiceOrder (Id, TableId, CreatedAtUtc, Status)
            VALUES (@Id, @TableId, @CreatedAtUtc, @Status);
            """;

        await using (var command = new SqlCommand(sql, connection, transaction))
        {
            _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = order.Id;
            _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value = tableId;
            _ = command.Parameters.Add("@CreatedAtUtc", SqlDbType.DateTimeOffset).Value =
                order.CreatedAt;
            _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                order.Status.ToString();
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertOrderLinesAsync(
            connection,
            transaction,
            order.Id,
            normalizedLines,
            cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return order;
    }

    public async Task<ServiceOrder> UpdateOrderLinesAsync(
        Guid orderId,
        IReadOnlyCollection<OrderLine> lines,
        CancellationToken cancellationToken = default)
    {
        var normalizedLines = NormalizeOrderLines(lines);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        var order = await GetOrderByIdAsync(
            connection,
            transaction,
            orderId,
            lockRow: true,
            cancellationToken)
            ?? throw new InvalidOperationException("Order not found.");

        const string deleteSql = """
            DELETE FROM dbo.FnbOrderLine
            WHERE OrderId = @OrderId;
            """;

        await using (var command = new SqlCommand(deleteSql, connection, transaction))
        {
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertOrderLinesAsync(
            connection,
            transaction,
            orderId,
            normalizedLines,
            cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return order with { Lines = normalizedLines };
    }

    public async Task<ServiceOrder> UpdateOrderStatusAsync(
        Guid orderId,
        OrderStatus status,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        var order = await GetOrderByIdAsync(
            connection,
            transaction,
            orderId,
            lockRow: true,
            cancellationToken)
            ?? throw new InvalidOperationException("Order not found.");

        if (order.Status == status)
        {
            await transaction.CommitAsync(cancellationToken);
            return order;
        }

        const string updateSql = """
            UPDATE dbo.FnbServiceOrder
            SET Status = @Status
            WHERE Id = @OrderId;
            """;

        await using (var command = new SqlCommand(updateSql, connection, transaction))
        {
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                status.ToString();
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }

        if (status is OrderStatus.Completed)
        {
            await UpdateTableStatusAsync(
                connection,
                transaction,
                order.TableId,
                TableStatus.Available,
                cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return order with { Status = status };
    }

    public async Task<IReadOnlyCollection<Reservation>> GetReservationsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, GuestName, PartySize, ReservedForUtc, ContactPhone, Notes, Status
            FROM dbo.FnbReservation
            ORDER BY ReservedForUtc, GuestName;
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = new List<Reservation>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new Reservation(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetDateTimeOffset(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                ParseEnum<ReservationStatus>(reader.GetString(6), "FnbReservation.Status")));
        }

        return result;
    }

    public async Task<Reservation> AddReservationAsync(
        string guestName,
        int partySize,
        DateTimeOffset reservedFor,
        string contactPhone,
        string? notes,
        CancellationToken cancellationToken = default)
    {
        if (partySize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partySize),
                "Party size must be greater than zero.");
        }

        var reservation = new Reservation(
            Guid.NewGuid(),
            guestName,
            partySize,
            reservedFor,
            contactPhone,
            notes,
            ReservationStatus.Pending);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            INSERT dbo.FnbReservation (
                Id, GuestName, PartySize, ReservedForUtc, ContactPhone, Notes, Status)
            VALUES (
                @Id, @GuestName, @PartySize, @ReservedForUtc, @ContactPhone, @Notes, @Status);
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = reservation.Id;
        _ = command.Parameters.Add("@GuestName", SqlDbType.NVarChar, 128).Value =
            reservation.GuestName;
        _ = command.Parameters.Add("@PartySize", SqlDbType.Int).Value = reservation.PartySize;
        _ = command.Parameters.Add("@ReservedForUtc", SqlDbType.DateTimeOffset).Value =
            reservation.ReservedFor;
        _ = command.Parameters.Add("@ContactPhone", SqlDbType.NVarChar, 32).Value =
            reservation.ContactPhone;
        AddNullable(command, "@Notes", SqlDbType.NVarChar, 512, reservation.Notes);
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            reservation.Status.ToString();
        _ = await command.ExecuteNonQueryAsync(cancellationToken);

        return reservation;
    }

    public async Task<IReadOnlyCollection<InventoryItem>> GetInventoryAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT Id, MenuItemId, Name, StockQuantity, Unit
            FROM dbo.FnbInventoryItem
            ORDER BY Name;
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = new List<InventoryItem>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new InventoryItem(
                reader.GetGuid(0),
                reader.GetGuid(1),
                reader.GetString(2),
                reader.GetDecimal(3),
                reader.GetString(4)));
        }

        return result;
    }

    public async Task<IReadOnlyCollection<StockMovement>> DeductInventoryForOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        try
        {
            var existingMovements = await GetStockMovementsForOrderAsync(
                connection,
                transaction,
                orderId,
                StockMovementDirection.Deduction,
                lockRows: true,
                cancellationToken);

            if (existingMovements.Count > 0)
            {
                await transaction.CommitAsync(cancellationToken);
                return existingMovements;
            }

            if (!await OrderExistsAsync(
                    connection,
                    transaction,
                    orderId,
                    lockRow: true,
                    cancellationToken))
            {
                throw new InvalidOperationException("Order not found.");
            }

            var requirements = await GetInventoryRequirementsForOrderAsync(
                connection,
                transaction,
                orderId,
                cancellationToken);

            if (requirements.Count == 0)
            {
                throw new InvalidOperationException("Order has no lines.");
            }

            foreach (var requirement in requirements)
            {
                if (requirement.StockQuantity - requirement.RequiredQuantity < 0)
                {
                    throw new InvalidOperationException(
                        $"Insufficient inventory for '{requirement.Name}'.");
                }
            }

            var occurredAt = DateTimeOffset.UtcNow;
            var movements = new List<StockMovement>();

            foreach (var requirement in requirements)
            {
                await AdjustInventoryAsync(
                    connection,
                    transaction,
                    requirement.InventoryItemId,
                    -requirement.RequiredQuantity,
                    requireSufficientStock: true,
                    cancellationToken);

                var movement = new StockMovement(
                    Guid.NewGuid(),
                    requirement.InventoryItemId,
                    orderId,
                    -requirement.RequiredQuantity,
                    occurredAt,
                    OrderClosedReason);

                await InsertStockMovementAsync(
                    connection,
                    transaction,
                    movement,
                    cancellationToken);
                movements.Add(movement);
            }

            await transaction.CommitAsync(cancellationToken);
            return movements;
        }
        catch (SqlException ex) when (IsUniqueConstraintViolation(ex))
        {
            await RollbackQuietlyAsync(transaction);
            var existingMovements = await GetStockMovementsForOrderAsync(
                orderId,
                StockMovementDirection.Deduction,
                cancellationToken);

            if (existingMovements.Count > 0)
            {
                return existingMovements;
            }

            throw;
        }
    }

    public async Task<IReadOnlyCollection<StockMovement>> RestoreInventoryForOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        try
        {
            var existingRestorations = await GetStockMovementsForOrderAsync(
                connection,
                transaction,
                orderId,
                StockMovementDirection.Restoration,
                lockRows: true,
                cancellationToken);

            if (existingRestorations.Count > 0)
            {
                await transaction.CommitAsync(cancellationToken);
                return existingRestorations;
            }

            var deductedMovements = await GetStockMovementsForOrderAsync(
                connection,
                transaction,
                orderId,
                StockMovementDirection.Deduction,
                lockRows: true,
                cancellationToken);

            var occurredAt = DateTimeOffset.UtcNow;
            var restorations = new List<StockMovement>();

            foreach (var movement in deductedMovements)
            {
                var restoredQuantity = Math.Abs(movement.QuantityChanged);
                await AdjustInventoryAsync(
                    connection,
                    transaction,
                    movement.InventoryItemId,
                    restoredQuantity,
                    requireSufficientStock: false,
                    cancellationToken);

                var restoration = new StockMovement(
                    Guid.NewGuid(),
                    movement.InventoryItemId,
                    orderId,
                    restoredQuantity,
                    occurredAt,
                    InventoryRestoredReason);

                await InsertStockMovementAsync(
                    connection,
                    transaction,
                    restoration,
                    cancellationToken);
                restorations.Add(restoration);
            }

            await transaction.CommitAsync(cancellationToken);
            return restorations;
        }
        catch (SqlException ex) when (IsUniqueConstraintViolation(ex))
        {
            await RollbackQuietlyAsync(transaction);
            var existingRestorations = await GetStockMovementsForOrderAsync(
                orderId,
                StockMovementDirection.Restoration,
                cancellationToken);

            if (existingRestorations.Count > 0)
            {
                return existingRestorations;
            }

            throw;
        }
    }

    public async Task<IReadOnlyCollection<Payment>> GetPaymentsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""
            SELECT {PaymentColumns}
            FROM dbo.FnbPayment
            ORDER BY PaidAtUtc, Id;
            """;

        await using var command = new SqlCommand(sql, connection);
        var result = new List<Payment>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapPayment(reader));
        }

        return result;
    }

    public async Task<Payment> AddPaymentAsync(
        Guid orderId,
        decimal amount,
        PaymentMethod paymentMethod,
        CancellationToken cancellationToken = default)
    {
        if (amount <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(amount),
                "Payment amount must be greater than zero.");
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync(
                IsolationLevel.Serializable,
                cancellationToken);

        try
        {
            var existingPayment = await GetSettledPaymentForOrderAsync(
                connection,
                transaction,
                orderId,
                cancellationToken);

            if (existingPayment is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                return existingPayment;
            }

            if (!await OrderExistsAsync(
                    connection,
                    transaction,
                    orderId,
                    lockRow: true,
                    cancellationToken))
            {
                throw new InvalidOperationException("Order not found.");
            }

            var payment = new Payment(
                Guid.NewGuid(),
                orderId,
                amount,
                paymentMethod,
                PaymentStatus.Settled,
                DateTimeOffset.UtcNow);

            const string sql = """
                INSERT dbo.FnbPayment (Id, OrderId, Amount, Method, Status, PaidAtUtc)
                VALUES (@Id, @OrderId, @Amount, @Method, @Status, @PaidAtUtc);
                """;

            await using (var command = new SqlCommand(sql, connection, transaction))
            {
                _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = payment.Id;
                _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value =
                    payment.OrderId;
                AddMoney(command, "@Amount", payment.Amount);
                _ = command.Parameters.Add("@Method", SqlDbType.NVarChar, 32).Value =
                    payment.Method.ToString();
                _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                    payment.Status.ToString();
                _ = command.Parameters.Add("@PaidAtUtc", SqlDbType.DateTimeOffset).Value =
                    payment.PaidAt;
                _ = await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            return payment;
        }
        catch (SqlException ex) when (IsUniqueConstraintViolation(ex))
        {
            await RollbackQuietlyAsync(transaction);
            var existingPayment = await GetSettledPaymentForOrderAsync(
                orderId,
                cancellationToken);

            if (existingPayment is not null)
            {
                return existingPayment;
            }

            throw;
        }
    }

    public async Task<IReadOnlyCollection<Payment>> RefundPaymentsForOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE dbo.FnbPayment
            SET Status = @RefundedStatus,
                PaidAtUtc = @PaidAtUtc
            OUTPUT inserted.Id, inserted.OrderId, inserted.Amount, inserted.Method,
                   inserted.Status, inserted.PaidAtUtc
            WHERE OrderId = @OrderId
              AND Status = @SettledStatus;
            """;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
        _ = command.Parameters.Add("@RefundedStatus", SqlDbType.NVarChar, 32).Value =
            PaymentStatus.Refunded.ToString();
        _ = command.Parameters.Add("@SettledStatus", SqlDbType.NVarChar, 32).Value =
            PaymentStatus.Settled.ToString();
        _ = command.Parameters.Add("@PaidAtUtc", SqlDbType.DateTimeOffset).Value =
            DateTimeOffset.UtcNow;

        var result = new List<Payment>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapPayment(reader));
        }

        return result;
    }

    private static async Task<IReadOnlyCollection<ServiceOrder>> GetOrdersAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        const string orderSql = """
            SELECT Id, TableId, CreatedAtUtc, Status
            FROM dbo.FnbServiceOrder
            ORDER BY CreatedAtUtc DESC, Id;
            """;

        var builders = new List<ServiceOrderBuilder>();
        var buildersById = new Dictionary<Guid, ServiceOrderBuilder>();
        await using (var command = new SqlCommand(orderSql, connection, transaction))
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var builder = new ServiceOrderBuilder(
                    reader.GetGuid(0),
                    reader.GetGuid(1),
                    reader.GetDateTimeOffset(2),
                    ParseEnum<OrderStatus>(reader.GetString(3), "FnbServiceOrder.Status"));

                builders.Add(builder);
                buildersById.Add(builder.Id, builder);
            }
        }

        if (builders.Count == 0)
        {
            return [];
        }

        const string lineSql = """
            SELECT OrderId, MenuItemId, Quantity, UnitPrice
            FROM dbo.FnbOrderLine
            ORDER BY OrderId, MenuItemId;
            """;

        await using (var command = new SqlCommand(lineSql, connection, transaction))
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!buildersById.TryGetValue(reader.GetGuid(0), out var builder))
                {
                    continue;
                }

                builder.Lines.Add(new OrderLine(
                    reader.GetGuid(1),
                    reader.GetInt32(2),
                    reader.GetDecimal(3)));
            }
        }

        return builders
            .Select(builder => builder.ToOrder())
            .ToArray();
    }

    private static async Task<ServiceOrder?> GetOrderByIdAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid orderId,
        bool lockRow,
        CancellationToken cancellationToken)
    {
        var tableHint = lockRow ? " WITH (UPDLOCK, HOLDLOCK)" : string.Empty;
        var orderSql = $"""
            SELECT Id, TableId, CreatedAtUtc, Status
            FROM dbo.FnbServiceOrder{tableHint}
            WHERE Id = @OrderId;
            """;

        ServiceOrderBuilder? builder = null;
        await using (var command = new SqlCommand(orderSql, connection, transaction))
        {
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                builder = new ServiceOrderBuilder(
                    reader.GetGuid(0),
                    reader.GetGuid(1),
                    reader.GetDateTimeOffset(2),
                    ParseEnum<OrderStatus>(reader.GetString(3), "FnbServiceOrder.Status"));
            }
        }

        if (builder is null)
        {
            return null;
        }

        const string lineSql = """
            SELECT MenuItemId, Quantity, UnitPrice
            FROM dbo.FnbOrderLine
            WHERE OrderId = @OrderId
            ORDER BY MenuItemId;
            """;

        await using (var command = new SqlCommand(lineSql, connection, transaction))
        {
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                builder.Lines.Add(new OrderLine(
                    reader.GetGuid(0),
                    reader.GetInt32(1),
                    reader.GetDecimal(2)));
            }
        }

        return builder.ToOrder();
    }

    private static async Task<DiningTable?> GetTableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid tableId,
        bool lockRow,
        CancellationToken cancellationToken)
    {
        var tableHint = lockRow ? " WITH (UPDLOCK, HOLDLOCK)" : string.Empty;
        var sql = $"""
            SELECT Id, Code, Capacity, Status
            FROM dbo.FnbDiningTable{tableHint}
            WHERE Id = @TableId;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value = tableId;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapTable(reader)
            : null;
    }

    private static async Task MarkTableOccupiedIfAvailableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid tableId,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE dbo.FnbDiningTable
            SET Status = @OccupiedStatus
            WHERE Id = @TableId
              AND Status = @AvailableStatus;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value = tableId;
        _ = command.Parameters.Add("@OccupiedStatus", SqlDbType.NVarChar, 32).Value =
            TableStatus.Occupied.ToString();
        _ = command.Parameters.Add("@AvailableStatus", SqlDbType.NVarChar, 32).Value =
            TableStatus.Available.ToString();
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateTableStatusAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid tableId,
        TableStatus status,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE dbo.FnbDiningTable
            SET Status = @Status
            WHERE Id = @TableId;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value = tableId;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value = status.ToString();
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertOrderLinesAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid orderId,
        IReadOnlyCollection<OrderLine> lines,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT dbo.FnbOrderLine (OrderId, MenuItemId, Quantity, UnitPrice)
            VALUES (@OrderId, @MenuItemId, @Quantity, @UnitPrice);
            """;

        foreach (var line in lines)
        {
            await using var command = new SqlCommand(sql, connection, transaction);
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            _ = command.Parameters.Add("@MenuItemId", SqlDbType.UniqueIdentifier).Value =
                line.MenuItemId;
            _ = command.Parameters.Add("@Quantity", SqlDbType.Int).Value = line.Quantity;
            AddMoney(command, "@UnitPrice", line.UnitPrice);
            _ = await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<bool> OrderExistsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid orderId,
        bool lockRow,
        CancellationToken cancellationToken)
    {
        var tableHint = lockRow ? " WITH (UPDLOCK, HOLDLOCK)" : string.Empty;
        var sql = $"""
            SELECT 1
            FROM dbo.FnbServiceOrder{tableHint}
            WHERE Id = @OrderId;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private static async Task<IReadOnlyCollection<InventoryRequirement>>
        GetInventoryRequirementsForOrderAsync(
            SqlConnection connection,
            SqlTransaction transaction,
            Guid orderId,
            CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT l.MenuItemId, l.Quantity, i.Id, i.Name, i.StockQuantity
            FROM dbo.FnbOrderLine AS l
            LEFT JOIN dbo.FnbInventoryItem AS i WITH (UPDLOCK, HOLDLOCK)
                ON i.MenuItemId = l.MenuItemId
            WHERE l.OrderId = @OrderId
            ORDER BY l.MenuItemId;
            """;

        var rows = new List<InventoryRequirementRow>();
        await using (var command = new SqlCommand(sql, connection, transaction))
        {
            _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new InventoryRequirementRow(
                    reader.GetGuid(0),
                    reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetGuid(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetDecimal(4)));
            }
        }

        foreach (var row in rows)
        {
            if (row.InventoryItemId is null)
            {
                throw new InvalidOperationException("Inventory item mapping not found.");
            }
        }

        return rows
            .GroupBy(row => row.InventoryItemId!.Value)
            .Select(group =>
            {
                var first = group.First();
                return new InventoryRequirement(
                    group.Key,
                    first.Name ?? throw new InvalidOperationException(
                        "Inventory item mapping not found."),
                    first.StockQuantity ?? throw new InvalidOperationException(
                        "Inventory item mapping not found."),
                    group.Sum(row => (decimal)row.Quantity));
            })
            .ToArray();
    }

    private static async Task AdjustInventoryAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        Guid inventoryItemId,
        decimal quantityChanged,
        bool requireSufficientStock,
        CancellationToken cancellationToken)
    {
        var stockPredicate = requireSufficientStock
            ? " AND StockQuantity >= @AbsoluteQuantity"
            : string.Empty;
        var sql = $"""
            UPDATE dbo.FnbInventoryItem
            SET StockQuantity = StockQuantity + @QuantityChanged
            WHERE Id = @InventoryItemId{stockPredicate};
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
            inventoryItemId;
        AddStockQuantity(command, "@QuantityChanged", quantityChanged);
        AddStockQuantity(command, "@AbsoluteQuantity", Math.Abs(quantityChanged));

        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new InvalidOperationException("Insufficient inventory.");
        }
    }

    private static async Task<IReadOnlyCollection<StockMovement>>
        GetStockMovementsForOrderAsync(
            SqlConnection connection,
            SqlTransaction? transaction,
            Guid orderId,
            StockMovementDirection direction,
            bool lockRows,
            CancellationToken cancellationToken)
    {
        var tableHint = lockRows ? " WITH (UPDLOCK, HOLDLOCK)" : string.Empty;
        var comparison = direction is StockMovementDirection.Deduction ? "<" : ">";
        var sql = $"""
            SELECT {StockMovementColumns}
            FROM dbo.FnbStockMovement{tableHint}
            WHERE OrderId = @OrderId
              AND QuantityChanged {comparison} 0
            ORDER BY OccurredAtUtc, Id;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
        var result = new List<StockMovement>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(MapStockMovement(reader));
        }

        return result;
    }

    private async Task<IReadOnlyCollection<StockMovement>> GetStockMovementsForOrderAsync(
        Guid orderId,
        StockMovementDirection direction,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        return await GetStockMovementsForOrderAsync(
            connection,
            transaction: null,
            orderId,
            direction,
            lockRows: false,
            cancellationToken);
    }

    private static async Task InsertStockMovementAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        StockMovement movement,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT dbo.FnbStockMovement (
                Id, InventoryItemId, OrderId, QuantityChanged, OccurredAtUtc, Reason)
            VALUES (
                @Id, @InventoryItemId, @OrderId, @QuantityChanged, @OccurredAtUtc, @Reason);
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@Id", SqlDbType.UniqueIdentifier).Value = movement.Id;
        _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
            movement.InventoryItemId;
        _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value =
            movement.OrderId;
        AddStockQuantity(command, "@QuantityChanged", movement.QuantityChanged);
        _ = command.Parameters.Add("@OccurredAtUtc", SqlDbType.DateTimeOffset).Value =
            movement.OccurredAt;
        _ = command.Parameters.Add("@Reason", SqlDbType.NVarChar, 128).Value = movement.Reason;
        _ = await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<Payment?> GetSettledPaymentForOrderAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        Guid orderId,
        CancellationToken cancellationToken)
    {
        var sql = $"""
            SELECT {PaymentColumns}
            FROM dbo.FnbPayment WITH (UPDLOCK, HOLDLOCK)
            WHERE OrderId = @OrderId
              AND Status = @Status;
            """;

        await using var command = new SqlCommand(sql, connection, transaction);
        _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value = orderId;
        _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
            PaymentStatus.Settled.ToString();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? MapPayment(reader)
            : null;
    }

    private async Task<Payment?> GetSettledPaymentForOrderAsync(
        Guid orderId,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        return await GetSettledPaymentForOrderAsync(
            connection,
            transaction: null,
            orderId,
            cancellationToken);
    }

    private static OrderLine[] NormalizeOrderLines(IReadOnlyCollection<OrderLine> lines)
    {
        if (lines.Count == 0)
        {
            throw new ArgumentException(
                "An order must contain at least one line item.",
                nameof(lines));
        }

        if (lines.Any(line => line.Quantity <= 0))
        {
            throw new ArgumentException(
                "Order line quantity must be greater than zero.",
                nameof(lines));
        }

        return lines
            .GroupBy(line => line.MenuItemId)
            .OrderBy(group => group.Key)
            .Select(group =>
            {
                var unitPrice = group.First().UnitPrice;
                if (group.Any(line => line.UnitPrice != unitPrice))
                {
                    throw new InvalidOperationException(
                        "Order lines for the same menu item must use one unit price.");
                }

                return new OrderLine(
                    group.Key,
                    group.Sum(line => line.Quantity),
                    unitPrice);
            })
            .ToArray();
    }

    private static DiningTable MapTable(SqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetInt32(2),
            ParseEnum<TableStatus>(reader.GetString(3), "FnbDiningTable.Status"));

    private static Payment MapPayment(SqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetGuid(1),
            reader.GetDecimal(2),
            ParseEnum<PaymentMethod>(reader.GetString(3), "FnbPayment.Method"),
            ParseEnum<PaymentStatus>(reader.GetString(4), "FnbPayment.Status"),
            reader.GetDateTimeOffset(5));

    private static StockMovement MapStockMovement(SqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetGuid(1),
            reader.GetGuid(2),
            reader.GetDecimal(3),
            reader.GetDateTimeOffset(4),
            reader.GetString(5));

    private static TEnum ParseEnum<TEnum>(string raw, string columnName)
        where TEnum : struct, Enum
    {
        if (Enum.TryParse<TEnum>(raw, out var value) &&
            Enum.IsDefined(typeof(TEnum), value))
        {
            return value;
        }

        throw new InvalidOperationException(
            $"Database value '{raw}' is not valid for {columnName}.");
    }

    private static void AddMoney(
        SqlCommand command,
        string name,
        decimal value)
    {
        var parameter = command.Parameters.Add(name, SqlDbType.Decimal);
        parameter.Precision = 18;
        parameter.Scale = 2;
        parameter.Value = value;
    }

    private static void AddStockQuantity(
        SqlCommand command,
        string name,
        decimal value)
    {
        var parameter = command.Parameters.Add(name, SqlDbType.Decimal);
        parameter.Precision = 18;
        parameter.Scale = 3;
        parameter.Value = value;
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

    private static bool IsUniqueConstraintViolation(SqlException exception)
    {
        foreach (SqlError error in exception.Errors)
        {
            if (error.Number is 2601 or 2627)
            {
                return true;
            }
        }

        return false;
    }

    private static async Task RollbackQuietlyAsync(SqlTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }
        catch
        {
            // Preserve the original SQL failure.
        }
    }

    private enum StockMovementDirection
    {
        Deduction,
        Restoration
    }

    private sealed record InventoryRequirementRow(
        Guid MenuItemId,
        int Quantity,
        Guid? InventoryItemId,
        string? Name,
        decimal? StockQuantity);

    private sealed record InventoryRequirement(
        Guid InventoryItemId,
        string Name,
        decimal StockQuantity,
        decimal RequiredQuantity);

    private sealed class ServiceOrderBuilder(
        Guid id,
        Guid tableId,
        DateTimeOffset createdAt,
        OrderStatus status)
    {
        public Guid Id { get; } = id;
        public Guid TableId { get; } = tableId;
        public DateTimeOffset CreatedAt { get; } = createdAt;
        public OrderStatus Status { get; } = status;
        public List<OrderLine> Lines { get; } = [];

        public ServiceOrder ToOrder() =>
            new(Id, TableId, CreatedAt, Status, Lines.ToArray());
    }
}
