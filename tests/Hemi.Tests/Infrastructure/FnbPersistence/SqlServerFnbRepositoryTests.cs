using System.Data;
using System.Text;
using Hemi.Domain;
using Hemi.Infrastructure;
using Microsoft.Data.SqlClient;

namespace Hemi.Tests.Infrastructure.FnbPersistence;

public sealed class SqlServerFnbRepositoryTests
{
    private static readonly Guid SeedTableId =
        Guid.Parse("20000000-0000-0000-0000-000000000001");

    private static readonly Guid SeedMenuItemId =
        Guid.Parse("30000000-0000-0000-0000-000000000001");

    private static readonly Guid SeedInventoryItemId =
        Guid.Parse("40000000-0000-0000-0000-000000000001");

    private static readonly Guid SeedReservationId =
        Guid.Parse("50000000-0000-0000-0000-000000000001");

    [SqlServerFact]
    public async Task Fnb_schema_applies_idempotently_and_seeds_reference_data()
    {
        await using var database = await FnbSqlTestDatabase.CreateAsync();
        await database.ApplySchemaAsync();
        await database.ApplySchemaAsync();

        var paymentColumns = await GetColumnNamesAsync(
            database.ConnectionString,
            "FnbPayment");
        Assert.Contains("OrderId", paymentColumns);
        Assert.Contains("Method", paymentColumns);
        Assert.Contains("Status", paymentColumns);

        var indexes = await GetIndexNamesAsync(database.ConnectionString);
        Assert.Contains("UX_FnbDiningTable_Code", indexes);
        Assert.Contains("UX_FnbPayment_Order_Settled", indexes);
        Assert.Contains("UX_FnbInventoryItem_MenuItemId", indexes);
        Assert.Contains("UX_FnbStockMovement_Order_Inventory_Reason", indexes);

        var repository = new SqlServerFnbRepository(database.ConnectionString);
        var profile = await repository.GetRestaurantProfileAsync();
        var tables = await repository.GetTablesAsync();
        var menu = await repository.GetMenuItemsAsync();
        var inventory = await repository.GetInventoryAsync();
        var reservations = await repository.GetReservationsAsync();

        Assert.Equal("Hemi Steak & Seafood Grill", profile.Name);
        Assert.Equal(60, profile.SeatCapacityMinimum);
        Assert.Equal(80, profile.SeatCapacityMaximum);

        Assert.Contains(
            tables,
            table =>
                table.Id == SeedTableId &&
                table.Code == "T01" &&
                table.Capacity == 4);
        Assert.Contains(
            menu,
            item =>
                item.Id == SeedMenuItemId &&
                item.Name == "USDA Prime Ribeye" &&
                item.IsAvailable);
        Assert.Contains(
            inventory,
            item =>
                item.Id == SeedInventoryItemId &&
                item.MenuItemId == SeedMenuItemId &&
                item.Unit == "portion");
        Assert.Contains(
            reservations,
            reservation =>
                reservation.Id == SeedReservationId &&
                reservation.ReservedFor ==
                    new DateTimeOffset(2030, 1, 15, 19, 0, 0, TimeSpan.Zero));
    }

    [SqlServerFact]
    public async Task Repository_persists_order_payment_and_inventory_mutations_idempotently()
    {
        await using var database = await FnbSqlTestDatabase.CreateAsync();
        var repository = new SqlServerFnbRepository(database.ConnectionString);
        var table = (await repository.GetTablesAsync())
            .Single(item => item.Id == SeedTableId);
        var menuItem = (await repository.GetMenuItemsAsync())
            .Single(item => item.Id == SeedMenuItemId);
        var inventoryItem = (await repository.GetInventoryAsync())
            .Single(item => item.Id == SeedInventoryItemId);
        var startingStock = inventoryItem.StockQuantity;
        ServiceOrder? order = null;

        try
        {
            order = await repository.AddOrderAsync(
                table.Id,
                [new OrderLine(menuItem.Id, 2, menuItem.Price)]);

            var firstPayment = await repository.AddPaymentAsync(
                order.Id,
                order.TotalAmount,
                PaymentMethod.Card);
            var secondPayment = await repository.AddPaymentAsync(
                order.Id,
                order.TotalAmount,
                PaymentMethod.Card);

            Assert.Equal(firstPayment.Id, secondPayment.Id);
            Assert.Single(
                await repository.GetPaymentsAsync(),
                payment =>
                    payment.OrderId == order.Id &&
                    payment.Status is PaymentStatus.Settled);

            var firstDeduction = await repository.DeductInventoryForOrderAsync(order.Id);
            var secondDeduction = await repository.DeductInventoryForOrderAsync(order.Id);

            Assert.Equal(
                firstDeduction.Select(movement => movement.Id),
                secondDeduction.Select(movement => movement.Id));
            Assert.Equal(
                startingStock - order.Lines.Sum(line => line.Quantity),
                await database.GetInventoryQuantityAsync(inventoryItem.Id));

            var firstRestoration = await repository.RestoreInventoryForOrderAsync(order.Id);
            var secondRestoration = await repository.RestoreInventoryForOrderAsync(order.Id);

            Assert.Equal(
                firstRestoration.Select(movement => movement.Id),
                secondRestoration.Select(movement => movement.Id));
            Assert.Equal(
                startingStock,
                await database.GetInventoryQuantityAsync(inventoryItem.Id));

            var completedOrder = await repository.UpdateOrderStatusAsync(
                order.Id,
                OrderStatus.Completed);

            Assert.Equal(OrderStatus.Completed, completedOrder.Status);
            Assert.Equal(
                TableStatus.Available,
                (await repository.GetTablesAsync())
                .Single(item => item.Id == table.Id)
                .Status);
        }
        finally
        {
            await database.SetTableStatusAsync(table.Id, TableStatus.Available);
            await database.SetInventoryQuantityAsync(inventoryItem.Id, startingStock);

            if (order is not null)
            {
                await database.DeleteOrderAsync(order.Id);
            }
        }
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
                OBJECT_ID(N'dbo.FnbDiningTable'),
                OBJECT_ID(N'dbo.FnbMenuItem'),
                OBJECT_ID(N'dbo.FnbServiceOrder'),
                OBJECT_ID(N'dbo.FnbOrderLine'),
                OBJECT_ID(N'dbo.FnbPayment'),
                OBJECT_ID(N'dbo.FnbReservation'),
                OBJECT_ID(N'dbo.FnbInventoryItem'),
                OBJECT_ID(N'dbo.FnbStockMovement'));
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

    private sealed class SqlServerFactAttribute : FactAttribute
    {
        public SqlServerFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(
                    FnbSqlTestDatabase.ConnectionStringFromEnvironment))
            {
                Skip = "Set HEMI_TEST_SQLSERVER_CONNECTION_STRING to run SQL Server FnB integration tests.";
            }
        }
    }

    private sealed class FnbSqlTestDatabase : IAsyncDisposable
    {
        private FnbSqlTestDatabase(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public static string? ConnectionStringFromEnvironment =>
            Environment.GetEnvironmentVariable("HEMI_TEST_SQLSERVER_CONNECTION_STRING");

        public string ConnectionString { get; }

        public static async Task<FnbSqlTestDatabase> CreateAsync()
        {
            var connectionString = ConnectionStringFromEnvironment
                ?? throw new InvalidOperationException(
                    "HEMI_TEST_SQLSERVER_CONNECTION_STRING is required.");

            var database = new FnbSqlTestDatabase(connectionString);
            await database.ApplySchemaAsync();
            return database;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public async Task ApplySchemaAsync()
        {
            var schemaPath = Path.Combine(
                FindRepositoryRoot(),
                "src",
                "Infrastructure",
                "FnbPersistence",
                "Sql",
                "FnbTables.sql");

            var schemaSql = await File.ReadAllTextAsync(schemaPath);

            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var batch in SplitSqlBatches(schemaSql))
            {
                await using var command = new SqlCommand(batch, connection);
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<decimal> GetInventoryQuantityAsync(Guid inventoryItemId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                SELECT StockQuantity
                FROM dbo.FnbInventoryItem
                WHERE Id = @InventoryItemId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
                inventoryItemId;

            var result = await command.ExecuteScalarAsync()
                ?? throw new InvalidOperationException("Inventory item not found.");

            return (decimal)result;
        }

        public async Task SetInventoryQuantityAsync(
            Guid inventoryItemId,
            decimal stockQuantity)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                UPDATE dbo.FnbInventoryItem
                SET StockQuantity = @StockQuantity
                WHERE Id = @InventoryItemId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@InventoryItemId", SqlDbType.UniqueIdentifier).Value =
                inventoryItemId;
            var stockParameter = command.Parameters.Add("@StockQuantity", SqlDbType.Decimal);
            stockParameter.Precision = 18;
            stockParameter.Scale = 3;
            stockParameter.Value = stockQuantity;
            _ = await command.ExecuteNonQueryAsync();
        }

        public async Task SetTableStatusAsync(
            Guid tableId,
            TableStatus status)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            const string sql = """
                UPDATE dbo.FnbDiningTable
                SET Status = @Status
                WHERE Id = @TableId;
                """;

            await using var command = new SqlCommand(sql, connection);
            _ = command.Parameters.Add("@TableId", SqlDbType.UniqueIdentifier).Value = tableId;
            _ = command.Parameters.Add("@Status", SqlDbType.NVarChar, 32).Value =
                status.ToString();
            _ = await command.ExecuteNonQueryAsync();
        }

        public async Task DeleteOrderAsync(Guid orderId)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            foreach (var sql in new[]
            {
                "DELETE FROM dbo.FnbStockMovement WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbPayment WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbOrderLine WHERE OrderId = @OrderId;",
                "DELETE FROM dbo.FnbServiceOrder WHERE Id = @OrderId;"
            })
            {
                await using var command = new SqlCommand(sql, connection);
                _ = command.Parameters.Add("@OrderId", SqlDbType.UniqueIdentifier).Value =
                    orderId;
                _ = await command.ExecuteNonQueryAsync();
            }
        }

        private static IEnumerable<string> SplitSqlBatches(string sql)
        {
            var batch = new StringBuilder();

            foreach (var line in sql.Replace("\r\n", "\n").Split('\n'))
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
