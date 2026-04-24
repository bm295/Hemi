using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryInventoryAdapter(InMemoryFnbStore store) : IInventoryQueryPort, IInventoryCommandPort
{
    public Task<IReadOnlyCollection<InventoryItem>> GetInventoryAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<InventoryItem>>(store.Inventory.AsReadOnly());

    public Task<IReadOnlyCollection<StockMovement>> DeductInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var order = store.Orders.SingleOrDefault(x => x.Id == orderId)
            ?? throw new InvalidOperationException("Order not found.");

        var movements = new List<StockMovement>();

        foreach (var line in order.Lines)
        {
            var inventoryItem = store.Inventory.SingleOrDefault(x => x.MenuItemId == line.MenuItemId)
                ?? throw new InvalidOperationException("Inventory item mapping not found.");

            var remaining = inventoryItem.StockQuantity - line.Quantity;
            if (remaining < 0)
            {
                throw new InvalidOperationException($"Insufficient inventory for '{inventoryItem.Name}'.");
            }

            var movement = new StockMovement(
                Guid.NewGuid(),
                inventoryItem.Id,
                orderId,
                -line.Quantity,
                DateTimeOffset.UtcNow,
                "Order Closed");

            movements.Add(movement);
            store.StockMovements.Add(movement);

            store.Inventory[store.Inventory.IndexOf(inventoryItem)] = inventoryItem with { StockQuantity = remaining };
        }

        return Task.FromResult<IReadOnlyCollection<StockMovement>>(movements.AsReadOnly());
    }

    public Task<IReadOnlyCollection<StockMovement>> RestoreInventoryForOrderAsync(Guid orderId, CancellationToken cancellationToken = default)
    {
        var deductedMovements = store.StockMovements
            .Where(x => x.OrderId == orderId && x.QuantityChanged < 0)
            .ToArray();

        var restorations = new List<StockMovement>();
        foreach (var movement in deductedMovements)
        {
            var inventoryItem = store.Inventory.Single(x => x.Id == movement.InventoryItemId);
            var restoredQuantity = inventoryItem.StockQuantity + Math.Abs(movement.QuantityChanged);

            store.Inventory[store.Inventory.IndexOf(inventoryItem)] = inventoryItem with { StockQuantity = restoredQuantity };

            var restoreMovement = new StockMovement(
                Guid.NewGuid(),
                movement.InventoryItemId,
                orderId,
                Math.Abs(movement.QuantityChanged),
                DateTimeOffset.UtcNow,
                "Saga Compensation: Inventory Restored");

            store.StockMovements.Add(restoreMovement);
            restorations.Add(restoreMovement);
        }

        return Task.FromResult<IReadOnlyCollection<StockMovement>>(restorations.AsReadOnly());
    }
}
