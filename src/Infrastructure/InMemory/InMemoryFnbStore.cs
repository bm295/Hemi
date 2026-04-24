using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryFnbStore
{
    public RestaurantProfile Profile { get; } = new(
        "Hemi Steak & Seafood Grill",
        "Not specified",
        60,
        80);

    public List<DiningTable> Tables { get; } =
    [
        new(Guid.NewGuid(), "T01", 4, TableStatus.Available),
        new(Guid.NewGuid(), "T02", 4, TableStatus.Available),
        new(Guid.NewGuid(), "T03", 4, TableStatus.Reserved),
        new(Guid.NewGuid(), "T04", 4, TableStatus.Occupied),
        new(Guid.NewGuid(), "T05", 6, TableStatus.Available),
        new(Guid.NewGuid(), "T06", 6, TableStatus.Available),
        new(Guid.NewGuid(), "T07", 6, TableStatus.Reserved),
        new(Guid.NewGuid(), "T08", 8, TableStatus.Available),
        new(Guid.NewGuid(), "T09", 8, TableStatus.Occupied),
        new(Guid.NewGuid(), "T10", 8, TableStatus.Available),
        new(Guid.NewGuid(), "P01", 10, TableStatus.Available),
        new(Guid.NewGuid(), "P02", 10, TableStatus.Reserved)
    ];

    public List<MenuItem> MenuItems { get; } =
    [
        new(Guid.NewGuid(), "USDA Prime Ribeye", "Steak", 1350000m, true),
        new(Guid.NewGuid(), "Grilled Lobster Tail", "Seafood", 1490000m, true),
        new(Guid.NewGuid(), "Pan-Seared Salmon", "Seafood", 690000m, true),
        new(Guid.NewGuid(), "Wagyu Beef Carpaccio", "Starter", 480000m, true),
        new(Guid.NewGuid(), "Caesar Salad", "Starter", 260000m, true),
        new(Guid.NewGuid(), "Chocolate Lava Cake", "Dessert", 240000m, true),
        new(Guid.NewGuid(), "Seasonal Oyster Platter", "Seafood", 820000m, false)
    ];

    public List<ServiceOrder> Orders { get; } = [];

    public List<Reservation> Reservations { get; } =
    [
        new(Guid.NewGuid(), "Nguyen Minh Anh", 4, DateTimeOffset.UtcNow.AddHours(2), "0901234567", "Window-side table", ReservationStatus.Confirmed),
        new(Guid.NewGuid(), "Tran Hoang Long", 6, DateTimeOffset.UtcNow.AddHours(4), "0912345678", null, ReservationStatus.Pending)
    ];

    public List<Payment> Payments { get; } = [];
    public List<StockMovement> StockMovements { get; } = [];
    public List<InventoryItem> Inventory { get; }

    public InMemoryFnbStore()
    {
        Inventory = MenuItems
            .Select(item => new InventoryItem(Guid.NewGuid(), item.Id, item.Name, 100, "portion"))
            .ToList();
    }
}
