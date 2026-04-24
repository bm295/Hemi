namespace Hemi.Domain;

public enum TableStatus
{
    Available,
    Occupied,
    Reserved
}

public sealed record DiningTable(
    Guid Id,
    string Code,
    int Capacity,
    TableStatus Status);
