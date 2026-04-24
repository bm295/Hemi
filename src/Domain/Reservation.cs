namespace Hemi.Domain;

public enum ReservationStatus
{
    Pending,
    Confirmed,
    Seated,
    Cancelled
}

public sealed record Reservation(
    Guid Id,
    string GuestName,
    int PartySize,
    DateTimeOffset ReservedFor,
    string ContactPhone,
    string? Notes,
    ReservationStatus Status);
