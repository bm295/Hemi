using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryReservationAdapter(InMemoryFnbStore store) : IReservationQueryPort, IReservationCommandPort
{
    public Task<IReadOnlyCollection<Reservation>> GetReservationsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<Reservation>>(store.Reservations.AsReadOnly());

    public Task<Reservation> AddReservationAsync(string guestName, int partySize, DateTimeOffset reservedFor, string contactPhone, string? notes, CancellationToken cancellationToken = default)
    {
        var reservation = new Reservation(
            Guid.NewGuid(),
            guestName,
            partySize,
            reservedFor,
            contactPhone,
            notes,
            ReservationStatus.Pending);

        store.Reservations.Add(reservation);
        return Task.FromResult(reservation);
    }
}
