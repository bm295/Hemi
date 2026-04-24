namespace Hemi.Domain;

public sealed record RestaurantProfile(
    string Name,
    string Location,
    int SeatCapacityMinimum,
    int SeatCapacityMaximum);
