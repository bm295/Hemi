namespace Hemi.Domain;

public sealed record MenuItem(
    Guid Id,
    string Name,
    string Category,
    decimal Price,
    bool IsAvailable);
