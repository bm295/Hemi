using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryRestaurantAdapter(InMemoryFnbStore store) : IRestaurantQueryPort
{
    public Task<RestaurantProfile> GetRestaurantProfileAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(store.Profile);
}
