using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryMenuAdapter(InMemoryFnbStore store) : IMenuQueryPort
{
    public Task<IReadOnlyCollection<MenuItem>> GetMenuItemsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<MenuItem>>(store.MenuItems.AsReadOnly());
}
