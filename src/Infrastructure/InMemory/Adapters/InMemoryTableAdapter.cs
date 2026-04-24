using Hemi.Application;
using Hemi.Domain;

namespace Hemi.Infrastructure;

public sealed class InMemoryTableAdapter(InMemoryFnbStore store) : ITableQueryPort
{
    public Task<IReadOnlyCollection<DiningTable>> GetTablesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyCollection<DiningTable>>(store.Tables.AsReadOnly());
}
