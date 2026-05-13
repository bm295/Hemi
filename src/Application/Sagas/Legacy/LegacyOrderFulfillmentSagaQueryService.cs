using Hemi.Application;

namespace Hemi.Application.Sagas.Legacy;

public sealed class LegacyOrderFulfillmentSagaQueryService(
    ISagaStateQueryPort sagaStateQueryPort)
{
    public Task<OrderFulfillmentSagaState?> GetSagaStateAsync(
        Guid orderId,
        CancellationToken cancellationToken = default) =>
        sagaStateQueryPort.GetOrderFulfillmentSagaAsync(
            orderId,
            cancellationToken);
}
