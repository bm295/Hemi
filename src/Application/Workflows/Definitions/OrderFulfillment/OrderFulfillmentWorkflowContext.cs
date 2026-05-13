using System.Globalization;
using System.Text.Json;
using Hemi.Application.Workflows.Execution;
using Hemi.Domain;

namespace Hemi.Application.Workflows.Definitions.OrderFulfillment;

public static class OrderFulfillmentWorkflowContext
{
    public const string OrderId = "order.id";

    public const string PaymentMethod = "payment.method";

    public const string PaymentAmount = "payment.amount";

    public const string OriginalOrderStatus = "order.originalStatus";

    public const string KitchenStatusChanged = "kitchen.statusChanged";

    public const string PaymentCreated = "payment.created";

    public const string PaymentId = "payment.id";

    public const string InventoryDeducted = "inventory.deducted";

    public const string InventoryRestored = "inventory.restored";

    public const string InventoryMovementIds = "inventory.movementIds";

    public const string OrderClosed = "order.closed";

    public static Guid GetOrderId(WorkflowContext context) =>
        GetGuid(context, OrderId);

    public static PaymentMethod GetPaymentMethod(WorkflowContext context) =>
        GetPaymentMethod(context, PaymentMethod);

    public static decimal? GetPaymentAmount(WorkflowContext context)
    {
        if (!TryGetRaw(context, PaymentAmount, out var rawValue))
        {
            return null;
        }

        return rawValue switch
        {
            decimal value => value,
            double value => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            float value => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
            int value => value,
            long value => value,
            string value when decimal.TryParse(
                value,
                NumberStyles.Number,
                CultureInfo.InvariantCulture,
                out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.Number } value
                when value.TryGetDecimal(out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.Null } => null,
            _ => throw new InvalidOperationException(
                $"Workflow context item '{PaymentAmount}' is not a decimal.")
        };
    }

    public static bool GetFlag(
        WorkflowContext context,
        string key) =>
        TryGetRaw(context, key, out var rawValue) &&
        rawValue switch
        {
            bool value => value,
            string value when bool.TryParse(value, out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.True } => true,
            JsonElement { ValueKind: JsonValueKind.False } => false,
            _ => false
        };

    public static Guid? GetOptionalGuid(
        WorkflowContext context,
        string key) =>
        TryGetRaw(context, key, out _) ? GetGuid(context, key) : null;

    public static OrderStatus? GetOriginalOrderStatus(
        WorkflowContext context)
    {
        if (!TryGetRaw(context, OriginalOrderStatus, out var rawValue))
        {
            return null;
        }

        return rawValue switch
        {
            OrderStatus value => value,
            string value when Enum.TryParse<OrderStatus>(
                value,
                ignoreCase: true,
                out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.String } value
                when Enum.TryParse<OrderStatus>(
                    value.GetString(),
                    ignoreCase: true,
                    out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.Number } value =>
                ToOrderStatus(value.GetInt32()),
            _ => throw new InvalidOperationException(
                $"Workflow context item '{OriginalOrderStatus}' is not an order status.")
        };
    }

    private static Guid GetGuid(
        WorkflowContext context,
        string key)
    {
        if (!TryGetRaw(context, key, out var rawValue))
        {
            throw new InvalidOperationException(
                $"Workflow context item '{key}' was not found.");
        }

        return rawValue switch
        {
            Guid value => value,
            string value when Guid.TryParse(value, out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.String } value
                when value.TryGetGuid(out var parsed) => parsed,
            _ => throw new InvalidOperationException(
                $"Workflow context item '{key}' is not a GUID.")
        };
    }

    private static PaymentMethod GetPaymentMethod(
        WorkflowContext context,
        string key)
    {
        if (!TryGetRaw(context, key, out var rawValue))
        {
            throw new InvalidOperationException(
                $"Workflow context item '{key}' was not found.");
        }

        return rawValue switch
        {
            PaymentMethod value => value,
            string value when Enum.TryParse<PaymentMethod>(
                value,
                ignoreCase: true,
                out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.String } value
                when Enum.TryParse<PaymentMethod>(
                    value.GetString(),
                    ignoreCase: true,
                    out var parsed) => parsed,
            JsonElement { ValueKind: JsonValueKind.Number } value =>
                ToPaymentMethod(value.GetInt32()),
            _ => throw new InvalidOperationException(
                $"Workflow context item '{key}' is not a payment method.")
        };
    }

    private static bool TryGetRaw(
        WorkflowContext context,
        string key,
        out object? rawValue) =>
        context.Items.TryGetValue(key, out rawValue) &&
        rawValue is not null;

    private static OrderStatus ToOrderStatus(int value)
    {
        var status = (OrderStatus)value;
        return Enum.IsDefined(typeof(OrderStatus), status)
            ? status
            : throw new InvalidOperationException(
                $"Workflow context item '{OriginalOrderStatus}' is not an order status.");
    }

    private static PaymentMethod ToPaymentMethod(int value)
    {
        var method = (PaymentMethod)value;
        return Enum.IsDefined(typeof(PaymentMethod), method)
            ? method
            : throw new InvalidOperationException(
                $"Workflow context item '{PaymentMethod}' is not a payment method.");
    }
}
