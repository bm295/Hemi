namespace Hemi.Domain;

public enum FnbBusinessArea
{
    RestaurantOperations,
    FrontOfHouse,
    MenuCatalog,
    OrderManagement,
    KitchenOperations,
    PaymentSettlement,
    InventoryControl,
    ReservationManagement,
    ReportingAnalytics,
    ExternalIntegrations,
    WorkflowReliability
}

public enum FnbRequirementPriority
{
    Must,
    Should,
    Could
}

public sealed record FnbBusinessRequirement(
    string Id,
    FnbBusinessArea Area,
    FnbRequirementPriority Priority,
    string Requirement,
    string AcceptanceCriterion);

public sealed record FnbBusinessCapability(
    string Id,
    FnbBusinessArea Area,
    string Name,
    string Purpose,
    IReadOnlyCollection<string> RequirementIds);

public sealed record FnbServicePolicy(
    string Id,
    string Name,
    string Rule);

public sealed record FnbBusinessCore(
    RestaurantProfile Restaurant,
    IReadOnlyCollection<FnbBusinessCapability> Capabilities,
    IReadOnlyCollection<FnbBusinessRequirement> Requirements,
    IReadOnlyCollection<FnbServicePolicy> Policies)
{
    public FnbBusinessCapability CapabilityFor(string requirementId) =>
        Capabilities.Single(capability => capability.RequirementIds.Contains(requirementId, StringComparer.Ordinal));
}

public static class HemiFnbCore
{
    public const string RestaurantName = "Hemi Steak & Seafood Grill";
    public const int MinimumSeats = 60;
    public const int MaximumSeats = 80;

    public static FnbBusinessCore Current { get; } = new(
        new RestaurantProfile(RestaurantName, "Not specified", MinimumSeats, MaximumSeats),
        BuildCapabilities(),
        BuildRequirements(),
        BuildPolicies());

    private static IReadOnlyCollection<FnbBusinessCapability> BuildCapabilities() =>
    [
        new("cap.restaurant-profile", FnbBusinessArea.RestaurantOperations, "Restaurant profile", "Publishes the canonical venue identity, location, and target capacity for downstream modules.", ["REQ-OPS-001"]),
        new("cap.table-service", FnbBusinessArea.FrontOfHouse, "Table and seating service", "Tracks dining-table capacity and service status across available, occupied, and reserved states.", ["REQ-FOH-001"]),
        new("cap.menu-catalog", FnbBusinessArea.MenuCatalog, "Menu catalog", "Provides priced, categorized, availability-filtered menu items for staff and channels.", ["REQ-MENU-001", "REQ-MENU-002"]),
        new("cap.order-lifecycle", FnbBusinessArea.OrderManagement, "Order lifecycle", "Creates, edits, sends, completes, and cancels service orders using menu-resolved prices.", ["REQ-ORDER-001", "REQ-ORDER-002", "REQ-ORDER-003"]),
        new("cap.kitchen-fulfillment", FnbBusinessArea.KitchenOperations, "Kitchen fulfillment", "Coordinates kitchen handoff and order completion through durable fulfillment workflows.", ["REQ-KITCHEN-001", "REQ-WORKFLOW-001"]),
        new("cap.payment-settlement", FnbBusinessArea.PaymentSettlement, "Payment settlement", "Validates tendered amount, records settled payments, and supports order refund compensation.", ["REQ-PAY-001", "REQ-PAY-002"]),
        new("cap.inventory-control", FnbBusinessArea.InventoryControl, "Inventory control", "Deducts and restores stock against order fulfillment with retry-safe stock movements.", ["REQ-INV-001", "REQ-INV-002"]),
        new("cap.reservation-book", FnbBusinessArea.ReservationManagement, "Reservation book", "Captures guest, party-size, contact, timing, notes, and status for upcoming bookings.", ["REQ-RES-001", "REQ-RES-002"]),
        new("cap.sales-reporting", FnbBusinessArea.ReportingAnalytics, "Sales reporting", "Summarizes settled revenue, payment count, and closed paid orders for a reporting window.", ["REQ-RPT-001"]),
        new("cap.channel-orders", FnbBusinessArea.ExternalIntegrations, "External channel orders", "Normalizes food-app orders into internal table orders while preserving source and external identifiers.", ["REQ-INT-001"]),
        new("cap.workflow-reliability", FnbBusinessArea.WorkflowReliability, "Workflow reliability", "Applies idempotent starts, correlation protection, persisted steps, leases, outbox delivery, retry, and compensation.", ["REQ-WORKFLOW-001", "REQ-WORKFLOW-002", "REQ-WORKFLOW-003"])
    ];

    private static IReadOnlyCollection<FnbBusinessRequirement> BuildRequirements() =>
    [
        new("REQ-OPS-001", FnbBusinessArea.RestaurantOperations, FnbRequirementPriority.Must, "The core module must expose the canonical restaurant profile for Hemi Steak & Seafood Grill.", "Profile returns the restaurant name, location placeholder when unknown, and 60-80 seat target."),
        new("REQ-FOH-001", FnbBusinessArea.FrontOfHouse, FnbRequirementPriority.Must, "The core module must manage dining table capacity and status.", "Tables include code, capacity, and Available/Occupied/Reserved status."),
        new("REQ-MENU-001", FnbBusinessArea.MenuCatalog, FnbRequirementPriority.Must, "The core module must manage menu items by category, name, price, and availability.", "Only available items are returned to order-entry use cases."),
        new("REQ-MENU-002", FnbBusinessArea.MenuCatalog, FnbRequirementPriority.Must, "Order pricing must be resolved from the menu catalog instead of caller-supplied prices.", "Created and appended order lines use menu item unit prices."),
        new("REQ-ORDER-001", FnbBusinessArea.OrderManagement, FnbRequirementPriority.Must, "Orders must contain at least one positive-quantity line.", "Creating or updating an order rejects empty or non-positive line quantities."),
        new("REQ-ORDER-002", FnbBusinessArea.OrderManagement, FnbRequirementPriority.Must, "Only open orders may be edited.", "Adding or removing line items from non-open orders is rejected."),
        new("REQ-ORDER-003", FnbBusinessArea.OrderManagement, FnbRequirementPriority.Must, "Orders must move through explicit service states.", "The supported lifecycle is Open, SentToKitchen, Completed, and Cancelled."),
        new("REQ-KITCHEN-001", FnbBusinessArea.KitchenOperations, FnbRequirementPriority.Must, "Open orders must be sendable to kitchen fulfillment.", "Only open orders can transition to SentToKitchen."),
        new("REQ-PAY-001", FnbBusinessArea.PaymentSettlement, FnbRequirementPriority.Must, "Payments must settle only positive amounts that cover the order total.", "Payments with non-positive or insufficient amounts are rejected."),
        new("REQ-PAY-002", FnbBusinessArea.PaymentSettlement, FnbRequirementPriority.Should, "Payment side effects should be retry-safe.", "Repeated settlement for the same order returns the existing settled payment."),
        new("REQ-INV-001", FnbBusinessArea.InventoryControl, FnbRequirementPriority.Must, "Completing an order must deduct inventory after settlement.", "Close order requires settled payment, deducts stock, then marks the order completed."),
        new("REQ-INV-002", FnbBusinessArea.InventoryControl, FnbRequirementPriority.Should, "Inventory fulfillment and restoration should be retry-safe.", "Repeated deduction or restoration returns existing stock movements without duplicate quantity changes."),
        new("REQ-RES-001", FnbBusinessArea.ReservationManagement, FnbRequirementPriority.Must, "Reservations must capture guest details, party size, reserved time, phone, notes, and status.", "Creating a reservation rejects non-positive party sizes."),
        new("REQ-RES-002", FnbBusinessArea.ReservationManagement, FnbRequirementPriority.Should, "Upcoming reservations must exclude cancelled reservations.", "Upcoming reservation queries return future non-cancelled reservations sorted by time."),
        new("REQ-RPT-001", FnbBusinessArea.ReportingAnalytics, FnbRequirementPriority.Should, "Sales reports must be based on settled payments within a requested window.", "Reports include total revenue, settled payment count, and completed paid order count."),
        new("REQ-INT-001", FnbBusinessArea.ExternalIntegrations, FnbRequirementPriority.Should, "Food-app orders must be normalized into internal service orders.", "Integration requires source app, external order id, known table code, and at least one item."),
        new("REQ-WORKFLOW-001", FnbBusinessArea.WorkflowReliability, FnbRequirementPriority.Must, "Order fulfillment must run through a durable workflow.", "Fulfillment records persisted instance state and step attempts."),
        new("REQ-WORKFLOW-002", FnbBusinessArea.WorkflowReliability, FnbRequirementPriority.Must, "Workflow starts must be idempotent by request key and protected by correlation.", "Repeated equivalent starts reuse the same workflow response; conflicting starts are rejected."),
        new("REQ-WORKFLOW-003", FnbBusinessArea.WorkflowReliability, FnbRequirementPriority.Should, "Workflow side effects must publish through a durable outbox.", "Lifecycle events are persisted before broker delivery and retried until terminal outcome.")
    ];

    private static IReadOnlyCollection<FnbServicePolicy> BuildPolicies() =>
    [
        new("POL-CAPACITY", "Capacity target", "The restaurant operating model is sized for 60-80 seats until a concrete floor plan overrides it."),
        new("POL-PRICE-SOURCE", "Menu is price authority", "All order totals are computed from catalog prices, never caller-supplied totals."),
        new("POL-PAYMENT-BEFORE-CLOSE", "Payment before close", "An order cannot complete until at least one settled payment exists for that order."),
        new("POL-IDEMPOTENCY", "Retry-safe side effects", "Payment, inventory, and workflow side effects must be safe to retry under the same business key.")
    ];
}
