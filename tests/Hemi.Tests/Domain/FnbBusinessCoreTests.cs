using Hemi.Domain;

namespace Hemi.Tests.Domain;

public sealed class FnbBusinessCoreTests
{
    [Fact]
    public void Current_defines_the_canonical_hemi_restaurant_profile()
    {
        var core = HemiFnbCore.Current;

        Assert.Equal("Hemi Steak & Seafood Grill", core.Restaurant.Name);
        Assert.Equal(60, core.Restaurant.SeatCapacityMinimum);
        Assert.Equal(80, core.Restaurant.SeatCapacityMaximum);
    }

    [Fact]
    public void Current_maps_every_requirement_to_a_business_capability()
    {
        var core = HemiFnbCore.Current;
        var mappedRequirementIds = core.Capabilities
            .SelectMany(capability => capability.RequirementIds)
            .ToHashSet(StringComparer.Ordinal);

        Assert.All(
            core.Requirements,
            requirement => Assert.Contains(requirement.Id, mappedRequirementIds));
    }

    [Fact]
    public void CapabilityFor_resolves_the_owner_of_a_requirement()
    {
        var capability = HemiFnbCore.Current.CapabilityFor("REQ-PAY-001");

        Assert.Equal(FnbBusinessArea.PaymentSettlement, capability.Area);
        Assert.Equal("Payment settlement", capability.Name);
    }
}
