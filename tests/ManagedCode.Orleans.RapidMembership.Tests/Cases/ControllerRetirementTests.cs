namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ControllerRetirementTests
{
    [Fact]
    public void ACoordinatorCannotRetireWhileDepartedMembersRemainInItsNativeView()
    {
        var configuration = ConfigurationHandoffTests.Configuration(3, 1, 2, 3, 4, 5, 6);
        var replies = Installed(configuration);
        var highest = configuration.Members[^1].Incarnation;
        Assert.True(ControllerRuntime.CanRetire(configuration, highest, replies));
        Assert.False(ControllerRuntime.CanRetire(configuration, highest, [.. replies.Skip(1)]));
        replies[0].IsDeparting = true;
        Assert.False(ControllerRuntime.CanRetire(configuration, highest, replies));
        replies[0].IsDeparting = false;
        Assert.True(ControllerRuntime.CanRetire(configuration, highest, replies));
    }

    [Fact]
    public void AnIncompleteTableHandoffCannotTriggerSurplusRetirement()
    {
        var configuration = ConfigurationHandoffTests.Configuration(3, 1, 2, 3, 4);
        var replies = Installed(configuration);
        var highest = configuration.Members[^1].Incarnation;
        replies[0].Configuration = configuration with { Version = 2 };
        Assert.False(ControllerRuntime.CanRetire(configuration, highest, replies));
        replies[0].Configuration = configuration;
        replies[0].Granted = false;
        Assert.False(ControllerRuntime.CanRetire(configuration, highest, replies));
        replies[0].Granted = true;
        Assert.True(ControllerRuntime.CanRetire(configuration, highest, replies));
    }

    [Fact]
    public void AStableCohortRetiresOnlyItsHighestSurplusIncarnation()
    {
        var configuration = ConfigurationHandoffTests.Configuration(3, 1, 2, 3, 4);
        var replies = Installed(configuration);
        Assert.False(ControllerRuntime.CanRetire(configuration, configuration.Members[0].Incarnation, replies));
        Assert.True(ControllerRuntime.CanRetire(configuration, configuration.Members[^1].Incarnation, replies));
        var required = ConfigurationHandoffTests.Configuration(4, 1, 2, 3);
        Assert.False(ControllerRuntime.CanRetire(required, required.Members[^1].Incarnation, Installed(required)));
    }

    private static PeerReply[] Installed(CoordinationConfiguration configuration)
        => [.. configuration.Members.Select(member => new PeerReply
        {
            Incarnation = member.Incarnation,
            Configuration = configuration,
            Granted = true,
        })];
}
