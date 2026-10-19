namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ViewRetentionTests
{
    [Fact]
    public void AnUnobservedIntermediateViewCannotBeSkippedOrInvented()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var later = ConfigurationHandoffTests.Configuration(2, 1, 2, 3);
        var replica = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        replica.Observe(initial);
        Assert.True(replica.Bootstrap());
        replica.Observe(later);
        Assert.Null(replica.Next);
        Assert.False(replica.Freeze(initial, later));
        Assert.False(replica.CanInstall(initial, later));
        Assert.False(replica.Freeze(initial, ConfigurationHandoffTests.Configuration(1, 1, 2)));
        Assert.False(replica.Allows(initial));
    }

    [Fact]
    public void PendingViewsAreBoundedAndInstalledPrefixesReleaseTheirRetainedMembers()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var first = ConfigurationHandoffTests.Configuration(1, [.. Enumerable.Range(1, 10000)]);
        var second = first with { Version = 2 };
        var replica = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        replica.Observe(initial);
        Assert.True(replica.Bootstrap());
        replica.Observe(first);
        _ = Assert.Throws<InvalidDataException>(() => replica.Observe(second));
        Assert.True(first.Matches(replica.Observed));
        Assert.True(first.Matches(replica.Next));
        Assert.True(replica.Freeze(initial, first));
        replica.Installed(initial, first);
        Assert.Null(replica.Next);
        replica.Observe(second);
        Assert.True(second.Matches(replica.Next));
        Assert.True(replica.Freeze(first, second));
        replica.Installed(first, second);
        Assert.True(replica.Allows(second));
        Assert.Null(replica.Next);
    }
}
