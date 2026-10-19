namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ReplicaCatchUpTests
{
    [Fact]
    public void ARetainedReplicaCanRejoinAfterMissingViewsWithoutReusingLostPromises()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var removed = ConfigurationHandoffTests.Configuration(1, 2, 3);
        var returned = ConfigurationHandoffTests.Configuration(2, 1, 2, 3);
        var replica = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        replica.Observe(initial);
        Assert.True(replica.Bootstrap());
        replica.Observe(removed);
        Assert.False(replica.CanInstall(initial, removed));
        Assert.False(replica.Allows(initial));
        replica.Observe(returned);
        Assert.True(replica.CanInstall(removed, returned));
        replica.Installed(removed, returned);
        Assert.True(replica.Allows(returned));
        var forgotten = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        forgotten.Observe(returned);
        Assert.False(forgotten.CanInstall(initial, returned));
    }
}
