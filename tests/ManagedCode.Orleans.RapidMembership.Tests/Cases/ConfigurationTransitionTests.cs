namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ConfigurationTransitionTests
{
    [Fact]
    public void ADelayedHandoffCannotBeRetargetedToSkipAnInstalledConfiguration()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var second = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var third = ConfigurationHandoffTests.Configuration(2, 1, 2, 4);
        var replica = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        replica.Observe(initial);
        Assert.True(replica.Bootstrap());
        replica.Observe(second);
        Assert.True(replica.Freeze(initial, second));
        replica.Observe(third);
        Assert.False(replica.Freeze(initial, third));
        Assert.True(replica.Freeze(initial, second));
        Assert.False(replica.Allows(initial));
        Assert.True(replica.CanInstall(initial, second));
        replica.Installed(initial, second);
        Assert.False(replica.Allows(second));
        Assert.True(replica.Freeze(second, third));
        replica.Installed(second, third);
        Assert.True(replica.Allows(third));
        Assert.False(replica.CanInstall(initial, second));
        replica.Observe(third);
        Assert.True(replica.Allows(third));
    }

    [Fact]
    public void AFreshSuccessorCanCatchUpBeforeHelpingTheFollowingTransition()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var second = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var third = ConfigurationHandoffTests.Configuration(2, 1, 2, 4);
        var replica = new ReplicaConfigurationFence(ConfigurationHandoffTests.Member(2).Incarnation);
        replica.Observe(third);
        Assert.True(replica.CanInstall(initial, second));
        replica.Installed(initial, second);
        Assert.False(replica.Allows(second));
        Assert.False(replica.Freeze(initial, third));
        Assert.True(replica.Freeze(second, third));
        Assert.False(replica.CanInstall(initial, third));
        replica.Installed(second, third);
        Assert.True(replica.Allows(third));
        Assert.False(replica.CanInstall(second, third with { ClusterId = 99 }));
    }

    [Fact]
    public void DepartedReplicaCanFreezeItsOldVoteButCannotReenterTheNewQuorum()
    {
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var second = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var third = ConfigurationHandoffTests.Configuration(2, 2, 3, 4);
        var replica = new ReplicaConfigurationFence(initial.Members[0].Incarnation);
        replica.Observe(initial);
        Assert.True(replica.Bootstrap());
        replica.Observe(second);
        Assert.False(replica.Freeze(initial, third));
        Assert.True(replica.Freeze(initial, second));
        replica.Installed(initial, second);
        replica.Observe(third);
        Assert.True(replica.Freeze(second, third));
        Assert.False(replica.CanInstall(second, third));
        Assert.False(replica.Allows(second));
        Assert.False(replica.Allows(third));
    }
}
