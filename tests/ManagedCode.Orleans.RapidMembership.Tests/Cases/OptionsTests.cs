using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class OptionsTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(65536)]
    public void InvalidListenerPortsAreRejectedForSilos(int port)
    {
        var options = Valid();
        options.ListenPort = port;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
        options.Validate(server: false);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(4095)]
    [InlineData(67108865)]
    public void InvalidSnapshotBoundsAreRejectedBeforeStartup(int bytes)
    {
        var options = Valid();
        options.MaximumSnapshotBytes = bytes;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65537)]
    public void InvalidOperationHistoryBoundsAreRejectedBeforeStartup(int count)
    {
        var options = Valid();
        options.MaximumOperationHistory = count;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
    }

    [Fact]
    public void AutomaticDefaultsRequireNoAddressListOrDirectory()
    {
        var options = Valid();
        options.Validate(server: true);
        options.Validate(server: false);
        options.ListenPort = 0;
        options.DiscoveryNetworks = ["127.0.0.1/32", "10.1.2.4-10.1.2.10"];
        options.Validate(server: true);
    }

    [Fact]
    public void WeakSecretsAndInvalidDeadlinesOrMemoryBoundsAreRejected()
    {
        var options = Valid();
        options.SharedSecret = "short";
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.OperationTimeout = TimeSpan.Zero;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.StartupTimeout = TimeSpan.Zero;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
        options = Valid();
        options.StartupTimeout = TimeSpan.FromMinutes(16);
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
        options = Valid();
        options.PeerTimeout = options.OperationTimeout.Add(TimeSpan.FromSeconds(1));
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.MaximumAdmissions = 0;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.MaximumEntries = 0;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.MaximumExchangeBytes = options.MaximumSnapshotBytes;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
    }

    [Fact]
    public void ClusterIdentitySeparatesServiceAndClusterAndDoesNotDependOnSearchNetworks()
    {
        var identity = RapidMembershipOptions.Identity(new ClusterOptions { ServiceId = "ab", ClusterId = "c" });
        Assert.NotEqual(identity, RapidMembershipOptions.Identity(new ClusterOptions { ServiceId = "a", ClusterId = "bc" }));
        Assert.NotEqual(identity, RapidMembershipOptions.Identity(new ClusterOptions { ServiceId = "ab", ClusterId = "d" }));
        Assert.Equal(identity, RapidMembershipOptions.Identity(new ClusterOptions { ServiceId = "ab", ClusterId = "c" }));
    }

    private static RapidMembershipOptions Valid() => new() { SharedSecret = "0123456789abcdef0123456789abcdef" };
}
