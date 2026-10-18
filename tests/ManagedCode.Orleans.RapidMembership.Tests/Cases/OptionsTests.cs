using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class OptionsTests
{
    [Theory]
    [InlineData("http://localhost:11112")]
    [InlineData("http://0.0.0.0:11112")]
    [InlineData("https://127.0.0.1:11112")]
    [InlineData("http://127.0.0.1:11112/path")]
    [InlineData("http://127.0.0.1:11112?query=1")]
    [InlineData("http://127.0.0.1:11112#fragment")]
    [InlineData("http://user@127.0.0.1:11112")]
    [InlineData("not-an-endpoint")]
    public void NonCanonicalOrUnreachableBindAddressesAreRejected(string seed)
    {
        var options = Valid();
        options.SeedEndpoints = [seed];
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
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
    public void VotersRequireDurableOwnershipAndClientsDoNotRequireAListener()
    {
        var options = Valid();
        options.SeedEndpoints = [options.ListenEndpoint];
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
        options.StorageDirectory = "state";
        options.Validate(server: true);
        options.ListenEndpoint = "unused-by-client";
        options.Validate(server: false);
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: true));
    }

    [Fact]
    public void DuplicateVotersWeakSecretsAndInvalidDeadlinesAreRejected()
    {
        var options = Valid();
        options.SeedEndpoints = [options.SeedEndpoints[0], options.SeedEndpoints[0]];
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.SharedSecret = "short";
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.OperationTimeout = TimeSpan.Zero;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.PeerTimeout = options.OperationTimeout.Add(TimeSpan.FromSeconds(1));
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.MaximumAdmissions = 0;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
        options = Valid();
        options.MaximumEntries = 0;
        _ = Assert.Throws<OptionsValidationException>(() => options.Validate(server: false));
    }

    [Fact]
    public void ClusterIdentitySeparatesServiceClusterAndVoterConfiguration()
    {
        var options = Valid();
        var identity = options.Identity(new ClusterOptions { ServiceId = "ab", ClusterId = "c" });
        Assert.NotEqual(identity, options.Identity(new ClusterOptions { ServiceId = "a", ClusterId = "bc" }));
        options.SeedEndpoints = ["http://127.0.0.3:11112"];
        Assert.NotEqual(identity, options.Identity(new ClusterOptions { ServiceId = "ab", ClusterId = "c" }));
    }

    private static RapidMembershipOptions Valid() => new()
    {
        SeedEndpoints = ["http://127.0.0.2:11112"],
        SharedSecret = "0123456789abcdef0123456789abcdef",
    };
}
