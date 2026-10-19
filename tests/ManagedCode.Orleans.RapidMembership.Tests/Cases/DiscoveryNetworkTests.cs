using System.Net;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class DiscoveryNetworkTests
{
    [Theory]
    [InlineData("192.168.10.0/30", "192.168.10.1", "192.168.10.2")]
    [InlineData("10.2.3.4/31", "10.2.3.4", "10.2.3.5")]
    [InlineData("127.0.0.1/32", "127.0.0.1", "127.0.0.1")]
    [InlineData("10.20.30.10-10.20.30.12", "10.20.30.10", "10.20.30.12")]
    public void CidrAndInclusiveRangesEnumerateOnlyTheConfiguredAddresses(string network, string first, string last)
    {
        var addresses = DiscoveryAddressRange.Expand([network], 16);
        Assert.Equal(IPAddress.Parse(first), addresses[0]);
        Assert.Equal(IPAddress.Parse(last), addresses[^1]);
        Assert.Equal(addresses.Count, addresses.Distinct().Count());
    }

    [Theory]
    [InlineData("10.0.0.0/8")]
    [InlineData("0.0.0.0/0")]
    [InlineData("10.0.0.2-10.0.0.1")]
    [InlineData("10.0.0.0/33")]
    [InlineData("10.0.0.0/-1")]
    [InlineData("10.0.0.0/word")]
    [InlineData("localhost/24")]
    [InlineData("::1/32")]
    [InlineData("not-a-network")]
    public void InvalidOrExcessiveNetworksFailBeforeAnySocketOpens(string network)
    {
        _ = Assert.Throws<ArgumentException>(() => DiscoveryAddressRange.Expand([network], 32));
        Assert.Equal(IPAddress.Loopback, Assert.Single(DiscoveryAddressRange.Expand(["127.0.0.1/32"], 1)));
    }

    [Fact]
    public void CombinedRangesAreDeduplicatedAndBounded()
    {
        Assert.Equal(3, DiscoveryAddressRange.Expand(["10.0.0.1-10.0.0.2", "10.0.0.2-10.0.0.3"], 3).Count);
        _ = Assert.Throws<ArgumentException>(() => DiscoveryAddressRange.Expand(["10.0.0.1-10.0.0.2", "10.0.0.3-10.0.0.4"], 3));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => DiscoveryAddressRange.Expand([], 0));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => DiscoveryAddressRange.Expand([], 65537));
    }
}
