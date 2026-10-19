using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class SameHostTransportTests
{
    [Fact]
    public async Task ALocalInterfaceAddressReachesTheSameRecipientThroughItsLoopbackListener()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        var address = NetworkInterface.GetAllNetworkInterfaces().Where(network => network.OperationalStatus == OperationalStatus.Up)
            .SelectMany(network => network.GetIPProperties().UnicastAddresses)
            .Select(value => value.Address).First(value => value.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(value));
        await using var network = await ReplicaNetwork.StartAsync(token);
        var original = network.Members[0];
        await network.Replicas.BootstrapAsync(new CoordinationConfiguration(42, 0, [original]));
        Assert.True(await network.Transport.SendAsync(original, new PeerRequest { Action = PeerAction.Locate }, token) is { Granted: true });
        var endpoint = new UriBuilder(original.Endpoint) { Host = address.ToString() }.Uri.GetLeftPart(UriPartial.Authority);
        var local = new CoordinationMember(original.Incarnation, endpoint);
        var observed = await network.Transport.SendAsync(local, new PeerRequest { Action = PeerAction.Locate }, token);
        Assert.True(observed is { Granted: true });
        Assert.Equal(original.Incarnation, observed.Incarnation);
        Assert.Null(await network.Transport.SendAsync(new CoordinationMember(Guid.NewGuid(), endpoint),
            new PeerRequest { Action = PeerAction.Locate }, token));
        Assert.Equal(original.Incarnation, (await network.Transport.SendAsync(original,
            new PeerRequest { Action = PeerAction.Locate }, token))!.Incarnation);
    }

    [Fact]
    public void RemoteDnsIpv6AndTlsAuthoritiesArePreserved()
    {
        var local = NetworkInterface.GetAllNetworkInterfaces().Where(network => network.OperationalStatus == OperationalStatus.Up)
            .SelectMany(network => network.GetIPProperties().UnicastAddresses)
            .Select(value => value.Address).First(value => value.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(value));
        Assert.False(global::RapidCluster.Grpc.LocalPeerAddress.IsAssigned(IPAddress.Parse("203.0.113.254")));
        Uri[] endpoints =
        [
            new("http://203.0.113.254:11112"),
            new("http://peer.example:11112"),
            new("http://[::1]:11112"),
            new UriBuilder(Uri.UriSchemeHttps, local.ToString(), 11112).Uri,
        ];
        Assert.All(endpoints, endpoint => Assert.Same(endpoint, global::RapidCluster.Grpc.LocalPeerAddress.ForLocalListener(endpoint)));
    }
}
