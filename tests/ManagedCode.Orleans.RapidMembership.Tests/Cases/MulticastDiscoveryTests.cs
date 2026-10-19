using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MulticastDiscoveryTests
{
    private const string Secret = "0123456789abcdef0123456789abcdef";

    [Fact]
    public async Task PeersDiscoverActualTcpListenersAndClientRemainsAnObserverWithoutConfiguredAddresses()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var token = timeout.Token;
        using var firstListener = new TcpListener(IPAddress.Any, 0);
        using var secondListener = new TcpListener(IPAddress.Any, 0);
        firstListener.Start();
        secondListener.Start();
        using var reservation = new UdpClient(0);
        var discoveryPort = ((IPEndPoint)reservation.Client.LocalEndPoint!).Port;
        reservation.Dispose();
        var identity = Guid.NewGuid().ToString("N");
        await using var first = new MulticastDiscovery(identity, Secret, ((IPEndPoint)firstListener.LocalEndpoint).Port, discoveryPort);
        await using var second = new MulticastDiscovery(identity, Secret, ((IPEndPoint)secondListener.LocalEndpoint).Port, discoveryPort);
        await using var client = new MulticastDiscovery(identity, Secret, 0, discoveryPort);
        await using var wrongSecret = new MulticastDiscovery(identity, new string('x', 32), ((IPEndPoint)firstListener.LocalEndpoint).Port, discoveryPort);
        await using var otherCluster = new MulticastDiscovery("other-" + identity, Secret, ((IPEndPoint)firstListener.LocalEndpoint).Port, discoveryPort);
        await first.StartAsync(token);
        await second.StartAsync(token);
        await client.StartAsync(token);
        await wrongSecret.StartAsync(token);
        await otherCluster.StartAsync(token);
        while (client.Peers.Count != 2 || first.Peers.Count != 1 || second.Peers.Count != 1)
        {
            await Task.Delay(50, token);
        }

        Assert.Equal(second.NodeId, Assert.Single(first.Peers).NodeId);
        Assert.Equal(first.NodeId, Assert.Single(second.Peers).NodeId);
        Assert.Empty(wrongSecret.Peers);
        Assert.Empty(otherCluster.Peers);
        await RequireReachableAsync(client.Peers.Single(peer => peer.NodeId == first.NodeId), firstListener, token);
        await RequireReachableAsync(client.Peers.Single(peer => peer.NodeId == second.NodeId), secondListener, token);
        await first.DisposeAsync();
        firstListener.Stop();
        while (client.Peers.Any(peer => peer.NodeId == first.NodeId) || second.Peers.Any(peer => peer.NodeId == first.NodeId))
        {
            await Task.Delay(50, token);
        }

        Assert.Equal(second.NodeId, Assert.Single(client.Peers).NodeId);
        await RequireReachableAsync(Assert.Single(client.Peers), secondListener, token);
    }

    [Fact]
    public void PacketsRejectForeignKeysIdentitiesTamperingExpiryAndInvalidLengths()
    {
        var now = DateTimeOffset.UtcNow;
        var nodeId = Guid.NewGuid();
        var packets = new DiscoveryPacket("test", Secret);
        var encoded = packets.Encode(nodeId, 11112, now);
        Assert.Equal(DiscoveryPacket.Size, encoded.Length);
        Assert.True(packets.TryDecode(encoded, now, out var decodedId, out var port));
        Assert.Equal(nodeId, decodedId);
        Assert.Equal(11112, port);
        Assert.False(new DiscoveryPacket("other", Secret).TryDecode(encoded, now, out _, out _));
        Assert.False(new DiscoveryPacket("test", "different").TryDecode(encoded, now, out _, out _));
        Assert.False(packets.TryDecode(encoded, now.AddSeconds(11), out _, out _));
        Assert.False(packets.TryDecode(encoded, now.AddSeconds(-11), out _, out _));
        Assert.False(packets.TryDecode([], now, out _, out _));
        Assert.False(packets.TryDecode(new byte[97], now, out _, out _));
        Assert.False(packets.TryDecode(packets.Encode(Guid.Empty, 11112, now), now, out _, out _));
        encoded[52] ^= 1;
        Assert.False(packets.TryDecode(encoded, now, out _, out _));
        encoded[0] ^= 1;
        Assert.False(packets.TryDecode(encoded, now, out _, out _));
    }

    internal static async Task RequireReachableAsync(DiscoveredPeer peer, TcpListener listener, CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        var endpoint = new Uri(peer.Endpoint);
        await client.ConnectAsync(endpoint.Host, endpoint.Port, cancellationToken);
        using var server = await listener.AcceptTcpClientAsync(cancellationToken);
        var nonce = Guid.NewGuid().ToByteArray();
        await client.GetStream().WriteAsync(nonce, cancellationToken);
        var received = new byte[nonce.Length];
        await server.GetStream().ReadExactlyAsync(received, cancellationToken);
        Assert.Equal(nonce, received);
        await server.GetStream().WriteAsync(received, cancellationToken);
        await client.GetStream().ReadExactlyAsync(received, cancellationToken);
        Assert.Equal(nonce, received);
    }
}
