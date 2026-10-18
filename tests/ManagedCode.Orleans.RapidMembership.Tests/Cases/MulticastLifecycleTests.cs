using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MulticastLifecycleTests
{
    private const string Secret = "0123456789abcdef0123456789abcdef";

    [Fact]
    public async Task BoundedObserverRecoversAfterCancellationAndReplacesAnExpiredPeer()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
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
        await using var observer = new MulticastDiscovery(identity, Secret, 0, discoveryPort, maximumPeers: 1);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => observer.StartAsync(cancelled.Token));
        await observer.StartAsync(token);
        await first.StartAsync(token);
        while (observer.Peers.Count == 0)
        {
            await Task.Delay(50, token);
        }

        Assert.Equal(first.NodeId, Assert.Single(observer.Peers).NodeId);
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => observer.StartAsync(token));
        await second.StartAsync(token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.Equal(first.NodeId, Assert.Single(observer.Peers).NodeId);
        await first.DisposeAsync();
        firstListener.Stop();
        while (!observer.Peers.Any(peer => peer.NodeId == second.NodeId))
        {
            Assert.InRange(observer.Peers.Count, 0, 1);
            await Task.Delay(50, token);
        }

        var replacement = Assert.Single(observer.Peers);
        Assert.Equal(second.NodeId, replacement.NodeId);
        await MulticastDiscoveryTests.RequireReachableAsync(replacement, secondListener, token);
        await observer.DisposeAsync();
        _ = Assert.Throws<ObjectDisposedException>(() => observer.Peers);
        _ = await Assert.ThrowsAsync<ObjectDisposedException>(() => observer.StartAsync(token));
    }

    [Fact]
    public async Task FailedNativeSocketBindIsReportedAndPortWorksAfterJoinedDisposal()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        using var occupied = new UdpClient(AddressFamily.InterNetwork);
        occupied.ExclusiveAddressUse = true;
        occupied.Client.Bind(new IPEndPoint(IPAddress.Any, 0));
        var port = ((IPEndPoint)occupied.Client.LocalEndPoint!).Port;
        using var listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var identity = Guid.NewGuid().ToString("N");
        await using var failed = new MulticastDiscovery(identity, Secret, ((IPEndPoint)listener.LocalEndpoint).Port, port);
        _ = await Assert.ThrowsAnyAsync<SocketException>(() => failed.StartAsync(token));
        await failed.DisposeAsync();
        occupied.Dispose();
        await using var healthy = new MulticastDiscovery(identity, Secret, ((IPEndPoint)listener.LocalEndpoint).Port, port);
        await using var observer = new MulticastDiscovery(identity, Secret, 0, port);
        await healthy.StartAsync(token);
        await observer.StartAsync(token);
        while (observer.Peers.Count == 0)
        {
            await Task.Delay(50, token);
        }

        var peer = Assert.Single(observer.Peers);
        Assert.Equal(healthy.NodeId, peer.NodeId);
        await MulticastDiscoveryTests.RequireReachableAsync(peer, listener, token);
    }

    [Theory]
    [InlineData(-1, 11113, 1)]
    [InlineData(65536, 11113, 1)]
    [InlineData(11112, 0, 1)]
    [InlineData(11112, 65536, 1)]
    [InlineData(11112, 11113, 0)]
    public void InvalidNetworkAndPeerBoundsFailBeforeSocketStartup(int servicePort, int discoveryPort, int maximumPeers)
        => _ = Assert.Throws<ArgumentOutOfRangeException>(() => new MulticastDiscovery("test", Secret, servicePort, discoveryPort, maximumPeers));
}
