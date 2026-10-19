using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class DirectedDiscoveryTests
{
    [Fact]
    public async Task InitialScanWaitsForTheTailOfTheConfiguredRangeAndActuallyProbesIt()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        using var listener = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        listener.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);
        listener.Bind(new IPEndPoint(IPAddress.Any, 0));
        var port = ((IPEndPoint)listener.LocalEndPoint!).Port;
        var expected = NetworkInterface.GetAllNetworkInterfaces().Where(adapter => adapter.OperationalStatus == OperationalStatus.Up)
            .SelectMany(adapter => adapter.GetIPProperties().UnicastAddresses).Select(value => value.Address)
            .First(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address));
        var first = expected.GetAddressBytes();
        var last = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(first);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(first, checked(last - 256));
        var addresses = DiscoveryAddressRange.Expand([new IPAddress(first).ToString() + "-" + expected.ToString()], 257);
        using var directed = new DirectedDiscovery(addresses, port);
        await Task.WhenAll(ReceiveTailAsync(listener, expected, timeout.Token), ProbeRangeAsync(directed, timeout.Token));
    }

    private static async Task ProbeRangeAsync(DirectedDiscovery directed, CancellationToken cancellationToken)
    {
        Assert.False(directed.InitialScanCompleted);
        Assert.True(await directed.ProbeAsync([1], [], cancellationToken));
        Assert.False(directed.InitialScanCompleted);
        Assert.True(await directed.ProbeAsync([2], [], cancellationToken));
        Assert.True(directed.InitialScanCompleted);
        Assert.True(await directed.ProbeAsync([3], [], cancellationToken));
        Assert.True(directed.InitialScanCompleted);
    }

    private static async Task ReceiveTailAsync(Socket listener, IPAddress expected, CancellationToken cancellationToken)
    {
        var buffer = new byte[96];
        while (true)
        {
            var datagram = await listener.ReceiveMessageFromAsync(buffer.AsMemory(), SocketFlags.None,
                new IPEndPoint(IPAddress.Any, 0), cancellationToken);
            if (datagram.PacketInformation.Address.Equals(expected) && buffer[0] == 2)
            {
                return;
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AuthenticatedUdpProbeFindsAReachableLibraryPeerWithoutJoiningMulticast(bool networkInterface)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        var available = NetworkInterface.GetAllNetworkInterfaces().Where(adapter => adapter.OperationalStatus == OperationalStatus.Up)
            .SelectMany(adapter => adapter.GetIPProperties().UnicastAddresses).Select(value => value.Address)
            .Where(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address)).ToArray();
        if (networkInterface)
        {
            Assert.NotEmpty(available);
        }

        var expected = networkInterface ? available[0] : IPAddress.Loopback;
        using var listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        using var reservation = new UdpClient(new IPEndPoint(IPAddress.Any, 0));
        var port = ((IPEndPoint)reservation.Client.LocalEndPoint!).Port;
        reservation.Dispose();
        var identity = Guid.NewGuid().ToString("N");
        const string secret = "0123456789abcdef0123456789abcdef";
        await using var peer = new MulticastDiscovery(identity, secret, ((IPEndPoint)listener.LocalEndpoint).Port, port);
        await peer.StartAsync(timeout.Token);
        using var directed = new DirectedDiscovery(DiscoveryAddressRange.Expand([expected.ToString() + "/32"], 1), port);
        using var receiving = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var observed = new TaskCompletionSource<DiscoveredPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
        var packets = new DiscoveryPacket(identity, secret);
        var receive = directed.ReceiveAsync(datagram =>
        {
            if (datagram.RemoteEndPoint.Address.Equals(expected)
                && packets.TryDecode(datagram.Buffer, DateTimeOffset.UtcNow, out var incarnation, out var servicePort)
                && incarnation == peer.NodeId && servicePort == ((IPEndPoint)listener.LocalEndpoint).Port)
            {
                _ = observed.TrySetResult(new DiscoveredPeer(incarnation, datagram.RemoteEndPoint.Address, servicePort, DateTimeOffset.UtcNow));
            }
        }, receiving.Token);
        try
        {
            Assert.True(await directed.ProbeAsync(packets.Encode(Guid.NewGuid(), 0, DateTimeOffset.UtcNow), [], timeout.Token));
            await MulticastDiscoveryTests.RequireReachableAsync(await observed.Task.WaitAsync(timeout.Token), listener, timeout.Token);
        }
        finally
        {
            await receiving.CancelAsync();
            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receive);
        }
    }
}
