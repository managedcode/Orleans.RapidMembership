using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MulticastRouteTests
{
    [Fact]
    public async Task UnroutableDirectedProbeStillReachesTheNativeLoopbackListener()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        using var listener = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        using var directed = new DirectedDiscovery([IPAddress.Parse("192.0.2.254")],
            ((IPEndPoint)listener.Client.LocalEndPoint!).Port);
        var socket = (UdpClient)typeof(DirectedDiscovery).GetField("_socket",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(directed)!;
        socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.DontRoute, true);
        socket.EnableBroadcast = false;
        var nonce = Guid.NewGuid().ToByteArray();
        Assert.True(await directed.ProbeAsync(nonce, [], timeout.Token));
        Assert.Equal(nonce, (await listener.ReceiveAsync(timeout.Token)).Buffer);
        socket.EnableBroadcast = true;
        Assert.True(await directed.ProbeAsync(nonce, [], timeout.Token));
        var delivered = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var receiving = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var receive = directed.ReceiveAsync(packet => delivered.TrySetResult(packet.Buffer), receiving.Token);
        await directed.ReplyAsync(nonce, (IPEndPoint)listener.Client.LocalEndPoint!, timeout.Token);
        var query = await listener.ReceiveAsync(timeout.Token);
        _ = await listener.SendAsync(nonce, query.RemoteEndPoint, timeout.Token);
        Assert.Equal(nonce, await delivered.Task.WaitAsync(timeout.Token));
        await receiving.CancelAsync();
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receive);
    }

    [Fact]
    public async Task UnavailableInterfaceDoesNotStopAHealthyNativeLoopbackAnnouncement()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        var group = IPAddress.Parse("239.192.44.44");
        var unavailable = IPAddress.Parse("192.0.2.254");
        using var receiver = new UdpClient(new IPEndPoint(IPAddress.Any, 0));
        receiver.JoinMulticastGroup(group, IPAddress.Loopback);
        using var sender = new UdpClient(new IPEndPoint(IPAddress.Any, 0)) { MulticastLoopback = true };
        var joined = MulticastInterfaces.Join(sender, group, [unavailable, IPAddress.Loopback]);
        Assert.Equal([IPAddress.Loopback], joined);
        var destination = new IPEndPoint(group, ((IPEndPoint)receiver.Client.LocalEndPoint!).Port);
        Assert.False(await MulticastInterfaces.SendAsync(sender, [unavailable], [3, 1, 4], destination, timeout.Token));
        Assert.True(await MulticastInterfaces.SendAsync(sender, [unavailable, IPAddress.Loopback], [3, 1, 4], destination, timeout.Token));
        var received = await receiver.ReceiveAsync(timeout.Token);
        Assert.Equal([3, 1, 4], received.Buffer);
    }
}
