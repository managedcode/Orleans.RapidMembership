using System.Net;
using System.Net.Sockets;
using System.Reflection;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MulticastSocketFailureTests
{
    [Fact]
    public async Task AbruptNativeSocketFailureIsReportedAndFreshObserverStillDiscoversReachablePeers()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        using var tcp = new TcpListener(IPAddress.Any, 0);
        tcp.Start();
        using var reservation = new DiscoveryPortReservation();
        var port = reservation.Port;
        const string secret = "0123456789abcdef0123456789abcdef";
        await using var sender = new MulticastDiscovery("socket-failure", secret, ((IPEndPoint)tcp.LocalEndpoint).Port, port);
        await using var failed = new MulticastDiscovery("socket-failure", secret, 0, port);
        await failed.StartAsync(timeout.Token);
        await sender.StartAsync(timeout.Token);
        reservation.Dispose();
        await WaitForPeerAsync(failed, sender.NodeId, timeout.Token);
        await MulticastDiscoveryTests.RequireReachableAsync(Assert.Single(failed.Peers), tcp, timeout.Token);

        // Close the actual owned socket abruptly, without invoking discovery's graceful cancellation.
        var field = typeof(MulticastDiscovery).GetField("_socket", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var socket = Assert.IsType<UdpClient>(field.GetValue(failed));
        socket.Dispose();
        InvalidOperationException? reported = null;
        while (reported is null)
        {
            try
            {
                _ = failed.Peers;
            }
            catch (InvalidOperationException exception)
            {
                reported = exception;
            }

            await Task.Delay(10, timeout.Token);
        }

        var nativeErrors = Assert.IsType<AggregateException>(reported.InnerException).Flatten().InnerExceptions;
        Assert.All(nativeErrors, error => Assert.True(error is SocketException or ObjectDisposedException));
        var disposalError = await Record.ExceptionAsync(() => failed.DisposeAsync().AsTask());
        Assert.True(disposalError is SocketException or ObjectDisposedException);
        await using var healthy = new MulticastDiscovery("socket-failure", secret, 0, port);
        await healthy.StartAsync(timeout.Token);
        await WaitForPeerAsync(healthy, sender.NodeId, timeout.Token);
        await MulticastDiscoveryTests.RequireReachableAsync(Assert.Single(healthy.Peers), tcp, timeout.Token);
    }

    private static async Task WaitForPeerAsync(MulticastDiscovery discovery, Guid incarnation, CancellationToken cancellationToken)
    {
        while (!discovery.Peers.Any(peer => peer.NodeId == incarnation))
        {
            await Task.Delay(20, cancellationToken);
        }
    }
}
