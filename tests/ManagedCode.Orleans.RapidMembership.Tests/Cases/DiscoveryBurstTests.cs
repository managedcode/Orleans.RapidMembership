using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class DiscoveryBurstTests(ITestOutputHelper output)
{
    [Fact]
    public async Task SimultaneousThirtyPeerAnnouncementsConvergeWithoutAddressInputs()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        var elapsed = System.Diagnostics.Stopwatch.StartNew();
        var token = timeout.Token;
        using var reservation = new DiscoveryPortReservation();
        var port = reservation.Port;
        var identity = Guid.NewGuid().ToString("N");
        var listeners = Enumerable.Range(0, 30).Select(_ => new TcpListener(IPAddress.Any, 0)).ToArray();
        var peers = new List<MulticastDiscovery>();
        try
        {
            foreach (var listener in listeners)
            {
                listener.Start();
                peers.Add(new MulticastDiscovery(identity, "0123456789abcdef0123456789abcdef",
                    ((IPEndPoint)listener.LocalEndpoint).Port, port));
            }

            await Task.WhenAll(peers.Select(peer => peer.StartAsync(token)));
            reservation.Dispose();
            output.WriteLine(FormattableString.Invariant($"All 30 discovery listeners started after {elapsed.Elapsed}."));
            var expected = peers.Select(peer => peer.NodeId).Order().ToArray();
            while (peers.Any(peer => !peer.Peers.Select(value => value.NodeId).Append(peer.NodeId).Order().SequenceEqual(expected)))
            {
                await Task.Delay(100, token);
            }

            for (var index = 0; index < peers.Count; index++)
            {
                var discovered = peers[(index + 1) % peers.Count].Peers.Single(peer => peer.NodeId == peers[index].NodeId);
                Assert.Equal(((IPEndPoint)listeners[index].LocalEndpoint).Port, discovered.Port);
                output.WriteLine(FormattableString.Invariant($"Peer {index}: {discovered.Endpoint}; elapsed {elapsed.Elapsed}."));
                await MulticastDiscoveryTests.RequireReachableAsync(discovered, listeners[index], token);
            }
        }
        finally
        {
            await Task.WhenAll(peers.Select(async peer => await peer.DisposeAsync()));
            foreach (var listener in listeners)
            {
                listener.Stop();
            }
        }
    }
}
