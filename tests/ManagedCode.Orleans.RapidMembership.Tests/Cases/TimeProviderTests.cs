using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Discovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class TimeProviderTests
{
    [Fact]
    public async Task DiscoveryExpiresOnlyAfterItsExactBoundaryAndAdmitsAFreshAuthenticatedPeer()
    {
        using var watchdog = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        watchdog.CancelAfter(TimeSpan.FromSeconds(20));
        var token = watchdog.Token;
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        using var sender = new UdpClient(AddressFamily.InterNetwork);
        using var reservation = new DiscoveryPortReservation();
        var port = reservation.Port;
        const string secret = "0123456789abcdef0123456789abcdef";
        var identity = Guid.NewGuid().ToString("N");
        var packets = new DiscoveryPacket(identity, secret);
        await using var discovery = new MulticastDiscovery(identity, secret, 0, port, maximumPeers: 1, timeProvider: clock);
        await discovery.StartAsync(token);
        reservation.Dispose();
        var first = Guid.NewGuid();
        var datagram = packets.Encode(first, 11112, clock.GetUtcNow());
        _ = await sender.SendAsync(datagram, new IPEndPoint(IPAddress.Loopback, port), token);
        while (discovery.Peers.Count == 0)
        {
            await Task.Delay(10, token);
        }

        clock.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(first, Assert.Single(discovery.Peers).NodeId);
        clock.Advance(TimeSpan.FromTicks(1));
        Assert.Empty(discovery.Peers);
        _ = await sender.SendAsync(datagram, new IPEndPoint(IPAddress.Loopback, port), token);
        var replacement = Guid.NewGuid();
        _ = await sender.SendAsync(packets.Encode(replacement, 11114, clock.GetUtcNow()), new IPEndPoint(IPAddress.Loopback, port), token);
        while (discovery.Peers.Count == 0)
        {
            await Task.Delay(10, token);
        }

        var admitted = Assert.Single(discovery.Peers);
        Assert.Equal(replacement, admitted.NodeId);
        Assert.Equal(11114, admitted.Port);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AdvancingTimeCancelsActualHeldRpcOrWholeOperationAndHealthyRequestsStillSucceed(bool wholeOperation)
    {
        using var watchdog = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        watchdog.CancelAfter(TimeSpan.FromSeconds(40));
        var token = watchdog.Token;
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var gate = new ProposerStageGate(PeerAction.Inspect, services.GetRequiredService<Serializer<PeerRequest>>(), services.GetRequiredService<Serializer<PeerReply>>());
        await using var network = await ReplicaNetwork.StartAsync(token, collection =>
        {
            _ = collection.AddSingleton(gate);
            _ = collection.AddGrpc(options => options.Interceptors.Add<ProposerStageGate>());
        });
        var initial = new CoordinationConfiguration(42, 0, [network.Members[0]]);
        var installed = new CoordinationConfiguration(42, 1, [.. network.Members]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, installed, await network.Replicas.FreezeAsync(initial, installed));
        await using var directory = await network.AnnounceAsync(token);
        using var proposer = new MembershipProposer(network.Transport, network.Replicas.Options,
            services.GetRequiredService<Serializer<MembershipSnapshot>>(), directory);
        _ = await proposer.ExecuteAsync(null, token);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        network.Replicas.Options.TimeProvider = clock;
        gate.Arm();
        var operation = wholeOperation ? proposer.ExecuteAsync(null, token) : null;
        var calls = wholeOperation ? [] : network.Members.Select(member => network.Transport.SendAsync(member,
            new PeerRequest { Action = PeerAction.Inspect, Configuration = installed }, token)).ToArray();
        try
        {
            await gate.Entered.Task.WaitAsync(token);
            clock.Advance(wholeOperation ? network.Replicas.Options.OperationTimeout : network.Replicas.Options.PeerTimeout);
            if (wholeOperation)
            {
                _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation!.WaitAsync(token));
            }
            else
            {
                Assert.All(await Task.WhenAll(calls).WaitAsync(token), reply => Assert.Null(reply));
            }
        }
        finally
        {
            gate.Release();
        }

        var updated = await proposer.ExecuteAsync(table => MembershipMutations.Write(table, MutationTests.Entry(1), null, table.Version.Next()), token);
        Assert.True(updated.Applied);
        _ = Assert.Single((await proposer.ExecuteAsync(null, token)).Table.Members);
    }
}
