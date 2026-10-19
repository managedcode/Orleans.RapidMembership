using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ReadRepairTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AReadRepairsObsoleteAcceptedWorkWithoutAppendingAnEmptySlot(bool olderSlot)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var gate = new QuorumReadGate(services.GetRequiredService<Serializer<PeerRequest>>());
        await using var network = await ReplicaNetwork.StartAsync(token, collection =>
        {
            _ = collection.AddSingleton(gate);
            _ = collection.AddGrpc(options => options.Interceptors.Add<QuorumReadGate>());
        });
        var initial = new CoordinationConfiguration(42, 0, [network.Members[0]]);
        network.Replicas.Options.PeerTimeout = TimeSpan.FromSeconds(10);
        var installed = new CoordinationConfiguration(42, 1, [.. network.Members]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, installed, await network.Replicas.FreezeAsync(initial, installed));
        var current = MemoryReplicaHandoffTests.Snapshot(1);
        await ChooseAsync(network, installed, current, acceptOnEveryReplica: true, token);
        if (olderSlot)
        {
            current = new MembershipSnapshot
            {
                Index = 2,
                OperationId = Guid.NewGuid(),
                Applied = true,
                Table = MembershipMutations.Write(current.Table, MutationTests.Entry(2), null, current.Table.Version.Next())!,
            };
            await ChooseAsync(network, installed, current, acceptOnEveryReplica: false, token);
        }

        var behind = await network.Transport.SendAsync(network.Members[^1], new PeerRequest { Action = PeerAction.Inspect, Configuration = installed }, token);
        Assert.Equal(0, behind!.State.Committed.Index);
        Assert.Equal(1, behind.State.Accepted!.Index);
        await using var directory = await network.AnnounceAsync(token);
        using var proposer = new MembershipProposer(network.Transport, network.Replicas.Options,
            services.GetRequiredService<Serializer<MembershipSnapshot>>(), directory);
        gate.Arm(network.Members[0].Incarnation);
        MembershipSnapshot read;
        try
        {
            read = await proposer.ExecuteAsync(null, token);
        }
        finally
        {
            gate.Release();
        }

        Assert.True(gate.BlockedCalls >= 2);
        Assert.Equal(current.Index, read.Index);
        Assert.True(MembershipSnapshotEquality.AreEqual(current, read));
        foreach (var member in network.Members)
        {
            var repaired = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = installed }, token);
            Assert.True(MembershipSnapshotEquality.AreEqual(current, repaired!.State.Committed));
            Assert.Null(repaired.State.Accepted);
        }

        var heartbeat = MutationTests.Entry(1);
        heartbeat.IAmAliveTime = DateTime.UtcNow;
        var healthy = await proposer.ExecuteAsync(table => MembershipMutations.Heartbeat(table, heartbeat), token);
        Assert.True(healthy.Applied);
        Assert.Equal(current.Index + 1, healthy.Index);
        var after = await proposer.ExecuteAsync(null, token);
        Assert.Equal(healthy.Index, after.Index);
        Assert.Equal(current.Table.Members.Count, after.Table.Members.Count);
        Assert.Equal(heartbeat.IAmAliveTime, after.Table.TryGet(heartbeat.SiloAddress)!.Item1.IAmAliveTime);
    }

    private static async Task ChooseAsync(ReplicaNetwork network, CoordinationConfiguration configuration,
        MembershipSnapshot snapshot, bool acceptOnEveryReplica, CancellationToken token)
    {
        var recipients = acceptOnEveryReplica ? network.Members : network.Members.Take(configuration.Quorum);
        foreach (var member in recipients)
        {
            foreach (var action in new[] { PeerAction.Prepare, PeerAction.Accept })
            {
                var reply = await network.Transport.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, action, snapshot, snapshot.Index), token);
                Assert.True(reply is { Granted: true });
            }
        }

        foreach (var member in network.Members.Take(configuration.Quorum))
        {
            var reply = await network.Transport.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Commit, snapshot, snapshot.Index), token);
            Assert.True(reply is { Granted: true });
        }
    }
}
