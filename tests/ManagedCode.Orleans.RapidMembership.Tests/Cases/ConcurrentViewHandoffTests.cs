namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ConcurrentViewHandoffTests
{
    [Fact]
    public async Task ConsecutiveAgreedViewsRecoverWithoutSplittingTheOldQuorumAcrossSuccessors()
    {
        var token = TestContext.Current.CancellationToken;
        await using var network = await ReplicaNetwork.StartAsync(token);
        var members = network.Members;
        var initial = new CoordinationConfiguration(42, 0, [members[0]]);
        var previous = new CoordinationConfiguration(42, 1, [.. members]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, previous, await network.Replicas.FreezeAsync(initial, previous));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in members)
        {
            Assert.True((await network.Transport.SendAsync(member,
                MemoryReplicaHandoffTests.Proposal(previous, PeerAction.Commit, snapshot, 1), token))!.Granted);
        }

        var views = new[]
        {
            new CoordinationConfiguration(42, 2, [members[0], members[1]]),
            new CoordinationConfiguration(42, 3, [.. members]),
            new CoordinationConfiguration(42, 4, [members[1], members[2]]),
        };
        for (var index = 0; index < views.Length; index++)
        {
            await network.Replicas.ObserveAsync(views[index]);
            _ = await network.Transport.SendAsync(members[index], new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = views[index],
            }, token);
        }

        var resolver = new ConfigurationResolver(network.Transport);
        CoordinationConfiguration? recovered = null;
        foreach (var _ in views)
        {
            recovered = await resolver.ResolveAsync(await network.Transport.LocateAsync(members, token), token);
        }

        Assert.True(views[^1].Matches(recovered));
        foreach (var member in views[^1].Members)
        {
            var read = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = views[^1] }, token);
            Assert.True(read is { Granted: true });
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, read.State.Committed));
        }

        var next = MemoryReplicaHandoffTests.Snapshot(2);
        foreach (var action in new[] { PeerAction.Prepare, PeerAction.Accept, PeerAction.Commit })
        {
            var replies = await network.Transport.ExchangeAsync(
                MemoryReplicaHandoffTests.Proposal(views[^1], action, next, 2), views[^1], token);
            Assert.Equal(views[^1].Quorum, replies.Count(reply => reply.Granted));
        }

        Assert.False((await network.Transport.SendAsync(members[0],
            new PeerRequest { Action = PeerAction.Inspect, Configuration = previous }, token))!.Granted);
    }
}
