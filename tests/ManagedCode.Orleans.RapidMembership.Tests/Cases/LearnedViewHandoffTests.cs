namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class LearnedViewHandoffTests
{
    [Fact]
    public async Task ALearnerThatMissedAnIntermediateViewRecoversUsingARealOldVotersFrozenWitness()
    {
        var token = TestContext.Current.CancellationToken;
        await using var network = await ReplicaNetwork.StartAsync(token);
        var members = network.Members;
        var initial = new CoordinationConfiguration(42, 0, [members[0]]);
        var previous = new CoordinationConfiguration(42, 1, [.. members]);
        var intermediate = new CoordinationConfiguration(42, 2, [members[0], members[1]]);
        var latest = new CoordinationConfiguration(42, 3, [.. members]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, previous, await network.Replicas.FreezeAsync(initial, previous));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in members)
        {
            Assert.True((await network.Transport.SendAsync(member,
                MemoryReplicaHandoffTests.Proposal(previous, PeerAction.Commit, snapshot, 1), token))!.Granted);
        }

        await network.Replicas.ObserveAsync(members[0], intermediate);
        await network.Replicas.ObserveAsync(latest);
        var firstVote = await network.Transport.SendAsync(members[0], new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = intermediate,
        }, token);
        Assert.True(firstVote is { Granted: true });
        var witness = new FrozenReplica(firstVote.Incarnation, previous, intermediate, firstVote.State);
        var denied = await network.Transport.SendAsync(members[1], new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = intermediate,
        }, token);
        Assert.True(denied is { Granted: false, NextConfiguration: null });
        await RequireRejectedWitnessesAsync(network, previous, intermediate, witness, token);
        var resolver = new ConfigurationResolver(network.Transport);
        CoordinationConfiguration? recovered = null;
        for (var round = 0; round < 3; round++)
        {
            recovered = await resolver.ResolveAsync(await network.Transport.LocateAsync(members, token), token);
        }

        Assert.True(latest.Matches(recovered));
        foreach (var member in members)
        {
            var read = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = latest }, token);
            Assert.True(read is { Granted: true });
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, read.State.Committed));
        }

        var next = MemoryReplicaHandoffTests.Snapshot(2);
        foreach (var action in new[] { PeerAction.Prepare, PeerAction.Accept, PeerAction.Commit })
        {
            var replies = await network.Transport.ExchangeAsync(
                MemoryReplicaHandoffTests.Proposal(latest, action, next, 2), latest, token);
            Assert.True(replies.Count(reply => reply.Granted) >= latest.Quorum);
        }
    }

    private static async Task RequireRejectedWitnessesAsync(ReplicaNetwork network, CoordinationConfiguration previous,
        CoordinationConfiguration intermediate, FrozenReplica witness, CancellationToken token)
    {
        var recipient = network.Members[1];
        var invalid = new[]
        {
            witness with { Incarnation = Guid.NewGuid() },
            witness with { Incarnation = recipient.Incarnation },
            witness with { State = new AcceptorState { Identity = "foreign" } },
            witness with { Next = intermediate with { Version = intermediate.Version + 1 } },
        };
        foreach (var attempt in invalid)
        {
            var rejected = await network.Transport.SendAsync(recipient, new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = intermediate,
                FrozenReplicas = [attempt],
            }, token);
            Assert.True(rejected is { Granted: false, NextConfiguration: null });
        }

        var duplicate = await network.Transport.SendAsync(recipient, new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = intermediate,
            FrozenReplicas = [witness, witness],
        }, token);
        Assert.True(duplicate is { Granted: false, NextConfiguration: null });
        Assert.Null(await network.Transport.SendAsync(recipient, new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = previous,
            Configuration = intermediate,
            FrozenReplicas = [witness],
        }, token));
    }
}
