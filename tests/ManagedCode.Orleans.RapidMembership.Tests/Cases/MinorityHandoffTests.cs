namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MinorityHandoffTests
{
    [Fact]
    public async Task AnUnfrozenReplicaRejectsMissingEvidenceAndLearnsOnlyAnOldMajorityCertificate()
    {
        using var replicas = new MemoryReplicaSet();
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var previous = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var next = ConfigurationHandoffTests.Configuration(2, 1, 2, 3, 4);
        foreach (var number in Enumerable.Range(1, 4))
        {
            _ = replicas.Add(ConfigurationHandoffTests.Member(number));
        }

        await replicas.BootstrapAsync(initial);
        await replicas.InstallAsync(initial, previous, await replicas.FreezeAsync(initial, previous));
        await replicas.ObserveAsync(next);
        _ = await Assert.ThrowsAsync<InvalidDataException>(() => replicas.SendAsync(previous.Members[0], new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = previous,
            Configuration = next,
        }));
        Assert.False((await replicas.SendAsync(previous.Members[0], new PeerRequest { Action = PeerAction.Inspect, Configuration = next })).Granted);
        var evidence = System.Collections.Immutable.ImmutableArray.CreateBuilder<FrozenReplica>();
        foreach (var member in previous.Members.Skip(1))
        {
            var frozen = await replicas.SendAsync(member, new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = next,
            });
            Assert.True(frozen.Granted);
            evidence.Add(new FrozenReplica(member.Incarnation, previous, next, frozen.State));
        }

        _ = await Assert.ThrowsAsync<InvalidDataException>(() => replicas.SendAsync(previous.Members[0], new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = previous,
            Configuration = next,
            FrozenReplicas = [evidence[0]],
        }));
        Assert.False((await replicas.SendAsync(previous.Members[0], new PeerRequest { Action = PeerAction.Inspect, Configuration = next })).Granted);
        await replicas.InstallAsync(previous, next, evidence.ToImmutable());
        foreach (var member in next.Members)
        {
            Assert.True((await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = next })).Granted);
            Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(next, PeerAction.Prepare,
                MemoryReplicaHandoffTests.Snapshot(1), 1))).Granted);
        }
    }

    [Fact]
    public async Task OldVotesFinishTheFirstAgreedSuccessorBeforeInstallingALaterView()
    {
        using var replicas = new MemoryReplicaSet();
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var previous = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var minority = ConfigurationHandoffTests.Configuration(2, 1, 2, 3, 4);
        var winner = ConfigurationHandoffTests.Configuration(3, 1, 2, 3, 5);
        foreach (var number in Enumerable.Range(1, 5))
        {
            _ = replicas.Add(ConfigurationHandoffTests.Member(number));
        }

        await replicas.BootstrapAsync(initial);
        await replicas.InstallAsync(initial, previous, await replicas.FreezeAsync(initial, previous));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in previous.Members)
        {
            Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(previous, PeerAction.Commit, snapshot, 1))).Granted);
        }

        await replicas.ObserveAsync(minority);
        Assert.True((await replicas.SendAsync(previous.Members[0], new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = minority,
        })).Granted);
        await replicas.ObserveAsync(winner);
        var evidence = System.Collections.Immutable.ImmutableArray.CreateBuilder<FrozenReplica>();
        foreach (var member in previous.Members)
        {
            Assert.False((await replicas.SendAsync(member, new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = winner,
            })).Granted);
            var frozen = await replicas.SendAsync(member, new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = minority,
            });
            Assert.True(frozen.Granted);
            evidence.Add(new FrozenReplica(member.Incarnation, previous, minority, frozen.State));
        }

        Assert.False((await replicas.SendAsync(previous.Members[0], new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = winner,
        })).Granted);
        await replicas.InstallAsync(previous, minority, evidence.ToImmutable());
        await replicas.InstallAsync(minority, winner, await replicas.FreezeAsync(minority, winner));
        foreach (var member in winner.Members)
        {
            var read = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = winner });
            Assert.True(read.Granted);
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, read.State.Committed));
            Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(winner, PeerAction.Prepare,
                MemoryReplicaHandoffTests.Snapshot(2), 7))).Granted);
        }
    }
}
