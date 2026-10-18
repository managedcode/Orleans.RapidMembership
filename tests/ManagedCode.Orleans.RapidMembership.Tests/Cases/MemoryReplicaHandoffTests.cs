using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MemoryReplicaHandoffTests
{
    [Fact]
    public async Task AcceptedNativeRowsSurviveHandoffAndRepeatedInstallPreservesNewerPromises()
    {
        using var replicas = new MemoryReplicaSet();
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var previous = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var next = ConfigurationHandoffTests.Configuration(2, 1, 2, 4);
        foreach (var number in Enumerable.Range(1, 4))
        {
            _ = replicas.Add(ConfigurationHandoffTests.Member(number));
        }

        await replicas.BootstrapAsync(initial);
        await replicas.InstallAsync(initial, previous, await replicas.FreezeAsync(initial, previous));
        var snapshot = Snapshot(1);
        foreach (var member in previous.Members.Take(previous.Quorum))
        {
            Assert.True((await replicas.SendAsync(member, Proposal(previous, PeerAction.Prepare, snapshot, 5))).Granted);
            Assert.True((await replicas.SendAsync(member, Proposal(previous, PeerAction.Accept, snapshot, 5))).Granted);
        }

        var evidence = await replicas.FreezeAsync(previous, next);
        Assert.False((await replicas.SendAsync(previous.Members[0], Proposal(previous, PeerAction.Accept, Snapshot(1), 20))).Granted);
        await replicas.InstallAsync(previous, next, evidence);
        foreach (var member in next.Members)
        {
            var described = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Describe });
            Assert.True(described.Granted);
            Assert.True(next.Matches(described.ObservedConfiguration));
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, described.State.Committed));
            Assert.True((await replicas.SendAsync(member, Proposal(next, PeerAction.Prepare, Snapshot(2), 10))).Granted);
        }

        await replicas.InstallAsync(previous, next, evidence);
        var healthy = Snapshot(2);
        foreach (var member in next.Members)
        {
            Assert.False((await replicas.SendAsync(member, Proposal(next, PeerAction.Accept, healthy, 9))).Granted);
            Assert.True((await replicas.SendAsync(member, Proposal(next, PeerAction.Prepare, healthy, 11))).Granted);
            Assert.True((await replicas.SendAsync(member, Proposal(next, PeerAction.Accept, healthy, 11))).Granted);
            Assert.True((await replicas.SendAsync(member, Proposal(next, PeerAction.Commit, healthy, 11))).Granted);
            var inspected = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = next });
            Assert.True(MembershipSnapshotEquality.AreEqual(healthy, inspected.State.Committed));
        }

        var removed = await replicas.SendAsync(previous.Members[2], new PeerRequest { Action = PeerAction.Inspect, Configuration = previous });
        Assert.False(removed.Granted);
    }

    [Fact]
    public async Task ANewIncarnationOnTheSameEndpointCannotAnswerForThePreviousProcess()
    {
        using var replicas = new MemoryReplicaSet();
        var oldMember = ConfigurationHandoffTests.Member(1);
        var freshMember = ConfigurationHandoffTests.Member(2) with { Endpoint = oldMember.Endpoint };
        var fresh = replicas.Add(freshMember);
        await fresh.ObserveAsync(ConfigurationHandoffTests.Configuration(1, 1, 3, 4), TestContext.Current.CancellationToken);
        Assert.False(await fresh.BootstrapAsync(TestContext.Current.CancellationToken));
        var wrong = await fresh.ExchangeAsync(new PeerRequest { Action = PeerAction.Describe, Recipient = oldMember.Incarnation }, TestContext.Current.CancellationToken);
        Assert.False(wrong.Granted);
        Assert.Equal(freshMember.Incarnation, wrong.Incarnation);
        var correct = await replicas.SendAsync(freshMember, new PeerRequest { Action = PeerAction.Describe });
        Assert.True(correct.Granted);
        Assert.Null(correct.Configuration);
        Assert.Equal(0, correct.State.Committed.Index);
        Assert.Null(correct.State.Accepted);
    }

    [Fact]
    public async Task RepliesAndAcceptedRequestsCannotMutateTheOwnedRamState()
    {
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        _ = replicas.Add(configuration.Members[0]);
        await replicas.BootstrapAsync(configuration);
        var snapshot = Snapshot(1);
        Assert.True((await replicas.SendAsync(configuration.Members[0], Proposal(configuration, PeerAction.Accept, snapshot, 4))).Granted);
        snapshot.Table.Members[0].Item1.HostName = "changed request";
        var first = await replicas.SendAsync(configuration.Members[0], new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.NotEqual("changed request", first.State.Accepted!.Table.Members[0].Item1.HostName);
        first.State.Accepted.Table.Members[0].Item1.HostName = "changed reply";
        var second = await replicas.SendAsync(configuration.Members[0], new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.NotEqual("changed reply", second.State.Accepted!.Table.Members[0].Item1.HostName);
        Assert.False((await replicas.SendAsync(configuration.Members[0], new PeerRequest { Action = PeerAction.Inspect })).Granted);
    }

    internal static MembershipSnapshot Snapshot(int index) => new()
    {
        Index = index,
        OperationId = Guid.NewGuid(),
        Applied = true,
        Table = new MembershipTableData(Tuple.Create(MutationTests.Entry(1), "row"), new TableVersion(index, "table")),
    };

    internal static PeerRequest Proposal(CoordinationConfiguration configuration, PeerAction action, MembershipSnapshot snapshot, long counter)
        => new() { Action = action, Configuration = configuration, Index = snapshot.Index, Snapshot = action == PeerAction.Prepare ? null : snapshot, Ballot = new Ballot(counter, "proposer") };
}
