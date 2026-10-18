using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MemoryReplicaBoundsTests
{
    [Theory]
    [InlineData("rows")]
    [InlineData("history")]
    [InlineData("snapshot-bytes")]
    [InlineData("exchange-bytes")]
    public async Task OversizedWorkLeavesThePromiseAndStateUsableForHealthyFollowUp(string bound)
    {
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        var request = MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Accept, snapshot, 5);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Prepare, snapshot, 5))).Granted);
        if (bound == "rows")
        {
            snapshot.Table = new MembershipTableData([.. snapshot.Table.Members,
                Tuple.Create(MutationTests.Entry(2), "second"), Tuple.Create(MutationTests.Entry(3), "third")], snapshot.Table.Version);
        }
        else if (bound == "history")
        {
            snapshot.Outcomes.AddRange(Enumerable.Range(0, 9).Select(index => new CompletedMutation(Guid.NewGuid(), index, true)));
        }
        else
        {
            snapshot.Table.Members[0].Item1.HostName = new string('x', bound == "snapshot-bytes" ? 8192 : 32768);
        }

        _ = await Assert.ThrowsAsync<InvalidDataException>(() => replicas.SendAsync(member, request));
        var inspected = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.Null(inspected.State.Accepted);
        Assert.Equal(0, inspected.State.Committed.Index);
        Assert.Equal(5, inspected.State.Promised.Counter);
        var healthy = MemoryReplicaHandoffTests.Snapshot(1);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Accept, healthy, 5))).Granted);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Commit, healthy, 5))).Granted);
        var result = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.True(MembershipSnapshotEquality.AreEqual(healthy, result.State.Committed));
    }

    [Fact]
    public async Task CancellationAndInvalidActionsLeaveTheReplicaAlive()
    {
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        var replica = replicas.Add(member);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => replica.ObserveAsync(configuration, cancelled.Token));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => replica.BootstrapAsync(cancelled.Token));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => replica.ExchangeAsync(new PeerRequest(), cancelled.Token));
        await replicas.BootstrapAsync(configuration);
        foreach (var action in new[] { PeerAction.FreezeConfiguration, PeerAction.InstallConfiguration, (PeerAction)100 })
        {
            Assert.False((await replicas.SendAsync(member, new PeerRequest { Action = action, Configuration = configuration })).Granted);
        }

        var next = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        Assert.False((await replicas.SendAsync(member, new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = configuration,
            Configuration = next,
        })).Granted);
        var healthy = MemoryReplicaHandoffTests.Snapshot(1);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Accept, healthy, 1))).Granted);
    }

    [Fact]
    public async Task InvalidOrStaleTransferredStateCannotEraseACommittedRow()
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
        var current = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in previous.Members)
        {
            Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(previous, PeerAction.Commit, current, 0))).Granted);
        }

        var evidence = await replicas.FreezeAsync(previous, next);
        var request = new PeerRequest { Action = PeerAction.InstallConfiguration, PreviousConfiguration = previous, Configuration = next };
        var target = next.Members[0];
        _ = await Assert.ThrowsAsync<InvalidDataException>(() => replicas.SendAsync(target, request));
        foreach (var replica in evidence)
        {
            replica.State.Committed = new MembershipSnapshot();
        }

        request.FrozenReplicas = evidence;
        Assert.False((await replicas.SendAsync(target, request)).Granted);
        evidence = await replicas.FreezeAsync(previous, next);
        var conflict = Guid.NewGuid();
        foreach (var replica in evidence)
        {
            replica.State.Committed.OperationId = conflict;
        }

        request.FrozenReplicas = evidence;
        Assert.False((await replicas.SendAsync(target, request)).Granted);
        await replicas.InstallAsync(previous, next, await replicas.FreezeAsync(previous, next));
        var read = await replicas.SendAsync(target, new PeerRequest { Action = PeerAction.Inspect, Configuration = next });
        Assert.True(MembershipSnapshotEquality.AreEqual(current, read.State.Committed));
    }
}
