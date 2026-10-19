using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ConfigurationHandoffTests
{
    [Fact]
    public void ANewViewBlocksOldWritesBeforeFreezingAndRequiresInstallation()
    {
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 1, 2, 4);
        var replica = new ReplicaConfigurationFence(Member(1).Incarnation);
        replica.Observe(Configuration(0, 1));
        Assert.True(replica.Bootstrap());
        Assert.False(replica.Bootstrap());
        var initial = Configuration(0, 1);
        Assert.True(replica.Allows(initial));
        replica.Observe(previous);
        Assert.False(replica.Allows(initial));
        Assert.True(replica.Freeze(initial, previous));
        Assert.False(replica.Allows(initial));
        replica.Installed(initial, previous);
        Assert.True(replica.Allows(previous));
        replica.Observe(next);
        Assert.False(replica.Allows(previous));
        Assert.False(replica.Freeze(initial, next));
        Assert.True(replica.CanInstall(previous, next));
        Assert.True(previous.Matches(replica.Current));
        Assert.False(replica.Allows(next));
        Assert.False(replica.Allows(previous));
        Assert.True(replica.Freeze(previous, next));
        replica.Installed(previous, next);
        Assert.True(replica.Allows(next));
    }

    [Fact]
    public void RemovedAndRestartedIncarnationsCannotVoteAsOldMembers()
    {
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 2, 3, 4);
        var forgotten = new ReplicaConfigurationFence(Member(1).Incarnation);
        forgotten.Observe(next);
        Assert.False(forgotten.Bootstrap());
        Assert.False(forgotten.Allows(previous));
        Assert.False(forgotten.Freeze(previous, next));
        Assert.False(forgotten.CanInstall(previous, next));
        var fresh = new ReplicaConfigurationFence(Member(4).Incarnation);
        fresh.Observe(next);
        Assert.True(fresh.CanInstall(previous, next));
        fresh.Installed(previous, next);
        Assert.True(fresh.Allows(next));
        Assert.False(fresh.Allows(previous));
        Assert.True(fresh.CanInstall(previous, next));
        _ = Assert.Throws<InvalidOperationException>(() => fresh.Installed(next, previous));
    }

    [Fact]
    public void UnknownPreviousMemberCannotUseEmptyRamAsTransferredState()
    {
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 1, 2, 3);
        var forgotten = new ReplicaConfigurationFence(Member(1).Incarnation);
        forgotten.Observe(next);
        Assert.False(forgotten.CanInstall(previous, next));
        Assert.False(forgotten.Allows(next));
        _ = Assert.Throws<InvalidOperationException>(() => forgotten.Installed(previous, next));
        _ = Assert.Throws<InvalidOperationException>(() => forgotten.Observe(previous));
        _ = Assert.Throws<InvalidOperationException>(() => forgotten.Observe(next with { ClusterId = 99 }));
        _ = Assert.Throws<InvalidOperationException>(() => forgotten.Observe(Configuration(2, 1, 2, 4)));
    }

    [Fact]
    public void RecoveryPreservesAcceptedValueAndDetachesEveryNativeRow()
    {
        using var services = Services();
        var serializer = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 2, 3, 4);
        var committed = Snapshot(4);
        var accepted = Snapshot(5);
        accepted.Outcomes.Add(new CompletedMutation(accepted.OperationId, 5, true));
        var replicas = ImmutableArray.Create(
            Frozen(1, previous, next, committed, Snapshot(5), new Ballot(6, "a")),
            Frozen(2, previous, next, committed, accepted, new Ballot(7, "b")));
        var recovered = ConfigurationHandoff.Recover(previous, next, replicas, "cluster", serializer);
        Assert.True(MembershipSnapshotEquality.AreEqual(accepted, recovered));
        recovered.Table.Members[0].Item1.HostName = "changed";
        recovered.Outcomes.Clear();
        Assert.NotEqual("changed", accepted.Table.Members[0].Item1.HostName);
        _ = Assert.Single(accepted.Outcomes);
        Assert.False(ConfigurationHandoff.HasInstalledQuorum(next, [Member(2).Incarnation]));
        Assert.True(ConfigurationHandoff.HasInstalledQuorum(next, [Member(2).Incarnation, Member(4).Incarnation]));
        Assert.False(ConfigurationHandoff.HasInstalledQuorum(next, [Member(2).Incarnation, Member(2).Incarnation]));
        Assert.False(ConfigurationHandoff.HasInstalledQuorum(next, [Member(1).Incarnation, Member(2).Incarnation]));
    }

    [Fact]
    public void ACommittedSlotDominatesOlderAcceptedWorkAndReadOnlyHandoffKeepsItsIndex()
    {
        using var services = Services();
        var serializer = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 2, 3, 4);
        var current = Snapshot(5);
        var replicas = ImmutableArray.Create(
            Frozen(1, previous, next, Snapshot(3), Snapshot(4), new Ballot(20, "a")),
            Frozen(2, previous, next, current));
        var recovered = ConfigurationHandoff.Recover(previous, next, replicas, "cluster", serializer);
        Assert.True(MembershipSnapshotEquality.AreEqual(current, recovered));
        Assert.Equal(5, recovered.Index);
    }

    [Theory]
    [InlineData("minority")]
    [InlineData("duplicate")]
    [InlineData("stranger")]
    [InlineData("old-view")]
    [InlineData("new-view")]
    [InlineData("identity")]
    [InlineData("wrong-slot")]
    [InlineData("unpromised")]
    [InlineData("conflicting-commit")]
    [InlineData("conflicting-ballot")]
    public void InvalidFrozenEvidenceCannotBecomeANewMembershipTable(string change)
    {
        using var services = Services();
        var serializer = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        var previous = Configuration(1, 1, 2, 3);
        var next = Configuration(2, 2, 3, 4);
        var committed = Snapshot(4);
        var accepted = Snapshot(5);
        var first = Frozen(1, previous, next, committed, accepted, new Ballot(6, "a"));
        var second = Frozen(2, previous, next, committed, accepted, new Ballot(6, "a"));
        switch (change)
        {
            case "minority":
                _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next, [first], "cluster", serializer));
                return;
            case "duplicate": second = first; break;
            case "stranger": second = second with { Incarnation = Member(4).Incarnation }; break;
            case "old-view": second = second with { Previous = Configuration(0, 1) }; break;
            case "new-view": second = second with { Next = Configuration(3, 2, 3, 4) }; break;
            case "identity": second.State.Identity = "another"; break;
            case "wrong-slot": second.State.Accepted = Snapshot(6); break;
            case "unpromised": second.State.Promised = default; break;
            case "conflicting-commit": second.State.Committed = Snapshot(4); break;
            case "conflicting-ballot": second.State.Accepted = Snapshot(5); break;
            default: throw new ArgumentOutOfRangeException(nameof(change));
        }

        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next, [first, second], "cluster", serializer));
        var healthy = Frozen(3, previous, next, committed);
        Assert.Equal(accepted.OperationId, ConfigurationHandoff.Recover(previous, next, [first, healthy], "cluster", serializer).OperationId);
    }

    internal static CoordinationConfiguration Configuration(long version, params int[] members)
        => new(42, version, [.. members.Select(Member).OrderBy(member => member.Incarnation)]);

    internal static CoordinationMember Member(int number)
        => new(new Guid(number, 0, 0, new byte[8]), string.Create(System.Globalization.CultureInfo.InvariantCulture, $"http://127.0.0.1:{11000 + number}"));

    private static FrozenReplica Frozen(int member, CoordinationConfiguration previous, CoordinationConfiguration next,
        MembershipSnapshot committed, MembershipSnapshot? accepted = null, Ballot ballot = default)
        => new(Member(member).Incarnation, previous, next, new AcceptorState
        {
            Identity = "cluster",
            Committed = committed,
            Accepted = accepted,
            AcceptedBallot = ballot,
            Promised = ballot,
        });

    private static MembershipSnapshot Snapshot(long index) => new()
    {
        Index = index,
        OperationId = Guid.NewGuid(),
        Table = new global::Orleans.MembershipTableData(Tuple.Create(MutationTests.Entry(1), "row"), new global::Orleans.TableVersion(checked((int)index), "table")),
    };

    private static ServiceProvider Services()
        => new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
}
