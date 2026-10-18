using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class AcceptorTests
{
    [Fact]
    public void PromisesAndAcceptedValuesSurviveRestartAndRejectAnOlderProposer()
    {
        using var directory = new TestDirectory();
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AcceptorState>>();
        var options = new RapidMembershipOptions { StorageDirectory = directory.Path };
        var ballot = new Ballot(4, "a");
        var snapshot = new MembershipSnapshot { Index = 1, OperationId = Guid.NewGuid() };
        using (var firstStorage = new VoterStorage(options, "cluster", serializer))
        using (var first = new MembershipAcceptor(firstStorage, serializer))
        {
            Assert.True(first.Exchange(new PeerRequest { Action = PeerAction.Prepare, Index = 1, Ballot = ballot }).Granted);
            Assert.True(first.Exchange(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = snapshot }).Granted);
        }

        using var reopenedStorage = new VoterStorage(options, "cluster", serializer);
        using var reopened = new MembershipAcceptor(reopenedStorage, serializer);
        var stale = reopened.Exchange(new PeerRequest { Action = PeerAction.Prepare, Index = 1, Ballot = new Ballot(3, "z") });
        Assert.False(stale.Granted);
        Assert.Equal(snapshot.OperationId, stale.State.Accepted!.OperationId);
        Assert.Equal(ballot, stale.State.AcceptedBallot);
        var next = reopened.Exchange(new PeerRequest { Action = PeerAction.Prepare, Index = 1, Ballot = new Ballot(5, "b") });
        Assert.True(next.Granted);
        Assert.Equal(snapshot.OperationId, next.State.Accepted!.OperationId);
    }

    [Fact]
    public void DuplicateCommitDoesNotErasePromisesForTheFollowingSlot()
    {
        using var directory = new TestDirectory();
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AcceptorState>>();
        using var storage = new VoterStorage(new RapidMembershipOptions { StorageDirectory = directory.Path }, "cluster", serializer);
        using var acceptor = new MembershipAcceptor(storage, serializer);
        var snapshot = new MembershipSnapshot { Index = 1 };
        Assert.True(acceptor.Exchange(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = snapshot }).Granted);
        Assert.True(acceptor.Exchange(new PeerRequest { Action = PeerAction.Prepare, Index = 2, Ballot = new Ballot(9, "a") }).Granted);
        Assert.True(acceptor.Exchange(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = snapshot }).Granted);
        Assert.False(acceptor.Exchange(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 2,
            Ballot = new Ballot(8, "b"),
            Snapshot = new MembershipSnapshot { Index = 2 },
        }).Granted);
    }

    [Fact]
    public void OneBallotCannotAcceptDifferentContentUnderTheSameOperationIdentity()
    {
        using var directory = new TestDirectory();
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AcceptorState>>();
        using var storage = new VoterStorage(new RapidMembershipOptions { StorageDirectory = directory.Path }, "cluster", serializer);
        using var acceptor = new MembershipAcceptor(storage, serializer);
        var operation = Guid.NewGuid();
        var ballot = new Ballot(1, "a");
        Assert.True(acceptor.Exchange(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 1,
            Ballot = ballot,
            Snapshot = new MembershipSnapshot { Index = 1, OperationId = operation, Applied = true },
        }).Granted);
        Assert.False(acceptor.Exchange(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 1,
            Ballot = ballot,
            Snapshot = new MembershipSnapshot { Index = 1, OperationId = operation, Applied = false },
        }).Granted);
    }

    [Theory]
    [InlineData("row-count")]
    [InlineData("row-address")]
    [InlineData("row-status")]
    [InlineData("row-etag")]
    public void OneBallotCannotReplaceNativeRowsOrTheirMetadata(string change)
    {
        using var directory = new TestDirectory();
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var states = services.GetRequiredService<Serializer<AcceptorState>>();
        var snapshots = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        using var storage = new VoterStorage(new RapidMembershipOptions { StorageDirectory = directory.Path }, "cluster", states);
        using var acceptor = new MembershipAcceptor(storage, states);
        var entry = MutationTests.Entry(1);
        var original = new MembershipSnapshot
        {
            Index = 1,
            OperationId = Guid.NewGuid(),
            Table = new MembershipTableData(Tuple.Create(entry, "original-etag"), new TableVersion(1, "table-etag")),
        };
        var ballot = new Ballot(1, "a");
        Assert.True(acceptor.Exchange(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = original }).Granted);
        var changed = snapshots.Deserialize(snapshots.SerializeToArray(original))!;
        switch (change)
        {
            case "row-count":
                changed.Table = new MembershipTableData(changed.Table.Version);
                break;
            case "row-address":
                changed.Table.Members[0].Item1.SiloAddress = MutationTests.Entry(2).SiloAddress;
                break;
            case "row-status":
                changed.Table.Members[0].Item1.Status = SiloStatus.Dead;
                break;
            case "row-etag":
                changed.Table = new MembershipTableData(Tuple.Create(entry, "changed-etag"), changed.Table.Version);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(change));
        }

        Assert.False(acceptor.Exchange(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = changed }).Granted);
        Assert.True(acceptor.Exchange(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = original }).Granted);
        var committed = acceptor.Exchange(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = original });
        Assert.True(committed.Granted);
        var inspected = acceptor.Exchange(new PeerRequest { Action = PeerAction.Inspect });
        var row = Assert.Single(inspected.State.Committed.Table.Members);
        Assert.Equal(entry.SiloAddress, row.Item1.SiloAddress);
        Assert.Equal(entry.Status, row.Item1.Status);
        Assert.Equal("original-etag", row.Item2);
    }

    [Fact]
    public void DurableStateRejectsWrongClusterAndCorruptionAndConcurrentOwnership()
    {
        using var directory = new TestDirectory();
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AcceptorState>>();
        var options = new RapidMembershipOptions { StorageDirectory = directory.Path };
        using (var storage = new VoterStorage(options, "one", serializer))
        {
            storage.Save(new AcceptorState { Identity = "one" });
            _ = Assert.Throws<IOException>(() => new VoterStorage(options, "one", serializer));
        }

        using (var storage = new VoterStorage(options, "two", serializer))
        {
            _ = Assert.Throws<InvalidDataException>(() => storage.Load());
        }

        File.WriteAllBytes(System.IO.Path.Combine(directory.Path, "membership.bin"), [1, 2, 3]);
        using var corrupt = new VoterStorage(options, "one", serializer);
        _ = Assert.Throws<InvalidDataException>(() => corrupt.Load());
    }
}
