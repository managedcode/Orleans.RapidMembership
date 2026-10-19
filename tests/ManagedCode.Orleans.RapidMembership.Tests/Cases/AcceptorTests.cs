using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class AcceptorTests
{
    [Fact]
    public async Task InvalidCommitNeverChangesRowsAndAHealthyFollowingSlotStillCommits()
    {
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        var request = MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Commit, snapshot, 1);
        request.Snapshot = null;
        Assert.False((await replicas.SendAsync(member, request)).Granted);
        request.Snapshot = snapshot;
        request.Index = 2;
        Assert.False((await replicas.SendAsync(member, request)).Granted);
        request.Index = 1;
        Assert.True((await replicas.SendAsync(member, request)).Granted);
        request.Snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        request.Snapshot.OperationId = Guid.NewGuid();
        Assert.False((await replicas.SendAsync(member, request)).Granted);
        var retained = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.Equal(snapshot.OperationId, retained.State.Committed.OperationId);
        var next = MemoryReplicaHandoffTests.Snapshot(2);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Prepare, next, 2))).Granted);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Accept, next, 2))).Granted);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Commit, next, 2))).Granted);
        var committed = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration });
        Assert.Equal(next.OperationId, committed.State.Committed.OperationId);
    }

    [Fact]
    public async Task PromisesFenceOlderProposersAndFreshRamCannotVoteForTheLostIncarnation()
    {
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Prepare, snapshot, 4))).Granted);
        Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Accept, snapshot, 4))).Granted);
        var older = await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Prepare, snapshot, 3));
        Assert.False(older.Granted);
        var next = await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(configuration, PeerAction.Prepare, snapshot, 5));
        Assert.True(next.Granted);
        Assert.Equal(snapshot.OperationId, next.State.Accepted!.OperationId);
        var fresh = replicas.Add(ConfigurationHandoffTests.Member(2));
        await fresh.ObserveAsync(configuration, TestContext.Current.CancellationToken);
        Assert.False(await fresh.BootstrapAsync(TestContext.Current.CancellationToken));
        Assert.False((await replicas.SendAsync(ConfigurationHandoffTests.Member(2), new PeerRequest
        {
            Action = PeerAction.Prepare,
            Configuration = configuration,
            Index = 1,
            Ballot = new Ballot(6, "new-process"),
        })).Granted);
    }

    [Fact]
    public async Task DuplicateCommitDoesNotErasePromisesForTheFollowingSlot()
    {
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        Task<PeerReply> ExchangeAsync(PeerRequest request)
        {
            request.Configuration = configuration;
            return replicas.SendAsync(member, request);
        }

        var snapshot = new MembershipSnapshot { Index = 1 };
        Assert.True((await ExchangeAsync(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = snapshot })).Granted);
        Assert.True((await ExchangeAsync(new PeerRequest { Action = PeerAction.Prepare, Index = 2, Ballot = new Ballot(9, "a") })).Granted);
        Assert.True((await ExchangeAsync(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = snapshot })).Granted);
        Assert.False((await ExchangeAsync(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 2,
            Ballot = new Ballot(8, "b"),
            Snapshot = new MembershipSnapshot { Index = 2 },
        })).Granted);
    }

    [Fact]
    public async Task OneBallotCannotAcceptDifferentContentUnderTheSameOperationIdentity()
    {
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        Task<PeerReply> ExchangeAsync(PeerRequest request)
        {
            request.Configuration = configuration;
            return replicas.SendAsync(member, request);
        }

        var operation = Guid.NewGuid();
        var ballot = new Ballot(1, "a");
        Assert.True((await ExchangeAsync(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 1,
            Ballot = ballot,
            Snapshot = new MembershipSnapshot { Index = 1, OperationId = operation, Applied = true },
        })).Granted);
        Assert.False((await ExchangeAsync(new PeerRequest
        {
            Action = PeerAction.Accept,
            Index = 1,
            Ballot = ballot,
            Snapshot = new MembershipSnapshot { Index = 1, OperationId = operation, Applied = false },
        })).Granted);
    }

    [Theory]
    [InlineData("row-count")]
    [InlineData("row-address")]
    [InlineData("row-status")]
    [InlineData("row-etag")]
    public async Task OneBallotCannotReplaceNativeRowsOrTheirMetadata(string change)
    {
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var snapshots = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        using var replicas = new MemoryReplicaSet();
        var configuration = ConfigurationHandoffTests.Configuration(0, 1);
        var member = configuration.Members[0];
        _ = replicas.Add(member);
        await replicas.BootstrapAsync(configuration);
        Task<PeerReply> ExchangeAsync(PeerRequest request)
        {
            request.Configuration = configuration;
            return replicas.SendAsync(member, request);
        }

        var entry = MutationTests.Entry(1);
        var original = new MembershipSnapshot
        {
            Index = 1,
            OperationId = Guid.NewGuid(),
            Table = new MembershipTableData(Tuple.Create(entry, "original-etag"), new TableVersion(1, "table-etag")),
        };
        var ballot = new Ballot(1, "a");
        Assert.True((await ExchangeAsync(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = original })).Granted);
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

        Assert.False((await ExchangeAsync(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = changed })).Granted);
        Assert.True((await ExchangeAsync(new PeerRequest { Action = PeerAction.Accept, Index = 1, Ballot = ballot, Snapshot = original })).Granted);
        var committed = await ExchangeAsync(new PeerRequest { Action = PeerAction.Commit, Index = 1, Snapshot = original });
        Assert.True(committed.Granted);
        var inspected = await ExchangeAsync(new PeerRequest { Action = PeerAction.Inspect });
        var row = Assert.Single(inspected.State.Committed.Table.Members);
        Assert.Equal(entry.SiloAddress, row.Item1.SiloAddress);
        Assert.Equal(entry.Status, row.Item1.Status);
        Assert.Equal("original-etag", row.Item2);
    }
}
