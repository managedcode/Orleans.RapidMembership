using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class NetworkConsensusTests
{
    [Theory]
    [InlineData(20)]
    [InlineData(30)]
    public async Task ConcurrentIndependentWritersAndReadersKeepAllAcknowledgedRows(int writers)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var requests = Enumerable.Range(0, writers).Select(async index =>
        {
            var proposer = cluster.Proposer();
            var written = await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
                MutationTests.Entry(index + 1), null, table.Version.Next()), timeout.Token);
            Assert.True(written.Applied);
            var read = await proposer.ExecuteAsync(null, timeout.Token);
            Assert.Contains(read.Table.Members, row => row.Item1.SiloAddress.Equals(MutationTests.Entry(index + 1).SiloAddress));
        });
        await Task.WhenAll(requests);
        var result = await cluster.Proposer().ExecuteAsync(null, timeout.Token);
        Assert.Equal(writers, result.Table.Members.Count);
    }

    [Fact]
    public async Task SettledMutationHistoryIsBoundedAndRemainsDurableAfterVoterLoss()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var proposer = cluster.Proposer(maximumOperationHistory: 2);
        MembershipSnapshot? latest = null;
        for (var generation = 1; generation <= 5; generation++)
        {
            latest = await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
                MutationTests.Entry(generation), null, table.Version.Next()), timeout.Token);
            Assert.True(latest.Applied);
            Assert.InRange(latest.Outcomes.Count, 1, 2);
        }

        Assert.NotNull(latest);
        Assert.Equal(3, latest.OutcomeFloor);
        await cluster.StopAsync(2);
        var read = await proposer.ExecuteAsync(null, timeout.Token);
        Assert.Equal(latest.Outcomes, read.Outcomes);
        Assert.Equal(latest.OutcomeFloor, read.OutcomeFloor);
        Assert.Equal(5, read.Table.Members.Count);
    }

    [Fact]
    public async Task QuorumReadKeepsCommittedIndexAndCannotUndoAcknowledgedWrites()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var proposer = cluster.Proposer();
        var first = await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(1), null, table.Version.Next()), timeout.Token);
        var reads = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => cluster.Proposer().ExecuteAsync(null, timeout.Token)));
        Assert.All(reads, read => Assert.Equal(first.Index, read.Index));
        Assert.All(reads, read => Assert.Equal(first.Table.Version, read.Table.Version));
        await cluster.StopAsync(2);
        var afterLoss = await cluster.Proposer().ExecuteAsync(null, timeout.Token);
        Assert.Equal(first.Table.Version, afterLoss.Table.Version);
        _ = Assert.Single(afterLoss.Table.Members);
    }

    [Fact]
    public async Task ConcurrentNativeCasHasOneWinnerAndAllReadersAgree()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var proposers = Enumerable.Range(0, 3).Select(_ => cluster.Proposer()).ToArray();
        var initial = await proposers[0].ExecuteAsync(null, timeout.Token);
        var attempts = proposers.Select((proposer, index) => proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(index + 1), null, initial.Table.Version.Next()), timeout.Token));
        var results = await Task.WhenAll(attempts);
        _ = Assert.Single(results, result => result.Applied);
        var views = await Task.WhenAll(proposers.Select(proposer => proposer.ExecuteAsync(null, timeout.Token)));
        Assert.All(views, view => Assert.Single(view.Table.Members));
        Assert.All(views, view => Assert.Equal(views[0].Table.Version, view.Table.Version));
        views[0].Table.Members[0].Item1.HostName = "mutated-caller-copy";
        var fresh = await proposers[1].ExecuteAsync(null, timeout.Token);
        Assert.Equal("host", fresh.Table.Members[0].Item1.HostName);
    }

    [Fact]
    public async Task MajorityAcceptedWriteIsRecoveredAfterProposerLossBeforeCommit()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var transport = cluster.Transport();
        var initial = new MembershipSnapshot();
        var table = MembershipMutations.Write(initial.Table, MutationTests.Entry(1), null, initial.Table.Version.Next());
        Assert.NotNull(table);
        var chosen = new MembershipSnapshot { Index = 1, OperationId = Guid.NewGuid(), Table = table, Applied = true };
        var ballot = new Ballot(100, "lost-proposer");
        var promises = await transport.ExchangeAsync(new PeerRequest { Action = PeerAction.Prepare, Configuration = cluster.Configuration, Index = 1, Ballot = ballot, Snapshot = initial }, cluster.Configuration, timeout.Token);
        Assert.True(promises.Count(reply => reply.Granted) >= 2);
        var accepted = await transport.ExchangeAsync(new PeerRequest { Action = PeerAction.Accept, Configuration = cluster.Configuration, Index = 1, Ballot = ballot, Snapshot = chosen }, cluster.Configuration, timeout.Token);
        Assert.True(accepted.Count(reply => reply.Granted) >= 2);
        await cluster.StopAsync(2);
        var recovered = await cluster.Proposer().ExecuteAsync(null, timeout.Token);
        _ = Assert.Single(recovered.Table.Members);
        Assert.Equal(table.Version, recovered.Table.Version);
    }

    [Fact]
    public async Task MissingQuorumNeverAcknowledgesAndCancellationIsBounded()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var proposer = cluster.Proposer();
        _ = await proposer.ExecuteAsync(null, timeout.Token);
        await cluster.StopAsync(1);
        await cluster.StopAsync(2);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMilliseconds(700));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(1), null, table.Version.Next()), deadline.Token));
    }

    [Fact]
    public async Task WrongClusterOrSecretCannotObserveOrVote()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var request = new PeerRequest { Action = PeerAction.Inspect, Configuration = cluster.Configuration };
        Assert.Empty(await cluster.Transport(identity: "other-cluster").ExchangeAsync(request, cluster.Configuration, timeout.Token));
        Assert.Empty(await cluster.Transport(secret: "other-secret").ExchangeAsync(request, cluster.Configuration, timeout.Token));
        Assert.True((await cluster.Transport().ExchangeAsync(request, cluster.Configuration, timeout.Token)).Count >= 2);
    }

    [Fact]
    public async Task UnicodeAdmissionAndOversizedSnapshotKeepHealthyFollowUp()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster("секрет-для-кластера-0123456789abcdef");
        await cluster.StartAsync(timeout.Token);
        var proposer = cluster.Proposer(maximumSnapshotBytes: 4096);
        var entry = MutationTests.Entry(1);
        entry.HostName = new string('x', 8192);
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => proposer.ExecuteAsync(table =>
            MembershipMutations.Write(table, entry, null, table.Version.Next()), timeout.Token));
        var empty = await proposer.ExecuteAsync(null, timeout.Token);
        Assert.Empty(empty.Table.Members);
        var healthy = await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(1), null, table.Version.Next()), timeout.Token);
        Assert.True(healthy.Applied);
        _ = Assert.Single(healthy.Table.Members);
    }
}
