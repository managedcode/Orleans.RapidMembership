namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class GracefulHandoffTests
{
    [Fact]
    public async Task GracefulThreeToTwoToOnePreservesRowsAndTotalLossStartsFreshWithTheSameClient()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(token);
        var proposer = cluster.Proposer();
        var original = await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(1), null, table.Version.Next()), token);
        Assert.Equal(1, original.Table.Version.Version);
        await cluster.DepartAsync(2);
        _ = Assert.Single((await proposer.ExecuteAsync(null, token)).Table.Members);
        Assert.All(cluster.VoterViews, view => Assert.Equal(2, view.Members.Length));
        await cluster.DepartAsync(1);
        _ = Assert.Single((await proposer.ExecuteAsync(null, token)).Table.Members);
        _ = Assert.Single((await proposer.ExecuteAsync(table => table, token)).Table.Members);
        _ = Assert.Single(Assert.Single(cluster.VoterViews).Members);
        await cluster.DepartAsync(0);
        await Task.Delay(TimeSpan.FromSeconds(6), token);
        await cluster.StartAsync(token);
        Assert.Empty((await proposer.ExecuteAsync(null, token)).Table.Members);
        Assert.Empty((await proposer.ExecuteAsync(table => table, token)).Table.Members);
        Assert.All(cluster.VoterViews, view => Assert.Equal(3, view.Members.Length));
    }
}
