using Orleans;
using Orleans.Runtime;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class NativeProviderTests
{
    [Fact]
    public async Task NativeOperationsPreserveAtomicRowsHeartbeatsGatewaySelectionAndCleanup()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        await using var table = cluster.NativeTable();
        await table.InitializeMembershipTable(true);
        await table.InitializeMembershipTableAsync(true, timeout.Token);
        var entry = MutationTests.Entry(1);
        entry.ProxyPort = 30000;
        var empty = await table.ReadAll();
        Assert.Empty((await table.ReadRow(entry.SiloAddress)).Members);
        Assert.True(await table.InsertRow(entry, empty.Version.Next()));
        var joining = await table.ReadRowAsync(entry.SiloAddress, timeout.Token);
        var row = Assert.Single(joining.Members);
        Assert.False(await table.UpdateRowAsync(entry, "incorrect-etag", joining.Version.Next(), timeout.Token));
        entry.Status = SiloStatus.Active;
        Assert.True(await table.UpdateRow(entry, row.Item2, joining.Version.Next()));
        var active = await table.ReadAllAsync(timeout.Token);
        var gateway = new RapidGatewayListProvider(cluster.Proposer());
        await gateway.InitializeGatewayListProvider();
        Assert.True(gateway.IsUpdatable);
        Assert.Equal(TimeSpan.FromSeconds(5), gateway.MaxStaleness);
        Assert.Equal("gwy.tcp://127.0.0.1:30000/1", Assert.Single(await gateway.GetGateways()).AbsoluteUri);
        entry.IAmAliveTime = DateTime.UtcNow;
        await table.UpdateIAmAlive(entry);
        await table.UpdateIAmAliveAsync(entry, timeout.Token);
        var alive = await table.ReadRowAsync(entry.SiloAddress, timeout.Token);
        Assert.Equal(active.Version, alive.Version);
        Assert.Equal(entry.IAmAliveTime, Assert.Single(alive.Members).Item1.IAmAliveTime);
        entry.Status = SiloStatus.Dead;
        entry.SuspectTimes = [Tuple.Create(MutationTests.Entry(2).SiloAddress, DateTime.UtcNow)];
        Assert.True(await table.UpdateRowAsync(entry, Assert.Single(alive.Members).Item2, alive.Version.Next(), timeout.Token));
        await table.CleanupDefunctSiloEntriesAsync(DateTimeOffset.UtcNow.AddMinutes(-1), timeout.Token);
        _ = Assert.Single((await table.ReadAll()).Members);
        await table.CleanupDefunctSiloEntries(DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Empty((await table.ReadAll()).Members);
        Assert.Empty(await gateway.GetGateways());
        Assert.False(table.IsVoter);
        Assert.Empty(table.VoterView.Members);
        Assert.All(cluster.VoterViews, view => Assert.Equal(3, view.Members.Length));
    }

    [Fact]
    public async Task DeletionRejectsAnotherClusterAndCancellationKeepsHealthyFollowUp()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        await using var table = cluster.NativeTable();
        await table.InitializeMembershipTableAsync(true, timeout.Token);
        var empty = await table.ReadAllAsync(timeout.Token);
        Assert.True(await table.InsertRowAsync(MutationTests.Entry(1), empty.Version.Next(), timeout.Token));
        _ = await Assert.ThrowsAsync<ArgumentException>(() => table.DeleteMembershipTableEntries("other"));
        _ = Assert.Single((await table.ReadAll()).Members);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.ReadAllAsync(cancelled.Token));
        await table.DeleteMembershipTableEntriesAsync(cluster.ClusterId, timeout.Token);
        Assert.Empty((await table.ReadAll()).Members);
        await table.DeleteMembershipTableEntries(cluster.ClusterId);
        await table.DisposeAsync();
        await table.DisposeAsync();
    }
}
