using System.Net;

namespace RapidCluster.Integration.Tests;

/// <summary>
/// Real-network tests for a lost member whose endpoint is reused by a new process before the loss is detected.
/// </summary>
public sealed class RestartedEndpointTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private static readonly TimeSpan ViewTimeout = TimeSpan.FromSeconds(60);
    private readonly TestCluster _cluster = new(outputHelper);

    public async ValueTask DisposeAsync()
    {
        await _cluster.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static IPEndPoint CreateAddress(int port) => new(IPAddress.Parse("127.0.0.1"), port);

    [Fact]
    public async Task UnjoinedProcessOnLostMemberEndpointCannotKeepLostMemberInView()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var secondAddress = CreateAddress(_cluster.GetNextPort());
        var lostAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, cancellationToken);
        var (_, second) = await _cluster.CreateJoinerNodeAsync(secondAddress, seedAddress, cancellationToken);
        var (lostApp, _) = await _cluster.CreateSeedNodeAsync(lostAddress, options => options.SeedAddresses = [seedAddress], cancellationToken);
        await TestCluster.WaitForClusterSizeExactAsync(seed, 3, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(second, 3, ViewTimeout);
        var lostId = seed.CurrentView.Members.Single(member => member.EndPoint.Equals(lostAddress)).Id;

        // The lost process sends no RAPID leave; its transport disappears as in an abrupt crash.
        await lostApp.StopAsync(cancellationToken);
        await lostApp.DisposeAsync();

        // A new process binds the same endpoint immediately and answers probes before it joins.
        var (_, restartedLifecycle, restarted) = await _cluster.CreateUnstartedNodeAsync(lostAddress, seedAddress, cancellationToken);
        await TestCluster.WaitForClusterSizeExactAsync(seed, 2, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(second, 2, ViewTimeout);
        Assert.DoesNotContain(seed.CurrentView.Members, member => member.EndPoint.Equals(lostAddress));

        // The new process then joins at its own endpoint with a new member identity.
        await restartedLifecycle.StartAsync(cancellationToken);
        await TestCluster.WaitForClusterSizeExactAsync(seed, 3, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(second, 3, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(restarted, 3, ViewTimeout);
        var rejoined = seed.CurrentView.Members.Single(member => member.EndPoint.Equals(lostAddress));
        Assert.NotEqual(lostId, rejoined.Id);
        Assert.Equal(seed.CurrentView.ConfigurationId, restarted.CurrentView.ConfigurationId);
        Assert.Equal(seed.CurrentView.ConfigurationId, second.CurrentView.ConfigurationId);
    }
}
