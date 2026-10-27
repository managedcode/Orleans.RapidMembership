using System.Net;

using Microsoft.Extensions.DependencyInjection;

namespace RapidCluster.Integration.Tests;

/// <summary>
/// Real-network tests for graceful membership departure, concurrent joins and reused process endpoints.
/// </summary>
public sealed class MembershipEndpointLifecycleTests : IAsyncDisposable
{
    private static readonly TimeSpan ViewTimeout = TimeSpan.FromSeconds(60);
    private readonly LeaveMessageDeliveryGate _leaveMessages = new();
    private readonly TestCluster _cluster;

    public MembershipEndpointLifecycleTests(ITestOutputHelper outputHelper)
    {
        ArgumentNullException.ThrowIfNull(outputHelper);
        _cluster = new TestCluster(outputHelper, services =>
        {
            _ = services.AddSingleton(_leaveMessages);
            _ = services.AddGrpc(options => options.Interceptors.Add<LeaveMessageDeliveryGate>());
        });
    }

    public async ValueTask DisposeAsync()
    {
        await _cluster.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static IPEndPoint CreateAddress(int port) => new(IPAddress.Parse("127.0.0.1"), port);

    [Theory]
    [InlineData(0, false)]
    [InlineData(20, false)]
    [InlineData(50, false)]
    [InlineData(20, true)]
    public async Task GracefulDepartureRemainsEffectiveWhenConcurrentJoinsChangeItsObservers(int joinDelayMilliseconds, bool holdFirstLeave)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(ViewTimeout);
        var token = timeout.Token;
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var secondAddress = CreateAddress(_cluster.GetNextPort());
        var departingAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, token);
        var (_, second) = await _cluster.CreateJoinerNodeAsync(secondAddress, seedAddress, token);
        var (departingApp, departing) = await _cluster.CreateSeedNodeAsync(departingAddress,
            options => options.SeedAddresses = [seedAddress], token);
        await TestCluster.WaitForClusterSizeExactAsync(departing, 3, ViewTimeout);
        var original = departing.CurrentView.ConfigurationId;
        departingApp.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<RapidClusterProtocolOptions>>()
            .Value.LeaveMessageTimeout = TimeSpan.FromSeconds(5);
        var joiningAddresses = Enumerable.Range(0, 6).Select(_ => CreateAddress(_cluster.GetNextPort())).ToArray();
        var newNodes = await Task.WhenAll(joiningAddresses.Select(address =>
            _cluster.CreateUnstartedNodeAsync(address, seedAddress, token)));
        if (holdFirstLeave)
        {
            _leaveMessages.HoldNext();
        }

        var joining = Task.WhenAll(newNodes.Select(node => node.Lifecycle.StartAsync(token)));
        await Task.WhenAll(joining, DepartAfterJoinDelayAsync());
        Assert.Equal(holdFirstLeave, _leaveMessages.Entered.IsCompletedSuccessfully);
        Assert.DoesNotContain(departing.CurrentView.Members, member => member.EndPoint.Equals(departingAddress));
        var survivors = new[] { seed, second }.Concat(newNodes.Select(node => node.Cluster)).ToArray();
        var expectedPorts = joiningAddresses.Prepend(secondAddress).Prepend(seedAddress).Select(address => address.Port).Order().ToArray();
        foreach (var survivor in survivors)
        {
            await TestCluster.WaitForClusterSizeExactAsync(survivor, 8, ViewTimeout);
            Assert.DoesNotContain(survivor.CurrentView.Members, member => member.EndPoint.Equals(departingAddress));
            Assert.Equal(expectedPorts, survivor.CurrentView.Members.Select(member => ((IPEndPoint)member.EndPoint).Port).Order());
            Assert.Equal(original.ClusterId, survivor.CurrentView.ConfigurationId.ClusterId);
            Assert.Equal(seed.CurrentView.ConfigurationId, survivor.CurrentView.ConfigurationId);
        }

        var healthyAddress = CreateAddress(_cluster.GetNextPort());
        var (_, healthy) = await _cluster.CreateJoinerNodeAsync(healthyAddress, seedAddress, token);
        await TestCluster.WaitForClusterSizeExactAsync(healthy, 9, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(seed, 9, ViewTimeout);
        Assert.Equal(seed.CurrentView.ConfigurationId, healthy.CurrentView.ConfigurationId);
        Assert.Equal(expectedPorts.Append(healthyAddress.Port).Order(),
            healthy.CurrentView.Members.Select(member => ((IPEndPoint)member.EndPoint).Port).Order());

        async Task DepartAfterJoinDelayAsync()
        {
            await Task.Delay(joinDelayMilliseconds, token);
            await departingApp.Services.GetRequiredService<IRapidClusterLifecycle>().StopAsync(token);
        }
    }

    [Fact]
    public async Task UnjoinedProcessOnLostMemberEndpointCannotKeepLostMemberInView()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var secondAddress = CreateAddress(_cluster.GetNextPort());
        var lostAddress = CreateAddress(_cluster.GetNextPort());
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, cancellationToken);
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

        // The surviving quorum must also tolerate a subsequent abrupt loss of the first seed.
        await seedApp.StopAsync(cancellationToken);
        await seedApp.DisposeAsync();
        await TestCluster.WaitForClusterSizeExactAsync(second, 2, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(restarted, 2, ViewTimeout);
        var (_, returnedSeed) = await _cluster.CreateJoinerNodeAsync(seedAddress, secondAddress, cancellationToken);
        await TestCluster.WaitForClusterSizeExactAsync(returnedSeed, 3, ViewTimeout);
        await TestCluster.WaitForClusterSizeExactAsync(second, 3, ViewTimeout);
        Assert.Equal(second.CurrentView.ConfigurationId, returnedSeed.CurrentView.ConfigurationId);
        Assert.Equal(second.CurrentView.ConfigurationId, restarted.CurrentView.ConfigurationId);
    }
}
