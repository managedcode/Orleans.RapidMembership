using System.Text.Json;
using Aspire.Hosting.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;
using Xunit;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class AspireChurnTests(ITestOutputHelper output)
{
    [Fact]
    public async Task AllCoordinatorsGracefullyLeaveTogetherAndTheirReplacementsKeepServingTheSameClients()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false", "--TestCluster:Nodes=6"], token);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(token);
        await app.StartAsync(token);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        await cluster.ChangeAsync(Enumerable.Range(3, 3), "resource-start", token);
        _ = await cluster.ConvergeAsync([0, 1, 2, 3, 4, 5], token);
        await using var clients = new ContinuousClients(evidence.Write);
        await clients.StartAsync(await cluster.EnvironmentAsync(token), token);
        await CheckAsync("all-coordinators-before-departure", [0, 1, 2, 3, 4, 5], cluster, clients, token);
        var controllers = await cluster.VotersAsync([0, 1, 2, 3, 4, 5], token);
        await cluster.ChangeAsync(controllers, "shutdown", token);
        var survivors = Enumerable.Range(0, 6).Except(controllers).ToArray();
        await CheckAsync("all-coordinators-replaced", survivors, cluster, clients, token);
        await cluster.ChangeAsync(controllers, "resource-start", token);
        await CheckAsync("departed-coordinators-return", [0, 1, 2, 3, 4, 5], cluster, clients, token);
    }

    [Fact]
    public async Task RealSiloShutdownJoinsZeroExitAndRestartRecoversExternalClients()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false", "--TestCluster:Nodes=4"], token);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(token);
        await app.StartAsync(token);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        await cluster.ChangeAsync([3], "resource-start", token);
        _ = await cluster.ConvergeAsync([0, 1, 2, 3], token);
        await using var clients = new ContinuousClients(evidence.Write);
        await clients.StartAsync(await cluster.EnvironmentAsync(token), token);
        await CheckAsync("graceful-initial-4", [0, 1, 2, 3], cluster, clients, token);
        await cluster.ChangeAsync([3], "shutdown", token);
        await CheckAsync("graceful-surviving-3", [0, 1, 2], cluster, clients, token);
        await cluster.ChangeAsync([3], "resource-start", token);
        await CheckAsync("graceful-return-4", [0, 1, 2, 3], cluster, clients, token);
    }

    [Fact, Trait("Category", "Scale")]
    public async Task FiftyRealSilosRecoverFromZeroAndConcurrentChurnWithContinuousClients()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(45));
        var token = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false", "--TestCluster:Nodes=50", "--TestCluster:ExplicitStart=true"], token);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(token);
        await app.StartAsync(token);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        await cluster.RequireEmptyAsync(token);
        await cluster.ChangeAsync(Enumerable.Range(0, 10), "resource-start", token);
        _ = await cluster.ConvergeAsync([.. Enumerable.Range(0, 10)], token);
        await using var clients = new ContinuousClients(evidence.Write);
        await clients.StartAsync(await cluster.EnvironmentAsync(token), token);
        await CheckAsync("initial-10", [.. Enumerable.Range(0, 10)], cluster, clients, token);
        await cluster.ChangeAsync(Enumerable.Range(10, 10), "resource-start", token);
        await CheckAsync("grow-20", [.. Enumerable.Range(0, 20)], cluster, clients, token);
        var controllers = await cluster.VotersAsync([.. Enumerable.Range(0, 20)], token);
        var survivors = controllers.Concat(Enumerable.Range(0, 20).Except(controllers)).Take(5).Order().ToArray();
        var departed = Enumerable.Range(0, 20).Except(survivors).ToArray();
        await cluster.ChangeAsync(departed, "crash", token);
        await CheckAsync("crash-to-5", survivors, cluster, clients, token);
        await cluster.ChangeAsync(departed, "resource-start", token);
        await CheckAsync("return-20", [.. Enumerable.Range(0, 20)], cluster, clients, token);
        controllers = await cluster.VotersAsync([.. Enumerable.Range(0, 20)], token);
        var concurrentDepartures = Enumerable.Range(0, 20).Except(controllers).Take(10).ToArray();
        var concurrentLive = Enumerable.Range(0, 20).Except(concurrentDepartures).Concat(Enumerable.Range(20, 10)).Order().ToArray();
        await Task.WhenAll(cluster.ChangeAsync(Enumerable.Range(20, 10), "resource-start", token),
            cluster.ChangeAsync(concurrentDepartures, "crash", token));
        await CheckAsync("concurrent-10-join-10-crash", concurrentLive, cluster, clients, token);
        await cluster.ChangeAsync(concurrentLive, "crash", token);
        await cluster.RequireEmptyAsync(token);
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        var successes = clients.Successes;
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        Assert.Equal(successes, clients.Successes);
        await clients.RequireUnavailableManagementAsync(token);
        output.WriteLine("All 50 resources stopped; existing clients remain alive across complete cluster loss.");
        await cluster.ChangeAsync(Enumerable.Range(0, 10), "resource-start", token);
        await CheckAsync("cold-return-10", [.. Enumerable.Range(0, 10)], cluster, clients, token);
        await cluster.ChangeAsync(Enumerable.Range(10, 40), "resource-start", token);
        await CheckAsync("grow-50", [.. Enumerable.Range(0, 50)], cluster, clients, token);
        await cluster.ChangeAsync(Enumerable.Range(25, 25), "shutdown", token);
        await CheckAsync("graceful-to-25", [.. Enumerable.Range(0, 25)], cluster, clients, token);
        await cluster.ChangeAsync(Enumerable.Range(25, 25), "resource-start", token);
        await CheckAsync("graceful-return-50", [.. Enumerable.Range(0, 50)], cluster, clients, token);
        Assert.True(clients.Silos.Length >= 50, $"Expected execution on at least 50 silo incarnations; observed {clients.Silos.Length}.");
    }

    [Fact, Trait("Category", "Scale")]
    public async Task LostVoterMajorityRejectsFreshIncarnationsUntilCompleteClusterRestart()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(10));
        var token = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false", "--TestCluster:Nodes=10"], token);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(token);
        await app.StartAsync(token);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        await cluster.ChangeAsync(Enumerable.Range(3, 7), "resource-start", token);
        var environment = await cluster.EnvironmentAsync(token);
        await using var clients = new ContinuousClients(evidence.Write);
        await clients.StartAsync(environment, token);
        await CheckAsync("quorum-initial-10", [.. Enumerable.Range(0, 10)], cluster, clients, token);
        var initialMembers = await cluster.ConvergeAsync([.. Enumerable.Range(0, 10)], token);
        var controllers = await cluster.VotersAsync([.. Enumerable.Range(0, 10)], token);
        var lost = controllers.Take(2).ToArray();
        await cluster.ChangeAsync(lost, "crash", token);
        await using var serializers = new ServiceCollection().AddSerializer(configuration => configuration.AddAssembly(typeof(RapidMembershipOptions).Assembly))
            .BuildServiceProvider();
        var options = new RapidMembershipOptions
        {
            SharedSecret = environment["RAPID_SECRET"],
            PeerTimeout = TimeSpan.FromMilliseconds(500),
            OperationTimeout = TimeSpan.FromSeconds(2),
        };
        using var transport = new PeerTransport(options, RapidMembershipOptions.Identity(new ClusterOptions { ClusterId = environment["CLUSTER_ID"], ServiceId = "rapid-membership-tests" }),
            serializers.GetRequiredService<Serializer<PeerRequest>>(), serializers.GetRequiredService<Serializer<PeerReply>>());
        await using var directory = new PeerDirectory(options, RapidMembershipOptions.Identity(new ClusterOptions
        {
            ClusterId = environment["CLUSTER_ID"],
            ServiceId = "rapid-membership-tests",
        }), transport);
        using var proposer = new MembershipProposer(transport, options, serializers.GetRequiredService<Serializer<MembershipSnapshot>>(), directory);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => proposer.ExecuteAsync(null, token));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => proposer.ExecuteAsync(table => table, token));
        await cluster.ChangeAsync([lost[0]], "resource-start", token);
        await Task.Delay(TimeSpan.FromSeconds(4), token);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => proposer.ExecuteAsync(table => table, token));
        int[] remaining = [.. Enumerable.Range(0, 10).Except(lost), lost[0]];
        await cluster.ChangeAsync(remaining, "crash", token);
        await cluster.RequireEmptyAsync(token, resources: 10);
        await cluster.ChangeAsync(Enumerable.Range(0, 10), "resource-start", token);
        await CheckAsync("fresh-epoch-after-total-loss-10", [.. Enumerable.Range(0, 10)], cluster, clients, token);
        var restoredMembers = await cluster.ConvergeAsync([.. Enumerable.Range(0, 10)], token);
        Assert.Empty(initialMembers.Intersect(restoredMembers, StringComparer.Ordinal));
    }

    [Fact, Trait("Category", "Scale")]
    public async Task FiftyRealSilosStartTogetherFromZeroAndServeExternalClients()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(15));
        var token = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false", "--TestCluster:Nodes=50", "--TestCluster:ExplicitStart=true"], token);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(token);
        await app.StartAsync(token);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        await cluster.RequireEmptyAsync(token);
        await cluster.ChangeAsync(Enumerable.Range(0, 50), "resource-start", token);
        _ = await cluster.ConvergeAsync([.. Enumerable.Range(0, 50)], token);
        await using var clients = new ContinuousClients(evidence.Write);
        await clients.StartAsync(await cluster.EnvironmentAsync(token), token);
        await CheckAsync("cold-start-50", [.. Enumerable.Range(0, 50)], cluster, clients, token);
    }

    private async Task CheckAsync(string phase, int[] live, AspireChurnCluster cluster, ContinuousClients clients, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromMinutes(10));
        var members = await cluster.ConvergeAsync(live, deadline.Token);
        await clients.RequireProgressAsync(deadline.Token);
        while (members.Except(clients.Silos, StringComparer.Ordinal).Any())
        {
            await clients.RequireProgressAsync(deadline.Token);
        }

        var managementHosts = await clients.RequireHostsAsync(members, deadline.Token);
        Assert.All(managementHosts, known => Assert.Equal(members, known));
        output.WriteLine(JsonSerializer.Serialize(new
        {
            phase,
            managementHosts,
            ManagementActive = managementHosts.Select(known => known.Length).ToArray(),
            live,
            members,
            clients.Successes,
            TransientFailures = clients.Failures,
            ExecutingSilos = clients.Silos,
        }));
    }
}
