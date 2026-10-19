using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using ManagedCode.Orleans.RapidMembership.Node.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class AspireMembershipTests(ITestOutputHelper output)
{
    [Fact]
    public async Task ThreeRealSilosDiscoverGatewaysSurviveAbruptLossAndRejoin()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(6));
        var cancellationToken = timeout.Token;
        await using var builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ManagedCode_Orleans_RapidMembership_AppHost>(
            ["--DcpPublisher:RandomizePorts=false"], cancellationToken);
        _ = builder.Services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        await using var app = await builder.BuildAsync(cancellationToken);
        await app.StartAsync(cancellationToken);
        await using var evidence = new ChurnEvidence(output, app);
        var cluster = new AspireChurnCluster(app, evidence.Write);
        _ = await cluster.ConvergeAsync([0, 1, 2], cancellationToken);
        var resource = app.Services.GetRequiredService<DistributedApplicationModel>().Resources.OfType<ProjectResource>().Single(node => node.Name == "silo-0");
        var configuration = await ExecutionConfigurationBuilder.Create(resource).WithEnvironmentVariablesConfig()
            .BuildAsync(new DistributedApplicationExecutionContext(DistributedApplicationOperation.Run),
                Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, cancellationToken);
        Assert.Null(configuration.Exception);
        var environment = configuration.EnvironmentVariables.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
        var hostBuilder = Host.CreateApplicationBuilder();
        _ = hostBuilder.UseOrleansClient(client =>
        {
            _ = client.Configure<ClusterOptions>(cluster =>
            {
                cluster.ClusterId = environment["CLUSTER_ID"];
                cluster.ServiceId = "rapid-membership-tests";
            });
            _ = client.UseRapidMembership(options => options.SharedSecret = environment["RAPID_SECRET"]);
        });
        using var clientHost = hostBuilder.Build();
        await clientHost.StartAsync(cancellationToken);
        var grains = clientHost.Services.GetRequiredService<IClusterClient>();
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1, 2], cancellationToken), cancellationToken);
        Assert.Equal("before", await grains.GetGrain<IProbeGrain>("before").Echo("before"));
        var commands = app.Services.GetRequiredService<ResourceCommandService>();
        var killed = await commands.ExecuteCommandAsync("silo-2", "crash", cancellationToken);
        Assert.True(killed.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1], cancellationToken), cancellationToken);
        Assert.Equal("after", await grains.GetGrain<IProbeGrain>("after").Echo("after"));
        var restarted = await commands.ExecuteCommandAsync("silo-2", "resource-start", cancellationToken);
        Assert.True(restarted.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1, 2], cancellationToken), cancellationToken);
        Assert.Equal("rejoined", await grains.GetGrain<IProbeGrain>("rejoined").Echo("rejoined"));
        var firstSeedKilled = await commands.ExecuteCommandAsync("silo-0", "crash", cancellationToken);
        Assert.True(firstSeedKilled.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([1, 2], cancellationToken), cancellationToken);
        Assert.Equal("seed-loss", await grains.GetGrain<IProbeGrain>("seed-loss").Echo("seed-loss"));
        var firstSeedRestarted = await commands.ExecuteCommandAsync("silo-0", "resource-start", cancellationToken);
        Assert.True(firstSeedRestarted.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1, 2], cancellationToken), cancellationToken);
        Assert.Equal("seed-rejoined", await grains.GetGrain<IProbeGrain>("seed-rejoined").Echo("seed-rejoined"));
        var joined = await commands.ExecuteCommandAsync("silo-3", "resource-start", cancellationToken);
        Assert.True(joined.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1, 2, 3], cancellationToken), cancellationToken);
        Assert.Equal("non-voter", await grains.GetGrain<IProbeGrain>("non-voter").Echo("non-voter"));
        var left = await commands.ExecuteCommandAsync("silo-3", "resource-stop", cancellationToken);
        Assert.True(left.Success);
        _ = await ContinuousClients.ReadHostsAsync(grains, await cluster.ConvergeAsync([0, 1, 2], cancellationToken), cancellationToken);
        await clientHost.StopAsync(cancellationToken);
    }
}
