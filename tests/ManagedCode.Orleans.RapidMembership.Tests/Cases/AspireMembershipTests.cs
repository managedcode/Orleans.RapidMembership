using System.Net;
using System.Net.Http.Json;
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
        await WaitForAsync(app, "silo-0", 3, cancellationToken);
        await WaitForAsync(app, "silo-1", 3, cancellationToken);
        await WaitForAsync(app, "silo-2", 3, cancellationToken);
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
        Assert.Equal("before", await grains.GetGrain<IProbeGrain>("before").Echo("before"));
        var commands = app.Services.GetRequiredService<ResourceCommandService>();
        var killed = await commands.ExecuteCommandAsync("silo-2", "crash", cancellationToken);
        Assert.True(killed.Success);
        await WaitForAsync(app, "silo-0", 2, cancellationToken);
        Assert.Equal("after", await grains.GetGrain<IProbeGrain>("after").Echo("after"));
        var restarted = await commands.ExecuteCommandAsync("silo-2", "resource-start", cancellationToken);
        Assert.True(restarted.Success);
        await WaitForAsync(app, "silo-2", 3, cancellationToken);
        Assert.Equal("rejoined", await grains.GetGrain<IProbeGrain>("rejoined").Echo("rejoined"));
        var firstSeedKilled = await commands.ExecuteCommandAsync("silo-0", "crash", cancellationToken);
        Assert.True(firstSeedKilled.Success);
        await WaitForAsync(app, "silo-1", 2, cancellationToken);
        Assert.Equal("seed-loss", await grains.GetGrain<IProbeGrain>("seed-loss").Echo("seed-loss"));
        var firstSeedRestarted = await commands.ExecuteCommandAsync("silo-0", "resource-start", cancellationToken);
        Assert.True(firstSeedRestarted.Success);
        await WaitForAsync(app, "silo-0", 3, cancellationToken);
        await WaitForAsync(app, "silo-1", 3, cancellationToken);
        Assert.Equal("seed-rejoined", await grains.GetGrain<IProbeGrain>("seed-rejoined").Echo("seed-rejoined"));
        var joined = await commands.ExecuteCommandAsync("silo-3", "resource-start", cancellationToken);
        Assert.True(joined.Success);
        await WaitForAsync(app, "silo-3", 4, cancellationToken);
        await WaitForAsync(app, "silo-0", 4, cancellationToken);
        Assert.Equal("non-voter", await grains.GetGrain<IProbeGrain>("non-voter").Echo("non-voter"));
        var left = await commands.ExecuteCommandAsync("silo-3", "resource-stop", cancellationToken);
        Assert.True(left.Success);
        await WaitForAsync(app, "silo-0", 3, cancellationToken);
        await clientHost.StopAsync(cancellationToken);
    }

    private static async Task WaitForAsync(DistributedApplication app, string resource, int active, CancellationToken cancellationToken)
    {
        using var client = app.CreateHttpClient(resource);
        client.Timeout = TimeSpan.FromMinutes(3);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var state = await client.GetFromJsonAsync<NodeState>("/state", cancellationToken);
                if (state.Active == active && state.RapidVoters == (state.IsVoter ? Math.Min(active, 3) : 0))
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
                if (app.ResourceNotifications.TryGetCurrentState(resource, out var current)
                    && current.Snapshot.State?.Text is "Exited" or "FailedToStart" or "Finished")
                {
                    throw new InvalidOperationException($"Resource {resource} exited before membership readiness.");
                }
            }

            await Task.Delay(100, cancellationToken);
        }
    }

    private readonly record struct NodeState(int Active, int Version, int RapidVoters, bool IsVoter);
}
