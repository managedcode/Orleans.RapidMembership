using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Node.Contracts;
using ManagedCode.Orleans.RapidMembership.Node.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class NativeHostTests
{
    [Fact]
    public async Task PublicRegistrationsStartARealSiloAndExternalClientWithDurableMembership()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var directory = new TestDirectory();
        var listeners = Enumerable.Range(0, 3).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
        {
            listener.Start();
        }

        var ports = listeners.Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
        {
            listener.Stop();
        }

        var endpoint = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"http://127.0.0.1:{ports[0]}");
        var clusterId = Guid.NewGuid().ToString("N");
        var siloBuilder = Host.CreateApplicationBuilder();
        _ = siloBuilder.UseOrleans(silo =>
        {
            _ = silo.Configure<ClusterOptions>(options => options.ClusterId = clusterId);
            _ = silo.Configure<GrainTypeOptions>(options => options.AddClass(typeof(ProbeGrain)));
            _ = silo.ConfigureEndpoints(IPAddress.Loopback, ports[1], ports[2]);
            _ = silo.UseRapidMembership(options =>
            {
                options.SeedEndpoints = [endpoint];
                options.ListenEndpoint = endpoint;
                options.StorageDirectory = directory.Path;
                options.SharedSecret = "0123456789abcdef0123456789abcdef";
            });
        });
        using var siloHost = siloBuilder.Build();
        await siloHost.StartAsync(timeout.Token);
        try
        {
            var clientBuilder = Host.CreateApplicationBuilder();
            _ = clientBuilder.UseOrleansClient(client =>
            {
                _ = client.Configure<ClusterOptions>(options => options.ClusterId = clusterId);
                _ = client.UseRapidMembership(options =>
                {
                    options.SeedEndpoints = [endpoint];
                    options.SharedSecret = "0123456789abcdef0123456789abcdef";
                });
            });
            using var clientHost = clientBuilder.Build();
            await clientHost.StartAsync(timeout.Token);
            var grains = clientHost.Services.GetRequiredService<IClusterClient>();
            Assert.Equal("native-client", await grains.GetGrain<IProbeGrain>("native-host").Echo("native-client"));
            var table = siloHost.Services.GetRequiredService<RapidMembershipTable>();
            Assert.True(table.IsVoter);
            _ = Assert.Single((await table.ReadAllAsync(timeout.Token)).Members);
            _ = Assert.Single(table.VoterView.Members);
            await clientHost.StopAsync(timeout.Token);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await siloHost.StopAsync(cleanup.Token);
        }
    }
}
