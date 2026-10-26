using System.Net;
using System.Net.Sockets;
using ManagedCode.Orleans.RapidMembership.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class DirectoryLifecycleTests
{
    [Fact]
    public async Task OccupiedNativeListenersRefuseStartupAndTheSameDirectoriesRecoverAfterRelease()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(1));
        var token = timeout.Token;
        using var tcp = new TcpListener(IPAddress.Any, 0);
        tcp.Start();
        using var udp = new UdpClient(AddressFamily.InterNetwork) { ExclusiveAddressUse = true };
        udp.Client.Bind(new IPEndPoint(IPAddress.Any, 0));
        var identity = Guid.NewGuid().ToString("N");
        var options = new RapidMembershipOptions
        {
            SharedSecret = "0123456789abcdef0123456789abcdef",
            ListenPort = ((IPEndPoint)tcp.LocalEndpoint).Port,
            DiscoveryPort = ((IPEndPoint)udp.Client.LocalEndPoint!).Port,
        };
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly))
            .BuildServiceProvider();
        using var transport = new PeerTransport(options, identity, services.GetRequiredService<Serializer<PeerRequest>>(),
            services.GetRequiredService<Serializer<PeerReply>>());
        await using var server = new PeerDirectory(options, identity, transport);
        await using var client = new PeerDirectory(options, identity, transport);
        _ = await Assert.ThrowsAnyAsync<IOException>(() => server.StartServerAsync(token));
        Assert.Null(server.Host);
        _ = await Assert.ThrowsAnyAsync<SocketException>(() => client.ResolveAsync(token));
        tcp.Stop();
        udp.Dispose();
        await server.StartServerAsync(token);
        var configuration = await client.ResolveAsync(token);
        Assert.Equal(server.Host!.Incarnation, Assert.Single(configuration.Members).Incarnation);
        Assert.True(configuration.Matches(await server.ResolveAsync(token)));
        await server.DisposeAsync();
        await Task.Delay(TimeSpan.FromSeconds(6), token);
        await using var restarted = new PeerDirectory(options, identity, transport);
        await restarted.StartServerAsync(token);
        var fresh = await client.ResolveAsync(token);
        Assert.NotEqual(configuration.ClusterId, fresh.ClusterId);
        Assert.NotEqual(configuration.Members[0].Incarnation, Assert.Single(fresh.Members).Incarnation);
        Assert.True(fresh.Matches(await restarted.ResolveAsync(token)));
    }

    [Fact]
    public async Task QuorumConfirmedDepartureDoesNotRequireStoppedSuccessorsToAnswerAgain()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var network = new NetworkCluster();
        await network.StartAsync(token);
        var original = network.Configuration;
        var retiring = network.Node(0);
        var host = retiring.Host!;
        await host.PrepareDepartureAsync();
        await host.StopWorkAsync();
        await host.Lifecycle.StopAsync(token);
        var transferred = await retiring.ResolveAsync(token);
        while (transferred.Contains(host.Incarnation) || transferred.Members.Length != 2)
        {
            await Task.Delay(20, token);
            transferred = await retiring.ResolveAsync(token);
        }

        Assert.Equal(original.ClusterId, transferred.ClusterId);
        Assert.True(transferred.Version > original.Version);
        await network.StopAsync(1);
        await network.StopAsync(2);
        _ = await retiring.RefreshAsync(token, force: true);
        using var read = CancellationTokenSource.CreateLinkedTokenSource(token);
        read.CancelAfter(TimeSpan.FromMilliseconds(250));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => retiring.ResolveAsync(read.Token));
        using var confirmation = CancellationTokenSource.CreateLinkedTokenSource(token);
        confirmation.CancelAfter(TimeSpan.FromSeconds(1));
        await retiring.CompleteDepartureAsync(transferred, confirmation.Token);
        await network.DepartAsync(0);
        Assert.Empty(await network.Transport().LocateAsync(original.Members, token));
        await using var fresh = new NetworkCluster();
        await fresh.StartAsync(token);
        Assert.NotEqual(original.ClusterId, fresh.Configuration.ClusterId);
        Assert.All(fresh.Configuration.Members, member => Assert.DoesNotContain(original.Members,
            previous => previous.Incarnation == member.Incarnation));
    }
}
