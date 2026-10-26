using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using RapidCluster;
using RapidCluster.Discovery;
using RapidCluster.Grpc;
using RapidCluster.Messaging;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class LifecycleCancellationTests
{
    [Fact]
    public async Task CancelledPeerJoinNeverReturnsSuccessfulInitialization()
    {
        using var seed = new TcpListener(IPAddress.Loopback, 0);
        using var local = new TcpListener(IPAddress.Loopback, 0);
        seed.Start();
        local.Start();
        var seedPort = ((IPEndPoint)seed.LocalEndpoint).Port;
        var localPort = ((IPEndPoint)local.LocalEndpoint).Port;
        local.Stop();
        var controlTimeout = TimeSpan.FromSeconds(30);
        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, localPort, listen => listen.Protocols = HttpProtocols.Http2));
        _ = builder.Services.AddRapidClusterManual(rapid => rapid.SeedAddresses = [new IPEndPoint(IPAddress.Loopback, seedPort)])
            .UseGrpcTransport(grpc =>
            {
                grpc.Timeout = controlTimeout;
                grpc.JoinTimeout = TimeSpan.FromSeconds(2);
            }).UseListenAddress(new IPEndPoint(IPAddress.Loopback, localPort));
        var observation = new RpcCancellationObservation();
        _ = builder.Services.AddSingleton(observation);
        _ = builder.Services.AddGrpc(grpc => grpc.Interceptors.Add<RpcCancellationObservation>());
        await using var app = builder.Build();
        _ = app.MapRapidClusterMembershipService();
        await app.StartAsync(TestContext.Current.CancellationToken);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => app.Services.GetRequiredService<IRapidClusterLifecycle>().StartAsync(timeout.Token));
        var messaging = app.Services.GetRequiredService<IMessagingClient>();
        var endpoint = new RapidCluster.Pb.Endpoint { Hostname = ByteString.CopyFromUtf8("127.0.0.1"), Port = localPort };
        var connected = await messaging.SendMessageAsync(endpoint, new RapidCluster.Pb.RapidClusterRequest
        {
            MembershipViewRequest = new RapidCluster.Pb.MembershipViewRequest { Sender = endpoint },
        }, TestContext.Current.CancellationToken);
        Assert.NotNull(connected.MembershipViewResponse);
        var pendingJoin = new RapidCluster.Pb.RapidClusterRequest
        {
            JoinMessage = new RapidCluster.Pb.JoinMessage
            {
                Sender = new RapidCluster.Pb.Endpoint { Hostname = endpoint.Hostname, Port = seedPort },
                ConfigurationId = new RapidCluster.Pb.ConfigurationId(),
            },
        };
        _ = await Assert.ThrowsAsync<TimeoutException>(() => messaging.SendMessageAsync(endpoint, pendingJoin, TestContext.Current.CancellationToken));
        Assert.True(await observation.Finished.Task.WaitAsync(controlTimeout, TestContext.Current.CancellationToken));
        var read = await messaging.SendMessageAsync(endpoint, new RapidCluster.Pb.RapidClusterRequest
        {
            MembershipViewRequest = new RapidCluster.Pb.MembershipViewRequest { Sender = endpoint },
        }, TestContext.Current.CancellationToken);
        Assert.NotNull(read.MembershipViewResponse);
        await app.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DisposedMessagingClientClosesRealConnectionAndServerAcceptsNewClient()
    {
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0, listen =>
        {
            listen.Protocols = HttpProtocols.Http2;
            _ = listen.Use(next => async connection =>
            {
                try
                {
                    await next(connection);
                }
                finally
                {
                    _ = disconnected.TrySetResult();
                }
            });
        }));
        _ = builder.Services.AddRapidClusterManual().UseGrpcTransport();
        _ = builder.Services.AddSingleton<IListenAddressProvider>(services => new ServerListenAddressProvider(
            services.GetRequiredService<IServer>(), preferHttps: false, preferredHost: "127.0.0.1"));
        await using var app = builder.Build();
        _ = app.MapRapidClusterMembershipService();
        var token = TestContext.Current.CancellationToken;
        await app.StartAsync(token);
        var port = ((IPEndPoint)app.Services.GetRequiredService<IListenAddressProvider>().ListenAddress).Port;
        var messaging = app.Services.GetRequiredService<IMessagingClient>();
        var endpoint = new RapidCluster.Pb.Endpoint { Hostname = ByteString.CopyFromUtf8("127.0.0.1"), Port = port };
        var request = new RapidCluster.Pb.RapidClusterRequest
        {
            MembershipViewRequest = new RapidCluster.Pb.MembershipViewRequest { Sender = endpoint },
        };
        var responses = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => messaging.SendMessageAsync(endpoint, request, token)));
        Assert.All(responses, response => Assert.NotNull(response.MembershipViewResponse));
        await messaging.DisposeAsync();
        await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
        await messaging.DisposeAsync();
        using var channel = GrpcChannel.ForAddress(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"http://127.0.0.1:{port}"));
        var client = new RapidCluster.Pb.MembershipService.MembershipServiceClient(channel);
        using var call = client.SendRequestAsync(request, cancellationToken: token);
        Assert.NotNull((await call.ResponseAsync).MembershipViewResponse);
        await app.StopAsync(token);
    }
}
