using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class ReplicaNetwork : IAsyncDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection()
        .AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();

    private readonly List<WebApplication> _servers = [];
    private readonly List<CoordinationMember> _members = [];

    private ReplicaNetwork()
    {
        Transport = new PeerTransport(Replicas.Options, "cluster",
        _services.GetRequiredService<Serializer<PeerRequest>>(), _services.GetRequiredService<Serializer<PeerReply>>());
    }

    public MemoryReplicaSet Replicas { get; } = new();

    public PeerTransport Transport { get; }

    public CoordinationMember[] Members => [.. _members.OrderBy(member => member.Incarnation)];

    public static async Task<ReplicaNetwork> StartAsync(CancellationToken cancellationToken)
    {
        var network = new ReplicaNetwork();
        try
        {
            for (var index = 0; index < 3; index++)
            {
                await network.AddAsync(cancellationToken);
            }

            return network;
        }
        catch
        {
            await network.DisposeAsync();
            throw;
        }
    }

    private async Task AddAsync(CancellationToken cancellationToken)
    {
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var port = ((IPEndPoint)reservation.LocalEndpoint).Port;
        var member = new CoordinationMember(Guid.NewGuid(), new UriBuilder(Uri.UriSchemeHttp, "127.0.0.1", port).Uri.GetLeftPart(UriPartial.Authority));
        var replica = Replicas.Add(member);
        _members.Add(member);
        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, port, listen => listen.Protocols = HttpProtocols.Http2));
        _ = builder.Services.AddSerializer(serializer => serializer.AddAssembly(typeof(RapidMembershipOptions).Assembly));
        _ = builder.Services.AddGrpc();
        _ = builder.Services.AddSingleton(replica);
        _ = builder.Services.AddSingleton(new GroupKnowledge());
        _ = builder.Services.AddSingleton(new HostParticipation());
        var app = builder.Build();
        _servers.Add(app);
        _ = app.MapGrpcService<MembershipPeerService>();
        reservation.Stop();
        await app.StartAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        Transport.Dispose();
        try
        {
            await Task.WhenAll(_servers.Select(async app =>
            {
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    await app.StopAsync(timeout.Token);
                }
                finally
                {
                    await app.DisposeAsync();
                }
            }));
        }
        finally
        {
            Replicas.Dispose();
            await _services.DisposeAsync();
        }
    }
}
