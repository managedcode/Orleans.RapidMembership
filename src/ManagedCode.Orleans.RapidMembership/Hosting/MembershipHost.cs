using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using RapidCluster;
using RapidCluster.Grpc;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class MembershipHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly SemaphoreSlim _admissions;

    public MembershipHost(RapidMembershipOptions options, string identity)
    {
        var endpoint = new Uri(options.ListenEndpoint);
        var address = IPAddress.Parse(endpoint.Host.Trim('[', ']'));
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ApplicationName = typeof(MembershipHost).Assembly.GetName().Name,
            Args = [],
        });
        _ = builder.Logging.SetMinimumLevel(LogLevel.Warning);
        _ = builder.WebHost.ConfigureKestrel(server => server.Listen(address, endpoint.Port, listen => listen.Protocols = HttpProtocols.Http2));
        _ = builder.Services.AddSerializer(serializer => serializer.AddAssembly(typeof(MembershipHost).Assembly));
        _ = builder.Services.AddGrpc(grpc =>
        {
            grpc.MaxReceiveMessageSize = checked(options.MaximumSnapshotBytes * 3);
            grpc.MaxSendMessageSize = checked(options.MaximumSnapshotBytes * 3);
        });
        _ = builder.Services.AddSingleton(options);
        if (options.SeedEndpoints.Contains(options.ListenEndpoint, StringComparer.Ordinal))
        {
            _ = builder.Services.AddSingleton(services => new VoterStorage(options, identity, services.GetRequiredService<Serializer<AcceptorState>>()));
            _ = builder.Services.AddSingleton<MembershipAcceptor>();
        }

        _ = builder.Services.AddRapidCluster(rapid =>
        {
            rapid.SeedAddresses = [.. options.SeedEndpoints.Select(value => new Uri(value))
                .Select(uri => (EndPoint)new IPEndPoint(IPAddress.Parse(uri.Host.Trim('[', ']')), uri.Port))];
            rapid.BootstrapExpect = options.SeedEndpoints.Count;
            rapid.JoinBeforeBootstrap = true;
        }).UseGrpcTransport(grpc =>
        {
            grpc.Timeout = options.PeerTimeout;
            grpc.JoinTimeout = options.OperationTimeout;
            grpc.RequestHeaders.Add(PeerTransport.IdentityHeader, identity);
            grpc.RequestHeaders.Add(PeerTransport.SecretHeader, options.AdmissionToken);
        }).UseListenAddress(new IPEndPoint(address, endpoint.Port));
        _app = builder.Build();
        _admissions = new SemaphoreSlim(options.MaximumAdmissions);
        var secret = Encoding.UTF8.GetBytes(options.AdmissionToken);
        _ = _app.Use(async (context, next) =>
        {
            if (!string.Equals(context.Request.Headers[PeerTransport.IdentityHeader], identity, StringComparison.Ordinal)
                || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(context.Request.Headers[PeerTransport.SecretHeader].ToString()), secret))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            if (!await _admissions.WaitAsync(0, context.RequestAborted))
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                return;
            }

            try
            {
                await next(context);
            }
            finally
            {
                _ = _admissions.Release();
            }
        });
        _ = _app.MapRapidClusterMembershipService();
        if (options.SeedEndpoints.Contains(options.ListenEndpoint, StringComparer.Ordinal))
        {
            _ = _app.MapGrpcService<MembershipPeerService>();
        }
    }

    public IRapidCluster Cluster => _app.Services.GetRequiredService<IRapidCluster>();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _ = _app.Services.GetService<MembershipAcceptor>();
        await _app.StartAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await _app.StopAsync(timeout.Token);
        }
        finally
        {
            await _app.DisposeAsync();
            _admissions.Dispose();
        }
    }
}
