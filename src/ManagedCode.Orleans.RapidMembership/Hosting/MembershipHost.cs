using System.Net;
using System.Security.Cryptography;
using System.Text;
using ManagedCode.Orleans.RapidMembership.Discovery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using RapidCluster;
using RapidCluster.Discovery;
using RapidCluster.Grpc;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class MembershipHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly SemaphoreSlim _admissions;
    private ControllerRuntime? _runtime;

    public MembershipHost(RapidMembershipOptions options, string identity, GroupKnowledge knowledge)
    {
        Options = options;
        Knowledge = knowledge;
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ApplicationName = typeof(MembershipHost).Assembly.GetName().Name,
            Args = [],
        });
        _ = builder.Logging.SetMinimumLevel(LogLevel.Warning);
        _ = builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Any, options.ListenPort,
            listen => listen.Protocols = HttpProtocols.Http2));
        _ = builder.Services.AddSerializer(serializer => serializer.AddAssembly(typeof(MembershipHost).Assembly));
        _ = builder.Services.AddGrpc(grpc =>
        {
            grpc.MaxReceiveMessageSize = options.MaximumExchangeBytes;
            grpc.MaxSendMessageSize = options.MaximumExchangeBytes;
        });
        _ = builder.Services.AddSingleton(options);
        _ = builder.Services.AddSingleton(knowledge);
        _ = builder.Services.AddSingleton(Participation);
        _ = builder.Services.AddSingleton(services => new MemoryMembershipReplica(Incarnation, identity, options,
            services.GetRequiredService<Serializer<PeerRequest>>(), services.GetRequiredService<Serializer<PeerReply>>(),
            services.GetRequiredService<Serializer<MembershipSnapshot>>()));
        _ = builder.Services.AddRapidClusterManual(rapid =>
        {
            rapid.Metadata["membership-incarnation"] = Incarnation.ToByteArray();
            rapid.BootstrapClusterId = new ClusterId(BitConverter.ToInt64(Incarnation.ToByteArray()) | 1);
        })
            .UseGrpcTransport(grpc =>
            {
                grpc.Timeout = options.PeerTimeout;
                grpc.JoinTimeout = options.OperationTimeout;
                grpc.RequestHeaders.Add(PeerTransport.IdentityHeader, identity);
                grpc.RequestHeaders.Add(PeerTransport.SecretHeader, options.AdmissionToken);
            });
        _ = builder.Services.AddSingleton<ISeedProvider>(Seeds);
        _ = builder.Services.Configure<RapidClusterProtocolOptions>(protocol => protocol.LeaveMessageTimeout = options.OperationTimeout);
        _ = builder.Services.Configure<RapidCluster.Monitoring.PingPongFailureDetectorOptions>(detector =>
        {
            detector.ProbeTimeout = options.PeerTimeout;
            detector.Interval = options.PeerTimeout + TimeSpan.FromSeconds(1);
        });
        var advertisedAddress = HostAddressResolver.ResolveIPAddressOrDefault() ?? IPAddress.Loopback;
        _ = builder.Services.AddSingleton<IListenAddressProvider>(services => new ServerListenAddressProvider(
            services.GetRequiredService<IServer>(), preferHttps: false, preferredHost: advertisedAddress.ToString()));
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
        _ = _app.MapGrpcService<MembershipPeerService>();
        Identity = identity;
    }

    public RapidMembershipOptions Options { get; }

    public string Identity { get; }

    public Guid Incarnation { get; } = Guid.NewGuid();

    public AutomaticSeeds Seeds { get; } = new();

    public HostParticipation Participation { get; } = new();

    public GroupKnowledge Knowledge { get; }

    public CoordinationMember Self { get; private set; }

    public MulticastDiscovery Discovery { get; private set; } = null!;

    public IRapidCluster Cluster => _app.Services.GetRequiredService<IRapidCluster>();

    public IRapidClusterLifecycle Lifecycle => _app.Services.GetRequiredService<IRapidClusterLifecycle>();

    public MemoryMembershipReplica Replica => _app.Services.GetRequiredService<MemoryMembershipReplica>();

    public bool TransportIsStopping => _app.Lifetime.ApplicationStopping.IsCancellationRequested;

    public ILogger<ControllerRuntime> RuntimeLogger => _app.Services.GetRequiredService<ILogger<ControllerRuntime>>();

    public bool IsVoter => _runtime?.IsParticipant == true;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _app.StartAsync(cancellationToken);
        var bound = (IPEndPoint)_app.Services.GetRequiredService<IListenAddressProvider>().ListenAddress;
        var endpoint = new UriBuilder(Uri.UriSchemeHttp, bound.Address.ToString(), bound.Port).Uri;
        Self = new CoordinationMember(Incarnation, endpoint.GetLeftPart(UriPartial.Authority));
        Discovery = new MulticastDiscovery(Identity, Options.SharedSecret, endpoint.Port, Options.DiscoveryPort, Options.MaximumPeers,
            Options.DiscoveryNetworks, Options.MaximumDiscoveryAddresses, Incarnation);
        await Discovery.StartAsync(cancellationToken);
    }

    public void Run(PeerDirectory directory) => _runtime = new ControllerRuntime(this, directory);

    public Task StopWorkAsync() => _runtime?.StopWorkAsync() ?? Task.CompletedTask;

    public Task PrepareDepartureAsync() => TransportIsStopping ? Task.CompletedTask : _runtime?.PrepareDepartureAsync() ?? Task.CompletedTask;

    public void ThrowIfFaulted() => _runtime?.ThrowIfFaulted();

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_runtime is not null)
            {
                await _runtime.DisposeAsync();
            }
        }
        finally
        {
            try
            {
                if (Discovery is not null)
                {
                    await Discovery.DisposeAsync();
                }
            }
            finally
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
    }
}
