using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class NetworkCluster(string secret = "0123456789abcdef0123456789abcdef") : IAsyncDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection()
        .AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();

    private readonly PeerDirectory?[] _nodes = new PeerDirectory?[3];
    private readonly List<PeerDirectory> _clients = [];
    private readonly List<PeerTransport> _transports = [];
    private readonly List<MembershipProposer> _proposers = [];
    private readonly ClusterOptions _cluster = new() { ClusterId = Guid.NewGuid().ToString("N"), ServiceId = "network-provider-tests" };

    public string ClusterId => _cluster.ClusterId;

    public CoordinationConfiguration Configuration { get; private set; } = null!;

    public IEnumerable<global::RapidCluster.ClusterMembershipView> VoterViews
        => _nodes.Where(node => node is not null).Select(node => node!.Host!.Cluster.CurrentView);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        for (var i = 0; i < _nodes.Length; i++)
        {
            var options = Options();
            _nodes[i] = Directory(options, Transport());
        }

        await Task.WhenAll(_nodes.Select(node => node!.StartServerAsync(cancellationToken)));
        while (true)
        {
            var configurations = await Task.WhenAll(_nodes.Select(node => node!.ResolveAsync(cancellationToken)));
            if (configurations.All(configuration => configuration.Members.Length == 3
                && configuration.Matches(configurations[0])) && VoterViews.All(view => view.Members.Length == 3))
            {
                Configuration = configurations[0];
                return;
            }

            await Task.Delay(20, cancellationToken);
        }
    }

    public MembershipProposer Proposer(int maximumSnapshotBytes = 4194304, int maximumOperationHistory = 4096)
    {
        var options = Options();
        options.MaximumSnapshotBytes = maximumSnapshotBytes;
        options.MaximumOperationHistory = maximumOperationHistory;
        var directory = Directory(options, Transport());
        _clients.Add(directory);
        var proposer = new MembershipProposer(_transports[^1], options,
            _services.GetRequiredService<Serializer<MembershipSnapshot>>(), directory);
        _proposers.Add(proposer);
        return proposer;
    }

    public RapidMembershipTable NativeTable() => new(global::Microsoft.Extensions.Options.Options.Create(Options()),
        global::Microsoft.Extensions.Options.Options.Create(_cluster), Proposer(), _services.GetRequiredService<Serializer<global::Orleans.MembershipEntry>>());

    public PeerTransport Transport(string? secret = null, string? identity = null)
    {
        var options = Options();
        options.SharedSecret = secret ?? options.SharedSecret;
        var transport = new PeerTransport(options, identity ?? RapidMembershipOptions.Identity(_cluster),
            _services.GetRequiredService<Serializer<PeerRequest>>(), _services.GetRequiredService<Serializer<PeerReply>>());
        _transports.Add(transport);
        return transport;
    }

    public async Task DepartAsync(int index)
    {
        await _nodes[index]!.DisposeAsync();
        _nodes[index] = null;
    }

    public async Task StopAsync(int index)
    {
        if (_nodes[index] is { Host: { } host } directory)
        {
            // Abruptly close the real server transport before joining cleanup; do not advertise a graceful departure first.
            var app = (WebApplication)typeof(MembershipHost).GetField("_app",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(host)!;
            app.Lifetime.StopApplication();
            await app.Services.GetRequiredService<IServer>().StopAsync(TestContext.Current.CancellationToken);
            await directory.DisposeAsync();
            _nodes[index] = null;
        }
    }

    private PeerDirectory Directory(RapidMembershipOptions options, PeerTransport transport)
        => new(options, RapidMembershipOptions.Identity(_cluster), transport);

    private RapidMembershipOptions Options() => new()
    {
        SharedSecret = secret,
        ListenPort = 0,
        PeerTimeout = TimeSpan.FromSeconds(2),
    };

    public async ValueTask DisposeAsync()
    {
        foreach (var client in _clients)
        {
            await client.DisposeAsync();
        }

        for (var index = 0; index < _nodes.Length; index++)
        {
            await StopAsync(index);
        }

        foreach (var proposer in _proposers)
        {
            proposer.Dispose();
        }

        foreach (var transport in _transports)
        {
            transport.Dispose();
        }

        await _services.DisposeAsync();
    }
}
