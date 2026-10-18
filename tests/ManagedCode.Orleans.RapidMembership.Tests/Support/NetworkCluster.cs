using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class NetworkCluster : IAsyncDisposable
{
    private readonly TestDirectory _directory = new();
    private readonly string _secret;
    private readonly ServiceProvider _services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
    private readonly MembershipHost?[] _hosts = new MembershipHost?[3];
    private readonly List<PeerTransport> _transports = [];
    private readonly List<MembershipProposer> _proposers = [];

    public NetworkCluster(string secret = "0123456789abcdef0123456789abcdef")
    {
        _secret = secret;
        var listeners = Enumerable.Range(0, 3).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
        {
            listener.Start();
        }

        Endpoints = [.. listeners.Select(listener => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}"))];
        foreach (var listener in listeners)
        {
            listener.Stop();
        }
    }

    public string[] Endpoints { get; }

    public IEnumerable<global::RapidCluster.ClusterMembershipView> VoterViews => _hosts.Where(host => host is not null).Select(host => host!.Cluster.CurrentView);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        for (var i = 0; i < _hosts.Length; i++)
        {
            var options = Options();
            options.ListenEndpoint = Endpoints[i];
            options.StorageDirectory = Path.Combine(_directory.Path, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _hosts[i] = new MembershipHost(options, options.Identity(new ClusterOptions { ClusterId = "test", ServiceId = "test" }));
        }

        await Task.WhenAll(_hosts.Select(host => host!.StartAsync(cancellationToken)));
        while (true)
        {
            var views = _hosts.Select(host => host!.Cluster.CurrentView).ToArray();
            if (views.All(view => view.Members.Length == _hosts.Length)
                && views.All(view => view.ConfigurationId == views[0].ConfigurationId))
            {
                return;
            }

            await Task.Delay(10, cancellationToken);
        }
    }

    public MembershipProposer Proposer(int maximumSnapshotBytes = 4194304, int maximumOperationHistory = 4096)
    {
        var options = Options();
        options.MaximumSnapshotBytes = maximumSnapshotBytes;
        options.MaximumOperationHistory = maximumOperationHistory;
        var proposer = new MembershipProposer(Transport(), options, _services.GetRequiredService<Serializer<MembershipSnapshot>>());
        _proposers.Add(proposer);
        return proposer;
    }

    public RapidMembershipTable NativeTable()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        var options = Options();
        options.ListenEndpoint = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"http://127.0.0.1:{port}");
        return new RapidMembershipTable(global::Microsoft.Extensions.Options.Options.Create(options),
            global::Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ClusterId = "test", ServiceId = "test" }),
            Proposer(), _services.GetRequiredService<Serializer<global::Orleans.MembershipEntry>>());
    }

    public PeerTransport Transport(string? secret = null, string? identity = null)
    {
        var options = Options();
        options.SharedSecret = secret ?? options.SharedSecret;
        var transport = new PeerTransport(options, identity ?? options.Identity(new ClusterOptions { ClusterId = "test", ServiceId = "test" }),
            _services.GetRequiredService<Serializer<PeerRequest>>(), _services.GetRequiredService<Serializer<PeerReply>>());
        _transports.Add(transport);
        return transport;
    }

    public async Task StopAsync(int index)
    {
        if (_hosts[index] is { } host)
        {
            await host.DisposeAsync();
            _hosts[index] = null;
        }
    }

    private RapidMembershipOptions Options()
    {
        var options = new RapidMembershipOptions
        {
            SharedSecret = _secret,
            PeerTimeout = TimeSpan.FromMilliseconds(500),
            SeedEndpoints = Endpoints,
        };
        return options;
    }

    public async ValueTask DisposeAsync()
    {
        for (var i = 0; i < _hosts.Length; i++)
        {
            await StopAsync(i);
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
        _directory.Dispose();
    }
}
