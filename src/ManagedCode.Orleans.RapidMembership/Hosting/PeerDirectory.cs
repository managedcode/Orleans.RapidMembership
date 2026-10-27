using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class PeerDirectory(RapidMembershipOptions options, string identity, PeerTransport transport) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lifecycle = new(1);
    private readonly SemaphoreSlim _resolution = new(1);
    private readonly CancellationTokenSource _stopping = new();
    private readonly GroupKnowledge _knowledge = new();
    private readonly ConfigurationResolver _resolver = new(transport);
    private readonly PeerSelection _selection = new();
    private MulticastDiscovery? _observer;
    private IReadOnlyList<PeerReply> _descriptions = [];
    private CoordinationConfiguration? _resolved;

    // Departure confirmation retains quorum-resolved history; routing still requires the current result.
    private CoordinationConfiguration? _lastConfirmedConfiguration;
    private DateTimeOffset _refreshed;
    private int _disposed;

    public MembershipHost? Host { get; private set; }

    public async Task StartServerAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken);
        try
        {
            if (Host is null)
            {
                Host = new MembershipHost(options, identity, _knowledge);
                try
                {
                    await Host.StartAsync(cancellationToken);
                    Host.Run(this);
                }
                catch
                {
                    await Host.DisposeAsync();
                    Host = null;
                    throw;
                }
            }
        }
        finally
        {
            _ = _lifecycle.Release();
        }
    }

    public async Task WaitForFormationAsync(CancellationToken cancellationToken)
    {
        using var timer = new CancellationTokenSource(options.StartupTimeout, options.TimeProvider);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token, timer.Token);
        while (true)
        {
            var descriptions = await RefreshAsync(deadline.Token);
            if (_resolved is { } configuration && configuration.Members.Length >= Math.Min(3, descriptions.Count)
                && configuration.Members.All(member => descriptions.Any(reply => reply.Incarnation == member.Incarnation
                    && reply.Granted && configuration.Matches(reply.Configuration))))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), options.TimeProvider, deadline.Token);
        }
    }

    public async Task<CoordinationConfiguration> ResolveAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            _ = await RefreshAsync(cancellationToken);
            if (_resolved is { } configuration)
            {
                return configuration;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), options.TimeProvider, cancellationToken);
        }
    }

    public async Task<IReadOnlyList<PeerReply>> RefreshAsync(CancellationToken cancellationToken, bool force = false)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
        await _resolution.WaitAsync(linked.Token);
        try
        {
            Host?.ThrowIfFaulted();
            if (!force && options.TimeProvider.GetUtcNow() - _refreshed < TimeSpan.FromMilliseconds(500))
            {
                return _descriptions;
            }

            await StartObserverAsync(linked.Token);
            var peers = Host?.Discovery.Peers ?? _observer!.Peers;
            IEnumerable<CoordinationMember> targets = _selection.Select(peers, _knowledge.Configuration, Host?.Self, force, options.TimeProvider.GetUtcNow());

            if (Host is { } host)
            {
                targets = targets.Append(host.Self);
                if (!host.Cluster.CurrentView.Members.IsDefaultOrEmpty)
                {
                    targets = targets.Concat(peers.Where(peer => host.Cluster.CurrentView.Members.Any(member =>
                        new Guid(member.Metadata["membership-incarnation"].Span) == peer.NodeId))
                        .Select(peer => new CoordinationMember(peer.NodeId, peer.Endpoint)));
                }
            }

            _descriptions = await transport.LocateAsync(targets.DistinctBy(member => member.Incarnation), linked.Token);
            _selection.Observe(_descriptions);
            _knowledge.Learn(_descriptions, Host is not null);
            _resolved = await _resolver.ResolveAsync(_descriptions, linked.Token);
            var confirmed = Volatile.Read(ref _lastConfirmedConfiguration);
            if (_resolved is { } resolved && (confirmed is null || resolved.ClusterId != confirmed.ClusterId
                || resolved.Version > confirmed.Version))
            {
                Volatile.Write(ref _lastConfirmedConfiguration, resolved);
            }

            _refreshed = options.TimeProvider.GetUtcNow();
            return _descriptions;
        }
        finally
        {
            _ = _resolution.Release();
        }
    }

    private async Task StartObserverAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken);
        try
        {
            if (Host is null && _observer is null)
            {
                _observer = new MulticastDiscovery(identity, options.SharedSecret, 0, options.DiscoveryPort, options.MaximumPeers,
                    options.DiscoveryNetworks, options.MaximumDiscoveryAddresses, timeProvider: options.TimeProvider);
                try
                {
                    await _observer.StartAsync(cancellationToken);
                }
                catch
                {
                    await _observer.DisposeAsync();
                    _observer = null;
                    throw;
                }
            }
        }
        finally
        {
            _ = _lifecycle.Release();
        }
    }

    public async Task CompleteDepartureAsync(CoordinationConfiguration target, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var confirmed = Volatile.Read(ref _lastConfirmedConfiguration);
        if (confirmed is not null && confirmed.ClusterId == target.ClusterId && confirmed.Version >= target.Version)
        {
            return;
        }

        var previous = _knowledge.Configuration;
        var members = previous is null ? target.Members : previous.Members.Concat(target.Members);
        while (true)
        {
            var descriptions = await transport.LocateAsync(members.DistinctBy(member => member.Incarnation), cancellationToken);
            var installed = await _resolver.ResolveAsync(descriptions, cancellationToken);
            if (installed is not null && installed.ClusterId == target.ClusterId && installed.Version >= target.Version)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), options.TimeProvider, cancellationToken);
        }
    }

    public async Task CompleteTerminalDepartureAsync(CoordinationConfiguration terminal, CancellationToken cancellationToken)
    {
        var host = Host ?? throw new InvalidOperationException("Terminal departure requires the native membership server.");
        var previous = _knowledge.Configuration;
        var members = (previous is null ? terminal.Members : previous.Members)
            .Concat(host.Discovery.Peers.Select(peer => new CoordinationMember(peer.NodeId, peer.Endpoint)))
            .Where(member => !terminal.Contains(member.Incarnation))
            .DistinctBy(member => member.Incarnation).ToArray();
        var descriptions = await transport.LocateAsync(members, cancellationToken);
        if (!descriptions.Any(reply => reply.IsDeparting))
        {
            return;
        }

        await CompleteDepartureAsync(terminal, cancellationToken);
        while (descriptions.Any(reply => reply.IsDeparting))
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), options.TimeProvider, cancellationToken);
            descriptions = await transport.LocateAsync(members, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            await JoinWorkAsync();
        }
        finally
        {
            try
            {
                if (Host is not null)
                {
                    await Host.DisposeAsync();
                }
            }
            finally
            {
                try
                {
                    if (_observer is not null)
                    {
                        await _observer.DisposeAsync();
                    }
                }
                finally
                {
                    _resolution.Dispose();
                    _lifecycle.Dispose();
                    _stopping.Dispose();
                }
            }
        }
    }

    private async Task JoinWorkAsync()
    {
        try
        {
            if (Host is not null)
            {
                await Host.PrepareDepartureAsync();
            }
        }
        finally
        {
            if (Host is not null)
            {
                await Host.StopWorkAsync();
            }

            await _stopping.CancelAsync();
            await _resolution.WaitAsync();
        }
    }
}
