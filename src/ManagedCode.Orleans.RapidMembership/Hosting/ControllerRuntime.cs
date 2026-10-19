using System.Net;
using Microsoft.Extensions.Logging;
using RapidCluster;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class ControllerRuntime : IAsyncDisposable
{
    private static readonly Action<ILogger, string, int, int, Exception?> BootstrapProgress = LoggerMessage.Define<string, int, int>(
        LogLevel.Information, new EventId(1, "BootstrapElection"),
        "Bootstrap candidate {Candidate} agreed by {Agreed} of {Discovered} discovered peers.");

    private static readonly Action<ILogger, string, Exception?> MissingBootstrapPeers = LoggerMessage.Define<string>(
        LogLevel.Information, new EventId(2, "BootstrapPending"),
        "Awaiting bootstrap acknowledgements from {Peers} (at most three shown).");

    private static readonly Action<ILogger, string, Exception?> NativeLifecycle = LoggerMessage.Define<string>(
        LogLevel.Information, new EventId(3, "NativeLifecycle"), "RAPID controller lifecycle: {Stage}.");

    private static readonly Action<ILogger, long, int, Exception?> NativeView = LoggerMessage.Define<long, int>(
        LogLevel.Information, new EventId(4, "NativeView"), "RAPID native view {Version} contains {Members} coordinators.");

    private readonly MembershipHost _host;
    private readonly PeerDirectory _directory;
    private readonly CancellationTokenSource _stopping = new();
    private readonly CancellationTokenSource _observing = new();
    private readonly Task _control;
    private readonly Task _completion;
    private volatile bool _started;
    private volatile bool _created;
    private volatile bool _retired;
    private DateTimeOffset _bootstrapObserved;
    private CoordinationConfiguration? _observed;

    public ControllerRuntime(MembershipHost host, PeerDirectory directory)
    {
        _host = host;
        _directory = directory;
        _control = RunAsync(ControlAsync, _stopping.Token);
        _completion = Task.WhenAll(_control, RunAsync(ObserveAsync, _observing.Token));
    }

    public bool IsParticipant => _started && !_retired && _observed?.Contains(_host.Incarnation) == true;

    public Task StopWorkAsync() => _stopping.CancelAsync();

    public void ThrowIfFaulted()
    {
        if (_completion.IsFaulted)
        {
            throw new InvalidOperationException("Automatic RAPID coordination stopped.", _completion.Exception);
        }
    }

    private async Task ControlAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(_host.Options.BootstrapDelay, _host.Options.TimeProvider, cancellationToken);
        while (true)
        {
            var descriptions = await _directory.RefreshAsync(cancellationToken);
            if (!_started)
            {
                if (CanBootstrap(descriptions))
                {
                    _created = true;
                    await StartNativeAsync(cancellationToken);
                }
                else if (_host.Knowledge.Configuration is { } configuration
                    && configuration.Members.Count(member => !descriptions.Any(reply =>
                        reply.Incarnation == member.Incarnation && reply.IsDeparting)) < 3
                    && !descriptions.Any(reply => reply.IsJoining)
                    && descriptions.Where(reply => reply.CanCoordinate && !reply.IsDeparting && !configuration.Contains(reply.Incarnation))
                        .MinBy(reply => reply.Incarnation)?.Incarnation == _host.Incarnation)
                {
                    _host.Seeds.Set(configuration);
                    await StartNativeAsync(cancellationToken);
                }
            }

            if (_started && !_retired && _observed is { Members.Length: > 3 } excess
                && CanRetire(excess, _host.Incarnation, descriptions))
            {
                await DepartAsync(cancellationToken);
                _retired = true;
                _host.Participation.IsJoining = false;
            }

            if (_started && !_retired && _host.Participation.IsJoining)
            {
                var description = await _host.Replica.ExchangeAsync(new PeerRequest
                {
                    Action = PeerAction.Locate,
                    Recipient = _host.Incarnation,
                }, cancellationToken);
                _host.Participation.IsJoining = description.Configuration?.Contains(_host.Incarnation) != true;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), _host.Options.TimeProvider, cancellationToken);
        }
    }

    internal static bool CanRetire(CoordinationConfiguration configuration, Guid incarnation, IReadOnlyList<PeerReply> descriptions)
        => configuration.Members.Length > 3 && configuration.Members[^1].Incarnation == incarnation
            && !descriptions.Any(reply => reply.IsDeparting)
            && configuration.Members.All(member => descriptions.Any(reply => reply.Incarnation == member.Incarnation
                && reply.Granted && configuration.Matches(reply.Configuration)));

    private bool CanBootstrap(IReadOnlyList<PeerReply> descriptions)
    {
        var peers = _host.Discovery.Peers.Select(peer => peer.NodeId).Append(_host.Incarnation).Distinct().Order().ToArray();
        var lowest = peers[0];
        foreach (var previous in descriptions.Select(reply => reply.BootstrapCandidate).Append(_host.Participation.BootstrapCandidate))
        {
            if (Guid.TryParseExact(previous, "N", out var observed) && observed.CompareTo(lowest) < 0)
            {
                lowest = observed;
            }
        }

        var candidate = lowest.ToString("N");
        if (!_host.Discovery.InitialScanCompleted || !string.Equals(_host.Participation.BootstrapCandidate, candidate, StringComparison.Ordinal))
        {
            _bootstrapObserved = _host.Options.TimeProvider.GetUtcNow();
        }

        _host.Participation.BootstrapCandidate = candidate;
        var agreed = peers.Count(peer => descriptions.Any(reply => reply.Incarnation == peer
            && (peer == _host.Incarnation || string.Equals(reply.BootstrapCandidate, candidate, StringComparison.Ordinal))));
        BootstrapProgress(_host.RuntimeLogger, candidate, agreed, peers.Length, null);
        if (lowest == _host.Incarnation && agreed != peers.Length)
        {
            var missing = peers.Where(peer => !descriptions.Any(reply => reply.Incarnation == peer
                && (peer == _host.Incarnation || string.Equals(reply.BootstrapCandidate, candidate, StringComparison.Ordinal))))
                .Take(3).Select(peer => peer == _host.Incarnation ? "self" : peer.ToString("N"));
            MissingBootstrapPeers(_host.RuntimeLogger, string.Join(",", missing), null);
        }

        return !_host.Knowledge.HasObservedCluster && lowest == _host.Incarnation
            && _host.Discovery.InitialScanCompleted
            && _host.Options.TimeProvider.GetUtcNow() - _bootstrapObserved >= _host.Options.BootstrapDelay
            && !descriptions.Any(reply => reply.IsJoining)
            && agreed == peers.Length;
    }

    private async Task StartNativeAsync(CancellationToken cancellationToken)
    {
        _host.Participation.IsJoining = true;
        _host.Participation.CanCoordinate = false;
        await _host.Lifecycle.StartAsync(cancellationToken);
        _started = true;
    }

    private async Task ObserveAsync(CancellationToken cancellationToken)
    {
        await foreach (var view in _host.Cluster.ViewUpdates.WithCancellation(cancellationToken))
        {
            if (!view.Members.IsDefaultOrEmpty && (_observed is null || view.ConfigurationId.Version > _observed.Version))
            {
                var configuration = FromView(view);
                await _host.Replica.ObserveAsync(configuration, cancellationToken);
                _observed = configuration;
                NativeView(_host.RuntimeLogger, configuration.Version, configuration.Members.Length, null);
                if (_created && configuration.Members.Length == 1)
                {
                    _ = await _host.Replica.BootstrapAsync(cancellationToken);
                }
            }
        }
    }

    internal static CoordinationConfiguration FromView(ClusterMembershipView view)
    {
        var members = view.Members.Select(member =>
        {
            var incarnation = new Guid(member.Metadata["membership-incarnation"].Span);
            var endpoint = (IPEndPoint)member.EndPoint;
            return new CoordinationMember(incarnation, new UriBuilder(Uri.UriSchemeHttp, endpoint.Address.ToString(), endpoint.Port).Uri
                .GetLeftPart(UriPartial.Authority));
        }).OrderBy(member => member.Incarnation);
        var configuration = new CoordinationConfiguration(view.ConfigurationId.ClusterId.Value, view.ConfigurationId.Version, [.. members]);
        configuration.Validate();
        return configuration;
    }

    private async Task RunAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
    {
        try
        {
            await operation(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
            await _stopping.CancelAsync();
            await _observing.CancelAsync();
            throw;
        }
    }

    public async Task PrepareDepartureAsync()
    {
        _host.Participation.IsDeparting = true;
        _host.Participation.CanCoordinate = false;
        if (!_started || _retired)
        {
            return;
        }

        using var deadline = new CancellationTokenSource(_host.Options.OperationTimeout, _host.Options.TimeProvider);
        while (true)
        {
            var descriptions = await _directory.RefreshAsync(deadline.Token, force: true);
            var desired = Math.Min(3, descriptions.Count(reply => reply.Incarnation != _host.Incarnation && !reply.IsDeparting));
            var configuration = _host.Knowledge.Configuration;
            if (desired == 0 || configuration is not null && configuration.Members.Count(member => descriptions.Any(reply =>
                reply.Incarnation == member.Incarnation && reply.Incarnation != _host.Incarnation
                && !reply.IsDeparting && configuration.Matches(reply.Configuration))) >= desired)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), _host.Options.TimeProvider, deadline.Token);
        }
    }

    private async Task DepartAsync(CancellationToken cancellationToken)
    {
        NativeLifecycle(_host.RuntimeLogger, "leaving", null);
        await _host.Lifecycle.StopAsync(cancellationToken);
        NativeLifecycle(_host.RuntimeLogger, "native stop completed", null);
        var view = _host.Cluster.CurrentView;
        if (!view.Members.IsDefaultOrEmpty)
        {
            var configuration = FromView(view);
            if (!configuration.Contains(_host.Incarnation))
            {
                await _directory.CompleteDepartureAsync(configuration, cancellationToken);
                NativeLifecycle(_host.RuntimeLogger, "table handoff completed", null);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync();
        try
        {
            await _control;
            if (_started && !_retired && !_host.TransportIsStopping)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await DepartAsync(timeout.Token);
            }
        }
        finally
        {
            await _observing.CancelAsync();
            try
            {
                await _completion;
            }
            finally
            {
                _stopping.Dispose();
                _observing.Dispose();
            }
        }
    }
}
