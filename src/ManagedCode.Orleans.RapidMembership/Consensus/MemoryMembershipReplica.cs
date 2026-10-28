using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal sealed class MemoryMembershipReplica(
    Guid incarnation, string identity, RapidMembershipOptions options,
    Serializer<PeerRequest> requests, Serializer<PeerReply> replies, Serializer<MembershipSnapshot> snapshots) : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1);
    private readonly ReplicaConfigurationFence _configuration = new(incarnation);
    private System.Collections.Immutable.ImmutableArray<FrozenReplica> _handoff = [];
    private AcceptorState _state = new() { Identity = identity };

    public async Task ObserveAsync(CoordinationConfiguration configuration, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            _configuration.Observe(configuration);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    // Called only by the host which created a new native RAPID cluster, never through a peer request.
    public async Task<bool> BootstrapAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return _configuration.Bootstrap();
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    public async Task<PeerReply> ExchangeAsync(PeerRequest request, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var input = requests.SerializeToArray(request);
            RequireBound(input.Length);
            var detached = requests.Deserialize(input) ?? throw new InvalidDataException("Empty replica request.");
            var granted = detached.Recipient == incarnation && Apply(detached);
            Diagnostics.MembershipMetrics.Exchange(detached.Action, granted);
            var output = replies.SerializeToArray(Reply(detached.Action, granted));
            RequireBound(output.Length);
            return replies.Deserialize(output) ?? throw new InvalidDataException("Empty replica reply.");
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private bool Apply(PeerRequest request)
    {
        if (request.Action is PeerAction.Describe or PeerAction.Locate or PeerAction.FetchTransfer)
        {
            return true;
        }

        if (request.Configuration is not { } configuration)
        {
            return false;
        }

        if (request.Action == PeerAction.FreezeConfiguration)
        {
            return request.PreviousConfiguration is { } previous && Freeze(previous, configuration, request);
        }

        if (request.Action == PeerAction.InstallConfiguration)
        {
            return request.PreviousConfiguration is { } previous && InstallConfiguration(previous, configuration, request);
        }

        if (!_configuration.Allows(configuration))
        {
            return false;
        }

        var next = new AcceptorState
        {
            Identity = _state.Identity,
            Committed = _state.Committed,
            Accepted = _state.Accepted,
            Promised = _state.Promised,
            AcceptedBallot = _state.AcceptedBallot,
        };
        var granted = MembershipBallot.Apply(next, request);
        if (granted)
        {
            ValidateSnapshot(next.Committed);
            if (next.Accepted is { } accepted)
            {
                ValidateSnapshot(accepted);
            }

            _state = next;
        }

        return granted;
    }

    private bool Freeze(CoordinationConfiguration previous, CoordinationConfiguration next, PeerRequest request)
    {
        if (_configuration.Freeze(previous, next))
        {
            return true;
        }

        // A real old voter's frozen view can fill a learner gap; installation still requires both full majorities.
        var witnesses = request.FrozenReplicas;
        if (witnesses.IsDefaultOrEmpty || witnesses.Length > previous.Members.Length)
        {
            return false;
        }

        var seen = new HashSet<Guid>();
        var valid = witnesses.All(witness => previous.Contains(witness.Incarnation) && seen.Add(witness.Incarnation)
            && witness.Previous.Matches(previous) && witness.Next.Matches(next) && witness.State.Identity == identity);
        return valid && witnesses.Any(witness => witness.Incarnation != incarnation)
            && _configuration.LearnNext(previous, next) && _configuration.Freeze(previous, next);
    }

    private bool InstallConfiguration(CoordinationConfiguration previous, CoordinationConfiguration next, PeerRequest request)
    {
        if (!_configuration.CanInstall(previous, next))
        {
            return false;
        }

        if (next.Matches(_configuration.Current))
        {
            return true;
        }

        var recovered = ConfigurationHandoff.Recover(previous, next, request.FrozenReplicas, identity, snapshots);
        ValidateSnapshot(recovered);
        if (recovered.Index < _state.Committed.Index || recovered.Index == _state.Committed.Index
            && !MembershipSnapshotEquality.AreEqual(recovered, _state.Committed))
        {
            return false;
        }

        _state = new AcceptorState { Identity = identity, Committed = recovered };
        _handoff = request.FrozenReplicas;
        _configuration.Installed(previous, next);
        return true;
    }

    private PeerReply Reply(PeerAction action, bool granted) => new()
    {
        Granted = granted,
        TransferEvidence = granted && action == PeerAction.FetchTransfer ? _handoff : [],
        Incarnation = incarnation,
        Configuration = _configuration.Current,
        FrozenFor = _configuration.FrozenFor,
        ObservedConfiguration = _configuration.Observed,
        NextConfiguration = _configuration.Next,
        State = granted && action is PeerAction.Describe or PeerAction.FreezeConfiguration or PeerAction.Inspect or PeerAction.Prepare ? _state : new AcceptorState { Promised = _state.Promised },
    };

    private void ValidateSnapshot(MembershipSnapshot snapshot)
    {
        if (snapshot.Table.Members.Count > options.MaximumEntries || snapshot.Outcomes.Count > options.MaximumOperationHistory
            || snapshots.SerializeToArray(snapshot).Length > options.MaximumSnapshotBytes)
        {
            throw new InvalidDataException("The replica snapshot exceeds the configured memory bounds.");
        }
    }

    private void RequireBound(int length)
    {
        if (length > options.MaximumExchangeBytes)
        {
            throw new InvalidDataException("The replica exchange exceeds the configured byte bound.");
        }
    }

    public void Dispose() => _gate.Dispose();
}
