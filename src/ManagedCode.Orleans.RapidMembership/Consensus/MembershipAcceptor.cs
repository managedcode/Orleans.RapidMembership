using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal sealed class MembershipAcceptor(VoterStorage storage, Serializer<AcceptorState> serializer) : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1);
    private readonly VoterStorage _storage = storage;
    private readonly Serializer<AcceptorState> _serializer = serializer;
    private AcceptorState _state = storage.Load();

    public PeerReply Exchange(PeerRequest request)
    {
        _gate.Wait();
        try
        {
            return ExchangeCore(request);
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    public async Task<byte[]> ExchangeAsync(PeerRequest request, Serializer<PeerReply> replies, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            // Encode under admission: no mutable state escapes and no intermediate deep copy is needed.
            return replies.SerializeToArray(ExchangeCore(request, detach: false));
        }
        finally
        {
            _ = _gate.Release();
        }
    }

    private PeerReply ExchangeCore(PeerRequest request, bool detach = true)
    {
        // Apply changes state references only; accepted and committed snapshots are never mutated here.
        var next = request.Action == PeerAction.Inspect ? _state : new AcceptorState
        {
            Identity = _state.Identity,
            Promised = _state.Promised,
            AcceptedBallot = _state.AcceptedBallot,
            Accepted = _state.Accepted,
            Committed = _state.Committed,
        };
        var granted = MembershipBallot.Apply(next, request);
        Diagnostics.MembershipMetrics.Exchange(request.Action, granted);
        if (granted && request.Action != PeerAction.Inspect
            && (request.Action != PeerAction.Commit || request.Index > _state.Committed.Index))
        {
            _storage.Save(next);
            _state = next;
        }

        var replyState = request.Action is PeerAction.Inspect or PeerAction.Prepare
            ? detach ? _serializer.Deserialize(_serializer.SerializeToArray(_state)) ?? throw new InvalidDataException("Empty membership state.")
                : _state
            : new AcceptorState { Promised = _state.Promised };
        return new PeerReply { Granted = granted, State = replyState };
    }

    public void Dispose()
    {
        _gate.Dispose();
        _storage.Dispose();
    }
}
