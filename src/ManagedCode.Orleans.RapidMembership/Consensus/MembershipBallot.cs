namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal static class MembershipBallot
{
    public static bool Apply(AcceptorState state, PeerRequest request)
    {
        if (request.Action == PeerAction.Inspect)
        {
            return true;
        }

        if (request.Action == PeerAction.Commit)
        {
            if (request.Snapshot is null || request.Snapshot.Index != request.Index)
            {
                return false;
            }

            if (request.Index == state.Committed.Index && !MembershipSnapshotEquality.AreEqual(state.Committed, request.Snapshot))
            {
                return false;
            }

            if (request.Index > state.Committed.Index)
            {
                Install(state, request.Snapshot);
            }

            return true;
        }

        if (request.Action == PeerAction.Prepare && request.Snapshot is { } previous && previous.Index > state.Committed.Index)
        {
            Install(state, previous);
        }

        if (request.Index != state.Committed.Index + 1 || request.Ballot.CompareTo(state.Promised) < 0)
        {
            return false;
        }

        if (request.Action == PeerAction.Accept)
        {
            if (request.Snapshot is null || request.Snapshot.Index != request.Index
                || state.Accepted is not null && request.Ballot == state.AcceptedBallot
                    && !MembershipSnapshotEquality.AreEqual(state.Accepted, request.Snapshot))
            {
                return false;
            }

            state.Accepted = request.Snapshot;
            state.AcceptedBallot = request.Ballot;
        }

        state.Promised = request.Ballot;
        return request.Action is PeerAction.Prepare or PeerAction.Accept;
    }

    private static void Install(AcceptorState state, MembershipSnapshot snapshot)
    {
        state.Committed = snapshot;
        state.Promised = default;
        state.AcceptedBallot = default;
        state.Accepted = null;
    }
}
