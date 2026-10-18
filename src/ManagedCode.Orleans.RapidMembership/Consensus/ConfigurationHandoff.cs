using System.Collections.Immutable;
using Orleans;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.FrozenReplica")]
internal sealed record FrozenReplica(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] CoordinationConfiguration Previous,
    [property: Id(2)] CoordinationConfiguration Next,
    [property: Id(3)] AcceptorState State);

internal static class ConfigurationHandoff
{
    public static MembershipSnapshot Recover(CoordinationConfiguration previous, CoordinationConfiguration next,
        ImmutableArray<FrozenReplica> frozen, string identity, Serializer<MembershipSnapshot> serializer)
    {
        Validate(previous, next, frozen, identity);
        var committed = frozen.MaxBy(replica => replica.State.Committed.Index)!.State.Committed;
        var accepted = frozen.Where(replica => replica.State.Accepted is { } value && value.Index > committed.Index)
            .OrderByDescending(replica => replica.State.AcceptedBallot).ToArray();
        RequireConsistentCommits(frozen, committed);
        var selected = SelectAccepted(accepted) ?? committed;
        return serializer.Deserialize(serializer.SerializeToArray(selected)) ?? throw new InvalidDataException("Empty handoff snapshot.");
    }

    public static bool HasInstalledQuorum(CoordinationConfiguration next, IEnumerable<Guid> acknowledgements)
    {
        next.Validate();
        var seen = new HashSet<Guid>();
        foreach (var incarnation in acknowledgements)
        {
            if (!next.Contains(incarnation) || !seen.Add(incarnation))
            {
                return false;
            }
        }

        return seen.Count >= next.Quorum;
    }

    private static void Validate(CoordinationConfiguration previous, CoordinationConfiguration next,
        ImmutableArray<FrozenReplica> frozen, string identity)
    {
        previous.Validate();
        next.Validate();
        if (previous.ClusterId != next.ClusterId || previous.Version >= next.Version
            || frozen.IsDefaultOrEmpty || frozen.Length < previous.Quorum || frozen.Length > previous.Members.Length)
        {
            throw new InvalidDataException("Handoff requires a frozen majority of the previous configuration.");
        }

        var seen = new HashSet<Guid>();
        foreach (var replica in frozen)
        {
            var state = replica.State;
            if (!previous.Contains(replica.Incarnation) || !seen.Add(replica.Incarnation)
                || !replica.Previous.Matches(previous) || !replica.Next.Matches(next) || state.Identity != identity
                || state.Committed.Index < 0 || state.Accepted is { } accepted
                    && (state.Committed.Index == long.MaxValue || accepted.Index != state.Committed.Index + 1
                        || state.AcceptedBallot.CompareTo(state.Promised) > 0))
            {
                throw new InvalidDataException("Handoff evidence does not match the frozen replicas and configurations.");
            }
        }
    }

    private static void RequireConsistentCommits(ImmutableArray<FrozenReplica> replicas, MembershipSnapshot committed)
    {
        if (replicas.Any(replica => replica.State.Committed.Index == committed.Index
            && !MembershipSnapshotEquality.AreEqual(replica.State.Committed, committed)))
        {
            throw new InvalidDataException("Conflicting committed snapshots cannot be transferred.");
        }
    }

    private static MembershipSnapshot? SelectAccepted(FrozenReplica[] replicas)
    {
        if (replicas.Length == 0)
        {
            return null;
        }

        var first = replicas[0].State;
        if (replicas.Any(replica => replica.State.AcceptedBallot == first.AcceptedBallot
            && !MembershipSnapshotEquality.AreEqual(replica.State.Accepted!, first.Accepted!)))
        {
            throw new InvalidDataException("One accepted ballot identifies conflicting snapshots.");
        }

        return first.Accepted;
    }
}
