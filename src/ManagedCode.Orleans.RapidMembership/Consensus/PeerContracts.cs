using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.Ballot")]
internal readonly record struct Ballot([property: Id(0)] long Counter, [property: Id(1)] string Proposer) : IComparable<Ballot>
{
    public int CompareTo(Ballot other)
    {
        var order = Counter.CompareTo(other.Counter);
        return order == 0 ? string.CompareOrdinal(Proposer, other.Proposer) : order;
    }
}

internal enum PeerAction
{
    Inspect,
    Prepare,
    Accept,
    Commit,
    Describe,
    Locate,
    FetchTransfer,
    FreezeConfiguration,
    InstallConfiguration,
}

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.Snapshot")]
internal sealed class MembershipSnapshot
{
    [Id(0)] public long Index { get; set; }

    [Id(1)] public Guid OperationId { get; set; }

    [Id(2)] public bool Applied { get; set; }

    [Id(3)] public MembershipTableData Table { get; set; } = new(new TableVersion(0, "initial"));

    [Id(4)] public List<CompletedMutation> Outcomes { get; set; } = [];

    [Id(5)] public long OutcomeFloor { get; set; }
}

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.AcceptorState")]
internal sealed class AcceptorState
{
    [Id(0)] public string Identity { get; set; } = string.Empty;

    [Id(1)] public MembershipSnapshot Committed { get; set; } = new();

    [Id(2)] public Ballot Promised { get; set; }

    [Id(3)] public Ballot AcceptedBallot { get; set; }

    [Id(4)] public MembershipSnapshot? Accepted { get; set; }
}

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.Request")]
internal sealed class PeerRequest
{
    [Id(0)] public PeerAction Action { get; set; }

    [Id(1)] public long Index { get; set; }

    [Id(2)] public Ballot Ballot { get; set; }

    [Id(3)] public MembershipSnapshot? Snapshot { get; set; }

    [Id(4)] public CoordinationConfiguration? Configuration { get; set; }

    [Id(5)] public CoordinationConfiguration? PreviousConfiguration { get; set; }

    [Id(6)] public System.Collections.Immutable.ImmutableArray<FrozenReplica> FrozenReplicas { get; set; } = [];

    [Id(7)] public Guid Recipient { get; set; }
}

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.Reply")]
internal sealed class PeerReply
{
    [Id(0)] public bool Granted { get; set; }

    // Inspect/Prepare return complete state; Accept/Commit acknowledge with the current promise only.
    [Id(1)] public AcceptorState State { get; set; } = new();

    [Id(2)] public CoordinationConfiguration? Configuration { get; set; }

    [Id(3)] public CoordinationConfiguration? FrozenFor { get; set; }

    [Id(4)] public Guid Incarnation { get; set; }

    [Id(5)] public CoordinationConfiguration? ObservedConfiguration { get; set; }

    [Id(6)] public CoordinationConfiguration? KnownConfiguration { get; set; }

    [Id(7)] public bool IsJoining { get; set; }

    [Id(8)] public System.Collections.Immutable.ImmutableArray<FrozenReplica> TransferEvidence { get; set; } = [];

    [Id(9)] public bool CanCoordinate { get; set; }

    [Id(10)] public string BootstrapCandidate { get; set; } = string.Empty;

    [Id(11)] public bool IsDeparting { get; set; }
}

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.CompletedMutation")]
internal readonly record struct CompletedMutation([property: Id(0)] Guid OperationId, [property: Id(1)] long Index, [property: Id(2)] bool Applied);
