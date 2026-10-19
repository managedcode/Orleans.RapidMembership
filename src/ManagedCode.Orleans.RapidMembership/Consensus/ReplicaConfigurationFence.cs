namespace ManagedCode.Orleans.RapidMembership.Consensus;

// The owning acceptor serializes these transitions together with its ballot and snapshot changes.
internal sealed class ReplicaConfigurationFence(Guid incarnation)
{
    private const int MaximumPendingMembers = 10000;
    private readonly SortedDictionary<long, CoordinationConfiguration> _pending = [];
    private int _pendingMembers;

    public CoordinationConfiguration? Current { get; private set; }

    public CoordinationConfiguration? Observed { get; private set; }

    public CoordinationConfiguration? FrozenFor { get; private set; }

    public CoordinationConfiguration? Next => Current is { Version: < long.MaxValue } current
        && _pending.TryGetValue(current.Version + 1, out var next) ? next : null;

    public void Observe(CoordinationConfiguration configuration)
    {
        configuration.Validate();
        if (Observed is { } previous && (previous.ClusterId != configuration.ClusterId
            || previous.Version > configuration.Version
            || previous.Version == configuration.Version && !previous.Matches(configuration)))
        {
            throw new InvalidOperationException("RAPID configurations cannot move backwards or switch cluster identity.");
        }

        if ((Current is null || configuration.Version > Current.Version) && !_pending.ContainsKey(configuration.Version))
        {
            if (_pendingMembers + configuration.Members.Length > MaximumPendingMembers)
            {
                throw new InvalidDataException("Pending RAPID views exceed the retained membership bound.");
            }

            _pending.Add(configuration.Version, configuration);
            _pendingMembers += configuration.Members.Length;
        }

        Observed = configuration;
    }

    public bool Bootstrap()
    {
        if (Current is not null || Observed is not { Members.Length: 1 } configuration || !configuration.Contains(incarnation))
        {
            return false;
        }

        Current = configuration;
        Prune();
        return true;
    }

    public bool Allows(CoordinationConfiguration configuration) => FrozenFor is null
        && Current is { } current && current.Contains(incarnation)
        && current.Matches(configuration) && current.Matches(Observed);

    public bool Freeze(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        if (!IsSuccessor(previous, next) || Current is not { } current || !current.Matches(previous)
            || !current.Contains(incarnation))
        {
            return false;
        }

        // A frozen old view can transfer only to its original successor, even after newer RAPID views arrive.
        if (FrozenFor is { } frozen)
        {
            return frozen.Matches(next);
        }

        if (!next.Matches(Next))
        {
            return false;
        }

        FrozenFor = next;
        return true;
    }

    public bool CanInstall(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        if (!IsSuccessor(previous, next) || !next.Contains(incarnation) || !CanRecoverAgreedView(next))
        {
            return false;
        }

        // Repeated installation preserves newer promises; validated old-majority evidence
        // also lets a lagging replica catch up through each agreed configuration.
        return Current is { } current
            ? current.Matches(next) && FrozenFor is null || current.Matches(previous)
                || current.ClusterId == previous.ClusterId && current.Version < previous.Version
            : !previous.Contains(incarnation);
    }

    public void Installed(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        if (!CanInstall(previous, next))
        {
            throw new InvalidOperationException("The replica cannot install this coordination configuration.");
        }

        Current = next;
        FrozenFor = null;
        Prune();
    }

    private void Prune()
    {
        foreach (var version in _pending.Keys.TakeWhile(version => version <= Current!.Version).ToArray())
        {
            _pendingMembers -= _pending[version].Members.Length;
            _ = _pending.Remove(version);
        }
    }

    // Installation still requires validated frozen-majority evidence; this only checks local eligibility.
    private bool CanRecoverAgreedView(CoordinationConfiguration next) => Observed is { } observed
        && observed.ClusterId == next.ClusterId && observed.Version >= next.Version
        && (observed.Version > next.Version || observed.Matches(next));

    private static bool IsSuccessor(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        previous.Validate();
        next.Validate();
        return next.ImmediatelyFollows(previous);
    }
}
