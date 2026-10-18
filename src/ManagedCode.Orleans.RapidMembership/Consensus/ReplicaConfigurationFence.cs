namespace ManagedCode.Orleans.RapidMembership.Consensus;

// The owning acceptor serializes these transitions together with its ballot and snapshot changes.
internal sealed class ReplicaConfigurationFence(Guid incarnation)
{
    public CoordinationConfiguration? Current { get; private set; }

    public CoordinationConfiguration? Observed { get; private set; }

    public CoordinationConfiguration? FrozenFor { get; private set; }

    public void Observe(CoordinationConfiguration configuration)
    {
        configuration.Validate();
        if (Observed is { } previous && (previous.ClusterId != configuration.ClusterId
            || previous.Version > configuration.Version
            || previous.Version == configuration.Version && !previous.Matches(configuration)))
        {
            throw new InvalidOperationException("RAPID configurations cannot move backwards or switch cluster identity.");
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

        if (!next.Matches(Observed))
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

        // Repeated installation acknowledges the view without clearing newer ballot promises or table slots.
        return Current is { } current
            ? current.Matches(next) && FrozenFor is null || current.Matches(previous) && next.Matches(FrozenFor)
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
    }

    // Installation still requires validated frozen-majority evidence; this only checks local eligibility.
    private bool CanRecoverAgreedView(CoordinationConfiguration next) => Observed is { } observed
        && observed.ClusterId == next.ClusterId && observed.Version >= next.Version
        && (observed.Version > next.Version || observed.Matches(next));

    private static bool IsSuccessor(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        previous.Validate();
        next.Validate();
        return previous.ClusterId == next.ClusterId && next.Version > previous.Version;
    }
}
