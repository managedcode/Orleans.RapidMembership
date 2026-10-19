namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class HostParticipation
{
    private int _joining;
    private int _eligible = 1;
    private int _departing;

    public bool IsDeparting
    {
        get => Volatile.Read(ref _departing) != 0;
        set => Volatile.Write(ref _departing, value ? 1 : 0);
    }

    public string? BootstrapCandidate
    {
        get => Volatile.Read(ref field);
        set => Volatile.Write(ref field, value);
    }

    public bool CanCoordinate
    {
        get => Volatile.Read(ref _eligible) != 0;
        set => Volatile.Write(ref _eligible, value ? 1 : 0);
    }

    public bool IsJoining
    {
        get => Volatile.Read(ref _joining) != 0;
        set => Volatile.Write(ref _joining, value ? 1 : 0);
    }
}
