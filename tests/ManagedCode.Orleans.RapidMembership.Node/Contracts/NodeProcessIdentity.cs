using System.Diagnostics;

namespace ManagedCode.Orleans.RapidMembership.Node.Contracts;

internal sealed record NodeProcessIdentity(int ProcessId, long StartTimeUtcTicks)
{
    public static NodeProcessIdentity Capture()
    {
        using var process = Process.GetCurrentProcess();
        return new NodeProcessIdentity(process.Id, process.StartTime.ToUniversalTime().Ticks);
    }
}
