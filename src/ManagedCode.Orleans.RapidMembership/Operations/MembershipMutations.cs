using Orleans;
using Orleans.Runtime;

namespace ManagedCode.Orleans.RapidMembership.Operations;

internal static class MembershipMutations
{
    public static MembershipTableData? Write(MembershipTableData current, MembershipEntry entry, string? etag, TableVersion version)
    {
        var row = current.TryGet(entry.SiloAddress);
        if (version.VersionEtag != current.Version.VersionEtag || version.Version != checked(current.Version.Version + 1)
            || (etag is null ? row is not null : row is null || row.Item2 != etag))
        {
            return null;
        }

        var rows = current.Members.Where(item => !item.Item1.SiloAddress.Equals(entry.SiloAddress)).ToList();
        rows.Add(Tuple.Create(entry, Guid.NewGuid().ToString("N")));
        return new MembershipTableData(rows, new TableVersion(version.Version, Guid.NewGuid().ToString("N")));
    }

    public static MembershipTableData? Heartbeat(MembershipTableData current, MembershipEntry entry)
    {
        var row = current.TryGet(entry.SiloAddress);
        if (row is null)
        {
            return null;
        }

        row.Item1.IAmAliveTime = entry.IAmAliveTime > row.Item1.IAmAliveTime ? entry.IAmAliveTime : row.Item1.IAmAliveTime;
        return current;
    }

    public static MembershipTableData? Cleanup(MembershipTableData current, DateTimeOffset beforeDate)
    {
        var rows = current.Members.Where(row => row.Item1.Status != SiloStatus.Dead || LastUpdate(row.Item1) >= beforeDate.UtcDateTime).ToList();
        return rows.Count == current.Members.Count ? null : new MembershipTableData(rows, new TableVersion(checked(current.Version.Version + 1), Guid.NewGuid().ToString("N")));
    }

    private static DateTime LastUpdate(MembershipEntry entry)
    {
        var latest = entry.StartTime > entry.IAmAliveTime ? entry.StartTime : entry.IAmAliveTime;
        foreach (var vote in entry.SuspectTimes ?? [])
        {
            if (vote.Item2 > latest)
            {
                latest = vote.Item2;
            }
        }

        return DateTime.SpecifyKind(latest, DateTimeKind.Utc);
    }
}
