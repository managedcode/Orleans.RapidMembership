using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal static class MembershipSnapshotEquality
{
    // Logical equality is independent of serializer reference sharing.
    public static bool AreEqual(MembershipSnapshot left, MembershipSnapshot right)
        => left.Index == right.Index && left.OperationId == right.OperationId && left.Applied == right.Applied
            && left.OutcomeFloor == right.OutcomeFloor && left.Outcomes.SequenceEqual(right.Outcomes)
            && left.Table.Version.Equals(right.Table.Version) && RowsEqual(left.Table, right.Table);

    private static bool RowsEqual(MembershipTableData left, MembershipTableData right)
    {
        if (left.Members.Count != right.Members.Count)
        {
            return false;
        }

        foreach (var row in left.Members)
        {
            var other = right.TryGet(row.Item1.SiloAddress);
            if (other is null || row.Item2 != other.Item2 || !EntriesEqual(row.Item1, other.Item1))
            {
                return false;
            }
        }

        return true;
    }

    private static bool EntriesEqual(MembershipEntry left, MembershipEntry right)
        => left.SiloAddress.Equals(right.SiloAddress) && left.Status == right.Status
            && left.ProxyPort == right.ProxyPort && left.HostName == right.HostName && left.SiloName == right.SiloName
            && left.RoleName == right.RoleName && left.UpdateZone == right.UpdateZone && left.FaultZone == right.FaultZone
            && left.StartTime == right.StartTime && left.IAmAliveTime == right.IAmAliveTime
            && (left.SuspectTimes is null && right.SuspectTimes is null
                || left.SuspectTimes is not null && right.SuspectTimes is not null && left.SuspectTimes.SequenceEqual(right.SuspectTimes));
}
