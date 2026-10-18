using System.Net;
using Orleans;
using Orleans.Runtime;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class MutationTests
{
    [Fact]
    public void InsertAndUpdateRequireBothNativeVersionsAndNeverMutateOnConflict()
    {
        var initial = new MembershipTableData(new TableVersion(0, "start"));
        var entry = Entry(1);
        var inserted = MembershipMutations.Write(initial, entry, null, initial.Version.Next());
        Assert.NotNull(inserted);
        Assert.Empty(initial.Members);
        Assert.Null(MembershipMutations.Write(inserted, entry, null, inserted.Version.Next()));
        Assert.Null(MembershipMutations.Write(inserted, entry, "wrong", inserted.Version.Next()));
        Assert.Null(MembershipMutations.Write(inserted, Entry(2), null, initial.Version.Next()));
        Assert.Null(MembershipMutations.Write(inserted, Entry(2), null, new TableVersion(99, inserted.Version.VersionEtag)));
        var active = Entry(1);
        active.Status = SiloStatus.Active;
        var updated = MembershipMutations.Write(inserted, active, inserted.Members[0].Item2, inserted.Version.Next());
        Assert.NotNull(updated);
        Assert.Equal(2, updated.Version.Version);
        Assert.Equal(SiloStatus.Active, updated.Members[0].Item1.Status);
        Assert.NotEqual(inserted.Members[0].Item2, updated.Members[0].Item2);
        Assert.NotEqual(inserted.Version.VersionEtag, updated.Version.VersionEtag);
    }

    [Fact]
    public void HeartbeatPreservesVersionsAndCleanupRetainsRecentDeathVotes()
    {
        var dead = Entry(1);
        dead.Status = SiloStatus.Dead;
        dead.SuspectTimes = [Tuple.Create(Entry(2).SiloAddress, DateTime.UtcNow)];
        var table = new MembershipTableData([Tuple.Create(dead, "row")], new TableVersion(1, "table"));
        var pulse = Entry(1);
        pulse.IAmAliveTime = dead.IAmAliveTime.AddSeconds(5);
        var updated = MembershipMutations.Heartbeat(table, pulse);
        Assert.NotNull(updated);
        Assert.Equal(table.Version, updated.Version);
        Assert.Equal("row", updated.Members[0].Item2);
        Assert.Equal(pulse.IAmAliveTime, updated.Members[0].Item1.IAmAliveTime);
        Assert.Null(MembershipMutations.Cleanup(updated, DateTimeOffset.UtcNow.AddMinutes(-1)));
        var cleaned = MembershipMutations.Cleanup(updated, DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.NotNull(cleaned);
        Assert.Empty(cleaned.Members);
        Assert.Equal(2, cleaned.Version.Version);
    }

    internal static MembershipEntry Entry(int generation) => new()
    {
        SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), generation),
        SiloName = "node",
        HostName = "host",
        Status = SiloStatus.Joining,
        StartTime = DateTime.UtcNow.AddHours(-1),
        IAmAliveTime = DateTime.UtcNow.AddMinutes(-5),
    };
}
