namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class TransferRecoveryTests
{
    [Fact]
    public async Task AnInstalledReplicaRetainsTheCertificateAfterTheTransferringCallerLosesIt()
    {
        using var replicas = new MemoryReplicaSet();
        var initial = ConfigurationHandoffTests.Configuration(0, 1);
        var previous = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var next = ConfigurationHandoffTests.Configuration(2, 1, 4, 5);
        foreach (var number in Enumerable.Range(1, 5))
        {
            _ = replicas.Add(ConfigurationHandoffTests.Member(number));
        }

        await replicas.BootstrapAsync(initial);
        await replicas.InstallAsync(initial, previous, await replicas.FreezeAsync(initial, previous));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in previous.Members)
        {
            Assert.True((await replicas.SendAsync(member, MemoryReplicaHandoffTests.Proposal(previous, PeerAction.Commit, snapshot, 1))).Granted);
        }

        var request = new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = previous,
            Configuration = next,
            FrozenReplicas = await replicas.FreezeAsync(previous, next),
        };
        Assert.True((await replicas.SendAsync(next.Members[0], request)).Granted);
        request.FrozenReplicas = [];
        var metadata = await replicas.SendAsync(next.Members[0], new PeerRequest { Action = PeerAction.Locate });
        Assert.Empty(metadata.TransferEvidence);
        var retained = await replicas.SendAsync(next.Members[0], new PeerRequest { Action = PeerAction.FetchTransfer });
        Assert.True(retained.Granted);
        Assert.Equal(3, retained.TransferEvidence.Length);
        request.FrozenReplicas = retained.TransferEvidence;
        foreach (var member in next.Members.Skip(1))
        {
            Assert.True((await replicas.SendAsync(member, request)).Granted);
            var actual = await replicas.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = next });
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, actual.State.Committed));
        }
    }
}
