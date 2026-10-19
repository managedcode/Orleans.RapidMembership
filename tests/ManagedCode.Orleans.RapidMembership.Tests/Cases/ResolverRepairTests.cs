namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ResolverRepairTests
{
    [Fact]
    public async Task AReadyMajorityRepairsTheResponsiveReplicaThatMissedItsInstallationOverHttp2()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        var token = timeout.Token;
        await using var network = await ReplicaNetwork.StartAsync(token);
        var members = network.Members;
        var initial = new CoordinationConfiguration(7, 0, [members[0]]);
        var next = new CoordinationConfiguration(7, 1, [.. members]);
        await network.Replicas.BootstrapAsync(initial);
        var certificate = await network.Replicas.FreezeAsync(initial, next);
        foreach (var member in members.Take(2))
        {
            Assert.True((await network.Replicas.SendAsync(member, new PeerRequest
            {
                Action = PeerAction.InstallConfiguration,
                PreviousConfiguration = initial,
                Configuration = next,
                FrozenReplicas = certificate,
            })).Granted);
        }

        var descriptions = await network.Transport.LocateAsync(members, token);
        Assert.Null(descriptions.Single(reply => reply.Incarnation == members[2].Incarnation).Configuration);
        var resolved = await new ConfigurationResolver(network.Transport).ResolveAsync(descriptions, token);
        Assert.True(next.Matches(resolved));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var member in members)
        {
            var read = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = next }, token);
            Assert.NotNull(read);
            Assert.True(read.Granted);
            Assert.True(next.Matches(read.Configuration));
            Assert.True((await network.Transport.SendAsync(member,
                MemoryReplicaHandoffTests.Proposal(next, PeerAction.Prepare, snapshot, 3), token))!.Granted);
            Assert.True((await network.Transport.SendAsync(member,
                MemoryReplicaHandoffTests.Proposal(next, PeerAction.Accept, snapshot, 3), token))!.Granted);
            Assert.True((await network.Transport.SendAsync(member,
                MemoryReplicaHandoffTests.Proposal(next, PeerAction.Commit, snapshot, 3), token))!.Granted);
        }
    }
}
