namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ReplicaNetworkTests
{
    [Fact]
    public async Task ParallelRealHttp2NetworksKeepDistinctBoundPortsAndServeEveryReplica()
    {
        var token = TestContext.Current.CancellationToken;
        var starting = Enumerable.Range(0, 8).Select(_ => ReplicaNetwork.StartAsync(token)).ToArray();
        try
        {
            var networks = await Task.WhenAll(starting);
            var members = networks.SelectMany(network => network.Members).ToArray();
            Assert.Equal(24, members.Length);
            Assert.Equal(members.Length, members.Select(member => new Uri(member.Endpoint).Port).Distinct().Count());
            Assert.All(members, member => Assert.NotEqual(0, new Uri(member.Endpoint).Port));
            foreach (var network in networks)
            {
                foreach (var member in network.Members)
                {
                    var reply = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Locate }, token);
                    Assert.NotNull(reply);
                    Assert.True(reply.Granted);
                    Assert.Equal(member.Incarnation, reply.Incarnation);
                }
            }
        }
        finally
        {
            await Task.WhenAll(starting.Where(task => task.IsCompletedSuccessfully)
                .Select(async task => await (await task).DisposeAsync()));
        }
    }
}
