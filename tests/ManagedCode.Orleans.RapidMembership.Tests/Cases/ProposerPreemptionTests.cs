using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class ProposerPreemptionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARealHigherPromisePreemptsPrepareOrAcceptAndTheWriterRetriesWithoutLosingRows(bool duringAccept)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        var token = timeout.Token;
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var gate = new ProposerStageGate(duringAccept ? PeerAction.Accept : PeerAction.Inspect, services.GetRequiredService<Serializer<PeerRequest>>(), services.GetRequiredService<Serializer<PeerReply>>());
        await using var network = await ReplicaNetwork.StartAsync(token, collection =>
        {
            _ = collection.AddSingleton(gate);
            _ = collection.AddGrpc(options => options.Interceptors.Add<ProposerStageGate>());
        });
        network.Replicas.Options.PeerTimeout = TimeSpan.FromSeconds(10);
        var initial = new CoordinationConfiguration(42, 0, [network.Members[0]]);
        var installed = new CoordinationConfiguration(42, 1, [.. network.Members]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, installed, await network.Replicas.FreezeAsync(initial, installed));
        await using var directory = await network.AnnounceAsync(token);
        using var proposer = new MembershipProposer(network.Transport, network.Replicas.Options,
            services.GetRequiredService<Serializer<MembershipSnapshot>>(), directory);
        var before = await proposer.ExecuteAsync(null, token);
        gate.Arm();
        var writing = proposer.ExecuteAsync(table => MembershipMutations.Write(table, MutationTests.Entry(1), null, table.Version.Next()), token);
        try
        {
            await gate.Entered.Task.WaitAsync(token);
            var promises = await Task.WhenAll(network.Members.Select(member => network.Transport.SendAsync(member, new PeerRequest
            {
                Action = PeerAction.Prepare,
                Configuration = installed,
                Index = before.Index + 1,
                Ballot = new Ballot(10000, "preempting-peer"),
                Snapshot = before,
            }, token)));
            Assert.All(promises, reply => Assert.True(reply is { Granted: true }));
            gate.Release();
            Assert.True((await writing).Applied);
            Assert.True(gate.Rejections >= installed.Quorum, FormattableString.Invariant($"Expected a real rejected quorum; observed {gate.Rejections} replies."));
            var next = await proposer.ExecuteAsync(table => MembershipMutations.Write(table, MutationTests.Entry(2), null, table.Version.Next()), token);
            Assert.True(next.Applied);
            var observed = await proposer.ExecuteAsync(null, token);
            Assert.Equal(2, observed.Table.Members.Count);
            Assert.Equal(next.Index, observed.Index);
        }
        finally
        {
            gate.Release();
            _ = await writing;
        }
    }
}
