using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class BootstrapLossTests
{
    [Fact]
    public async Task LosingTheFoundingCandidateCannotCreateAnIndependentEpochAfterDiscoveryExpiry()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var identity = Guid.NewGuid().ToString("N");
        var options = new RapidMembershipOptions
        {
            SharedSecret = "0123456789abcdef0123456789abcdef",
            BootstrapDelay = TimeSpan.FromSeconds(2),
        };
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly))
            .BuildServiceProvider();
        using var transport = new PeerTransport(options, identity, services.GetRequiredService<Serializer<PeerRequest>>(),
            services.GetRequiredService<Serializer<PeerReply>>());
        await using var first = new PeerDirectory(options, identity, transport);
        await using var second = new PeerDirectory(options, identity, transport);
        await Task.WhenAll(first.StartServerAsync(token), second.StartServerAsync(token));
        var ordered = new[] { first, second }.OrderBy(directory => directory.Host!.Incarnation).ToArray();
        var founding = ordered[0].Host!.Incarnation.ToString("N");
        while (ordered.Any(directory => directory.Host!.Participation.BootstrapCandidate != founding))
        {
            await Task.Delay(10, token);
        }

        Assert.False(ordered[0].Host!.Knowledge.HasObservedCluster);
        await ordered[0].DisposeAsync();
        await Task.Delay(TimeSpan.FromSeconds(36), token);
        Assert.Empty(ordered[1].Host!.Discovery.Peers);
        Assert.False(ordered[1].Host!.Knowledge.HasObservedCluster);
        Assert.True(ordered[1].Host!.Cluster.CurrentView.Members.IsDefaultOrEmpty);
        await ordered[1].DisposeAsync();
        await using var restarted = new PeerDirectory(options, identity, transport);
        await restarted.StartServerAsync(token);
        var configuration = await restarted.ResolveAsync(token);
        Assert.Equal(restarted.Host!.Incarnation, Assert.Single(configuration.Members).Incarnation);
    }
}
