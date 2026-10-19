using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
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
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var options = new RapidMembershipOptions
        {
            SharedSecret = "0123456789abcdef0123456789abcdef",
            BootstrapDelay = TimeSpan.FromSeconds(2),
            PeerTimeout = TimeSpan.FromMinutes(1),
            TimeProvider = clock,
        };
        await using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly))
            .BuildServiceProvider();
        using var transport = new PeerTransport(options, identity, services.GetRequiredService<Serializer<PeerRequest>>(),
            services.GetRequiredService<Serializer<PeerReply>>());
        await using var first = new PeerDirectory(options, identity, transport);
        await using var second = new PeerDirectory(options, identity, transport);
        await Task.WhenAll(first.StartServerAsync(token), second.StartServerAsync(token));
        while (first.Host!.Discovery.Peers.Count != 1 || second.Host!.Discovery.Peers.Count != 1)
        {
            await Task.Delay(10, token);
        }

        var ordered = new[] { first, second }.OrderBy(directory => directory.Host!.Incarnation).ToArray();
        var founding = ordered[0].Host!.Incarnation.ToString("N");
        clock.Advance(options.BootstrapDelay);
        while (ordered.Any(directory => directory.Host!.Participation.BootstrapCandidate != founding))
        {
            await Task.Delay(10, token);
        }

        Assert.False(ordered[0].Host!.Knowledge.HasObservedCluster);
        await ordered[0].DisposeAsync();
        clock.Advance(TimeSpan.FromSeconds(41));
        Assert.Empty(ordered[1].Host!.Discovery.Peers);
        await RequireClosedAsync(ordered[1], clock, token);
        Assert.False(ordered[1].Host!.Knowledge.HasObservedCluster);
        Assert.True(ordered[1].Host!.Cluster.CurrentView.Members.IsDefaultOrEmpty);
        await ordered[1].DisposeAsync();
        await using var restarted = new PeerDirectory(options, identity, transport);
        await restarted.StartServerAsync(token);
        try
        {
            var formation = restarted.ResolveAsync(token);
            await DriveAsync(formation, clock, token);
            Assert.Equal(restarted.Host!.Incarnation, Assert.Single((await formation).Members).Incarnation);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await DriveAsync(restarted.DisposeAsync().AsTask(), clock, cleanup.Token);
        }
    }

    private static async Task RequireClosedAsync(PeerDirectory directory, FakeTimeProvider clock, CancellationToken token)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(token);
        var resolution = directory.ResolveAsync(stop.Token);
        try
        {
            for (var seconds = 0; seconds < 40; seconds++)
            {
                clock.Advance(TimeSpan.FromSeconds(1));
                await Task.Delay(10, token);
            }

            Assert.False(resolution.IsCompleted);
        }
        finally
        {
            await stop.CancelAsync();
            try
            {
                _ = await resolution;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
        }
    }

    private static async Task DriveAsync(Task operation, FakeTimeProvider clock, CancellationToken token)
    {
        while (!operation.IsCompleted)
        {
            clock.Advance(TimeSpan.FromMilliseconds(250));
            await Task.Delay(10, token);
        }

        await operation;
    }
}
