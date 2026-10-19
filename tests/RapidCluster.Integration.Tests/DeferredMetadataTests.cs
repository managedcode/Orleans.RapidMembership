using System.Net;
using System.Reflection;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using RapidCluster.Grpc;
using RapidCluster.Pb;

namespace RapidCluster.Integration.Tests;

public sealed class DeferredMetadataTests(ITestOutputHelper output)
{
    [Fact]
    public async Task HeldConsensusPreservesTheFollowingJoinersMetadataAcrossTheFirstDecision()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var gate = new ConsensusGate();
        await using var cluster = new TestCluster(output, Configure);
        var addresses = Enumerable.Range(0, 6).Select(_ => new IPEndPoint(IPAddress.Loopback, cluster.GetNextPort())).ToArray();
        var identities = addresses.ToDictionary(address => address.Port, _ => Guid.NewGuid());
        List<(WebApplication App, IRapidCluster Cluster)> nodes =
        [
            await cluster.CreateSeedNodeAsync(addresses[0], options => SetMetadata(options, 0), token),
        ];
        for (var index = 1; index < 3; index++)
        {
            nodes.Add(await cluster.CreateJoinerNodeAsync(addresses[index], addresses[0], options => SetMetadata(options, index), token));
        }

        var joining = new List<Task<(WebApplication App, IRapidCluster Cluster)>>();
        try
        {
            gate.Arm();
            joining.Add(cluster.CreateJoinerNodeAsync(addresses[3], addresses[0], options => SetMetadata(options, 3), token));
            await gate.Entered.Task.WaitAsync(token);
            while (nodes.Any(node => !ReadState(node.App).Announced))
            {
                await Task.Delay(20, token);
            }

            joining.Add(cluster.CreateJoinerNodeAsync(addresses[4], addresses[0], options => SetMetadata(options, 4), token));
            while (nodes.Any(node => !ReadState(node.App).Deferred))
            {
                await Task.Delay(20, token);
            }

            gate.Release();
            nodes.AddRange(await Task.WhenAll(joining));
            await RequireMetadataAsync(5);
            nodes.Add(await cluster.CreateJoinerNodeAsync(addresses[5], addresses[0], options => SetMetadata(options, 5), token));
            await RequireMetadataAsync(6);
        }
        finally
        {
            gate.Release();
            _ = await Task.WhenAll(joining);
        }

        void SetMetadata(RapidClusterOptions options, int index) => options.Metadata["incarnation"] = identities[addresses[index].Port].ToByteArray();
        void Configure(IServiceCollection services)
        {
            _ = services.AddSingleton(gate);
            _ = services.AddGrpc(options => options.Interceptors.Add<ConsensusGate>());
            _ = services.Configure<RapidClusterGrpcOptions>(options => options.Timeout = TimeSpan.FromSeconds(15));
        }

        async Task RequireMetadataAsync(int count)
        {
            while (nodes.Any(node => node.Cluster.CurrentView.Members.Length != count))
            {
                await Task.Delay(20, token);
            }

            foreach (var (App, Cluster) in nodes)
            {
                foreach (var member in Cluster.CurrentView.Members)
                {
                    Assert.Equal(identities[((IPEndPoint)member.EndPoint).Port], new Guid(member.Metadata["incarnation"].Span));
                }
            }
        }
    }

    private static (bool Announced, bool Deferred) ReadState(WebApplication app)
    {
        var type = typeof(IRapidCluster).Assembly.GetType("RapidCluster.MembershipService", throwOnError: true)!;
        var service = app.Services.GetRequiredService(type);
        const BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
        var synchronization = (Lock)type.GetField("_membershipUpdateLock", flags)!.GetValue(service)!;
        lock (synchronization)
        {
            return ((bool)type.GetField("_announcedProposal", flags)!.GetValue(service)!,
                ((SortedSet<Endpoint>)type.GetField("_deferredProposals", flags)!.GetValue(service)!).Count > 0);
        }
    }

    private sealed class ConsensusGate : Interceptor
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Release()
        {
            Volatile.Write(ref _armed, 0);
            _ = _release.TrySetResult();
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            if (Volatile.Read(ref _armed) != 0 && request is RapidClusterRequest { Phase2BMessage: not null })
            {
                _ = Entered.TrySetResult();
                await _release.Task.WaitAsync(context.CancellationToken);
            }

            return await continuation(request, context);
        }
    }
}
