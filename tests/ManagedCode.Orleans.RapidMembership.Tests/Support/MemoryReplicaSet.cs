using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class MemoryReplicaSet : IDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection()
        .AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();

    private readonly Dictionary<Guid, MemoryMembershipReplica> _replicas = [];

    public RapidMembershipOptions Options { get; } = new() { MaximumSnapshotBytes = 4096, MaximumEntries = 2, MaximumOperationHistory = 8 };

    public MemoryMembershipReplica Add(CoordinationMember member)
    {
        var replica = new MemoryMembershipReplica(member.Incarnation, "cluster", Options,
            _services.GetRequiredService<Serializer<PeerRequest>>(), _services.GetRequiredService<Serializer<PeerReply>>(),
            _services.GetRequiredService<Serializer<MembershipSnapshot>>());
        _replicas.Add(member.Incarnation, replica);
        return replica;
    }

    public Task<PeerReply> SendAsync(CoordinationMember member, PeerRequest request)
    {
        request.Recipient = member.Incarnation;
        return _replicas[member.Incarnation].ExchangeAsync(request, TestContext.Current.CancellationToken);
    }

    public Task ObserveAsync(CoordinationConfiguration configuration) => Task.WhenAll(
        _replicas.Values.Select(replica => replica.ObserveAsync(configuration, TestContext.Current.CancellationToken)));

    public async Task BootstrapAsync(CoordinationConfiguration initial)
    {
        await ObserveAsync(initial);
        Assert.True(await _replicas[initial.Members[0].Incarnation].BootstrapAsync(TestContext.Current.CancellationToken));
    }

    public async Task<ImmutableArray<FrozenReplica>> FreezeAsync(CoordinationConfiguration previous, CoordinationConfiguration next)
    {
        await ObserveAsync(next);
        var evidence = ImmutableArray.CreateBuilder<FrozenReplica>();
        foreach (var member in previous.Members)
        {
            var reply = await SendAsync(member, new PeerRequest
            {
                Action = PeerAction.FreezeConfiguration,
                PreviousConfiguration = previous,
                Configuration = next,
            });
            Assert.True(reply.Granted);
            Assert.Equal(member.Incarnation, reply.Incarnation);
            Assert.True(previous.Matches(reply.Configuration));
            Assert.True(next.Matches(reply.FrozenFor));
            evidence.Add(new FrozenReplica(reply.Incarnation, reply.Configuration!, reply.FrozenFor!, reply.State));
        }

        return evidence.ToImmutable();
    }

    public async Task InstallAsync(CoordinationConfiguration previous, CoordinationConfiguration next, ImmutableArray<FrozenReplica> evidence)
    {
        var acknowledged = new List<Guid>();
        foreach (var member in next.Members)
        {
            var reply = await SendAsync(member, new PeerRequest
            {
                Action = PeerAction.InstallConfiguration,
                PreviousConfiguration = previous,
                Configuration = next,
                FrozenReplicas = evidence,
            });
            Assert.True(reply.Granted);
            acknowledged.Add(reply.Incarnation);
        }

        Assert.True(ConfigurationHandoff.HasInstalledQuorum(next, acknowledged));
    }

    public void Dispose()
    {
        foreach (var replica in _replicas.Values)
        {
            replica.Dispose();
        }

        _services.Dispose();
    }
}
