using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class CoordinationConfigurationTests
{
    [Fact]
    public void NativeSerializationPreservesConfigurationAndFrozenEvidence()
    {
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var configuration = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var serializer = services.GetRequiredService<Serializer<CoordinationConfiguration>>();
        var copy = serializer.Deserialize(serializer.SerializeToArray(configuration))!;
        copy.Validate();
        Assert.True(configuration.Matches(copy));
        Assert.False(configuration.Matches(null));
        Assert.False(configuration.Matches(copy with { ClusterId = 100 }));
        Assert.False(configuration.Matches(copy with { Version = 2 }));
        Assert.False(configuration.Matches(ConfigurationHandoffTests.Configuration(1, 1, 2, 4)));
        Assert.Equal(2, copy.Quorum);
        var evidence = new FrozenReplica(configuration.Members[0].Incarnation, configuration,
            ConfigurationHandoffTests.Configuration(2, 2, 3, 4), new AcceptorState { Identity = "cluster" });
        var frozen = services.GetRequiredService<Serializer<FrozenReplica>>();
        var roundTrip = frozen.Deserialize(frozen.SerializeToArray(evidence))!;
        Assert.Equal(evidence.Incarnation, roundTrip.Incarnation);
        Assert.True(roundTrip.Previous.Matches(evidence.Previous));
        Assert.True(roundTrip.Next.Matches(evidence.Next));
        Assert.Equal("cluster", roundTrip.State.Identity);
    }

    [Theory]
    [InlineData("default")]
    [InlineData("empty")]
    [InlineData("negative")]
    [InlineData("many")]
    [InlineData("duplicate-id")]
    [InlineData("duplicate-endpoint")]
    [InlineData("unordered")]
    [InlineData("empty-id")]
    [InlineData("wildcard")]
    [InlineData("ipv6-wildcard")]
    [InlineData("hostname")]
    [InlineData("path")]
    [InlineData("query")]
    [InlineData("scheme")]
    [InlineData("zero-port")]
    public void UntrustedConfigurationsHaveBoundedCanonicalMembers(string change)
    {
        var configuration = ConfigurationHandoffTests.Configuration(1, 1, 2, 3);
        var members = configuration.Members;
        configuration = change switch
        {
            "default" => configuration with { Members = (new ImmutableArray<CoordinationMember>[1])[0] },
            "empty" => configuration with { Members = [] },
            "negative" => configuration with { Version = -1 },
            "many" => configuration with { Members = [.. Enumerable.Repeat(members[0], 10001)] },
            "duplicate-id" => configuration with { Members = [members[0], members[0]] },
            "duplicate-endpoint" => configuration with { Members = [members[0], members[1] with { Endpoint = members[0].Endpoint }] },
            "unordered" => configuration with { Members = [members[1], members[0]] },
            "empty-id" => configuration with { Members = [members[0] with { Incarnation = Guid.Empty }] },
            _ => configuration with { Members = [members[0] with { Endpoint = Endpoint(change) }] },
        };
        _ = Assert.Throws<InvalidDataException>(configuration.Validate);
        var healthy = ConfigurationHandoffTests.Configuration(2, 1);
        healthy.Validate();
        Assert.Equal(1, healthy.Quorum);
    }

    [Fact]
    public void HandoffRejectsMissingEvidenceAndUnrelatedOrOlderViews()
    {
        using var services = new ServiceCollection().AddSerializer(builder => builder.AddAssembly(typeof(RapidMembershipOptions).Assembly)).BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<MembershipSnapshot>>();
        var previous = ConfigurationHandoffTests.Configuration(1, 1);
        var next = ConfigurationHandoffTests.Configuration(2, 1, 2);
        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next, (new ImmutableArray<FrozenReplica>[1])[0], "cluster", serializer));
        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next, [], "cluster", serializer));
        var evidence = new FrozenReplica(previous.Members[0].Incarnation, previous, next, new AcceptorState { Identity = "cluster" });
        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, previous, [evidence], "cluster", serializer));
        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next with { ClusterId = 9 }, [evidence], "cluster", serializer));
        _ = Assert.Throws<InvalidDataException>(() => ConfigurationHandoff.Recover(previous, next, [evidence, evidence], "cluster", serializer));
        Assert.Equal(0, ConfigurationHandoff.Recover(previous, next, [evidence], "cluster", serializer).Index);
        var replica = new ReplicaConfigurationFence(previous.Members[0].Incarnation);
        Assert.False(replica.Bootstrap());
        Assert.False(replica.Freeze(previous, next));
        Assert.False(replica.CanInstall(previous, next));
    }

    private static string Endpoint(string change) => change switch
    {
        "wildcard" => "http://0.0.0.0:11112",
        "ipv6-wildcard" => "http://[::]:11112",
        "hostname" => "http://localhost:11112",
        "path" => "http://127.0.0.1:11112/state",
        "query" => "http://127.0.0.1:11112?state=1",
        "scheme" => "https://127.0.0.1:11112",
        "zero-port" => "http://127.0.0.1:0",
        _ => throw new ArgumentOutOfRangeException(nameof(change)),
    };
}
