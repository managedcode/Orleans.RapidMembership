using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Messaging;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership;

/// <summary>Registers self-hosted RAPID membership for Orleans silos and clients.</summary>
public static class RapidMembershipExtensions
{
    /// <summary>Uses self-hosted durable quorum membership instead of an external membership table.</summary>
    public static ISiloBuilder UseRapidMembership(this ISiloBuilder builder, Action<RapidMembershipOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        Register(builder.Services, configure, server: true);
        _ = builder.Services.AddSingleton(services => new RapidMembershipTable(
            services.GetRequiredService<IOptions<RapidMembershipOptions>>(), services.GetRequiredService<IOptions<ClusterOptions>>(),
            services.GetRequiredService<MembershipProposer>(), services.GetRequiredService<Serializer<MembershipEntry>>()));
        _ = builder.Services.Replace(ServiceDescriptor.Singleton<IMembershipTable>(services => services.GetRequiredService<RapidMembershipTable>()));
        return builder;
    }

    /// <summary>Discovers active Orleans gateways through the same membership quorum.</summary>
    public static IClientBuilder UseRapidMembership(this IClientBuilder builder, Action<RapidMembershipOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        Register(builder.Services, configure, server: false);
        _ = builder.Services.Replace(ServiceDescriptor.Singleton<IGatewayListProvider, RapidGatewayListProvider>());
        return builder;
    }

    private static void Register(IServiceCollection services, Action<RapidMembershipOptions> configure, bool server)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _ = services.AddOptions<RapidMembershipOptions>().Configure(configure);
        _ = services.AddSerializer(serializer => serializer.AddAssembly(typeof(RapidMembershipOptions).Assembly));
        services.TryAddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<RapidMembershipOptions>>().Value;
            options.Validate(server);
            var cluster = provider.GetRequiredService<IOptions<ClusterOptions>>().Value;
            return new PeerTransport(options, options.Identity(cluster), provider.GetRequiredService<Serializer<PeerRequest>>(),
                provider.GetRequiredService<Serializer<PeerReply>>());
        });
        services.TryAddSingleton(provider => new MembershipProposer(provider.GetRequiredService<PeerTransport>(),
            provider.GetRequiredService<IOptions<RapidMembershipOptions>>().Value, provider.GetRequiredService<Serializer<MembershipSnapshot>>()));
    }
}
