using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership;

/// <summary>Configures automatic in-memory membership and bounded peer discovery.</summary>
public sealed class RapidMembershipOptions
{
    /// <summary>Gets or sets the HTTP/2 membership port. Zero requests an available port.</summary>
    public int ListenPort { get; set; }

    /// <summary>Gets or sets the shared UDP discovery port.</summary>
    public int DiscoveryPort { get; set; } = 11113;

    /// <summary>Gets or sets additional IPv4 CIDR subnets or inclusive start-end address ranges to probe.</summary>
    public IReadOnlyList<string> DiscoveryNetworks { get; set; } = [];

    /// <summary>Gets or sets the maximum additional addresses retained for directed discovery.</summary>
    public int MaximumDiscoveryAddresses { get; set; } = 4096;

    /// <summary>Gets or sets the maximum retained peer incarnations and channels.</summary>
    public int MaximumPeers { get; set; } = 10000;

    /// <summary>Gets or sets the initial discovery observation interval before cold bootstrap.</summary>
    public TimeSpan BootstrapDelay { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>Gets or sets the deadline for initial discovery and coordination formation.</summary>
    public TimeSpan StartupTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Gets or sets the shared admission secret, with at least 32 UTF-8 bytes.</summary>
    public string SharedSecret { get; set; } = string.Empty;

    /// <summary>Gets or sets the deadline for a complete table operation.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Gets or sets the deadline for an individual peer exchange.</summary>
    public TimeSpan PeerTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Gets or sets the maximum serialized table snapshot size.</summary>
    public int MaximumSnapshotBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>Gets or sets the maximum RPC size, including frozen-quorum state during handoff.</summary>
    public int MaximumExchangeBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>Gets or sets the maximum retained silo rows.</summary>
    public int MaximumEntries { get; set; } = 10000;

    /// <summary>Gets or sets the retained outcomes used to settle internal retries.</summary>
    public int MaximumOperationHistory { get; set; } = 4096;

    /// <summary>Gets or sets the maximum concurrently admitted peer exchanges.</summary>
    public int MaximumAdmissions { get; set; } = 32;

    internal string AdmissionToken => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(SharedSecret)));

    internal static string Identity(ClusterOptions cluster) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
        $"{cluster.ServiceId.Length}:{cluster.ServiceId}{cluster.ClusterId.Length}:{cluster.ClusterId}")));

    internal void Validate(bool server)
    {
        var valid = (!server || ListenPort is >= 0 and <= 65535) && DiscoveryPort is > 0 and <= 65535
            && MaximumDiscoveryAddresses is > 0 and <= 65536 && MaximumPeers is > 0 and <= 10000
            && BootstrapDelay > TimeSpan.Zero && BootstrapDelay <= TimeSpan.FromMinutes(1)
            && StartupTimeout > TimeSpan.Zero && StartupTimeout <= TimeSpan.FromMinutes(15)
            && Encoding.UTF8.GetByteCount(SharedSecret) >= 32
            && OperationTimeout > TimeSpan.Zero && OperationTimeout <= TimeSpan.FromMinutes(5)
            && PeerTimeout > TimeSpan.Zero && PeerTimeout <= OperationTimeout
            && MaximumSnapshotBytes is >= 4096 and <= 67108864
            && MaximumExchangeBytes >= (long)MaximumSnapshotBytes * 8 && MaximumExchangeBytes <= 536870912
            && MaximumEntries > 0 && MaximumAdmissions is > 0 and <= 1024
            && MaximumOperationHistory is > 0 and <= 65536;
        if (!valid)
        {
            throw new OptionsValidationException(nameof(RapidMembershipOptions), typeof(RapidMembershipOptions),
                ["Provide valid ports, a 32-byte secret and positive, bounded discovery, memory and timeout settings."]);
        }

        _ = Discovery.DiscoveryAddressRange.Expand(DiscoveryNetworks, MaximumDiscoveryAddresses);
    }
}
