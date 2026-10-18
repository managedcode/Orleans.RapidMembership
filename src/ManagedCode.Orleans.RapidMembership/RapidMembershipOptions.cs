using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership;

/// <summary>Configures self-hosted membership voters, discovery and resource bounds.</summary>
public sealed class RapidMembershipOptions
{
    /// <summary>Gets or sets the stable, identical, ordered voter endpoints configured on every silo and client.</summary>
    public IReadOnlyList<string> SeedEndpoints { get; set; } = [];

    /// <summary>Gets or sets this silo's literal-IP identity endpoint. Only configured voters bind an HTTP/2 listener.</summary>
    public string ListenEndpoint { get; set; } = "http://127.0.0.1:11112";

    /// <summary>Gets or sets the exclusively owned durable directory for a voter. Preserve it across restarts.</summary>
    public string StorageDirectory { get; set; } = string.Empty;

    /// <summary>Gets or sets the shared admission secret. Use at least 32 UTF-8 bytes.</summary>
    public string SharedSecret { get; set; } = string.Empty;

    /// <summary>Gets or sets the deadline for a whole membership operation.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Gets or sets the deadline of an individual peer exchange.</summary>
    public TimeSpan PeerTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Gets or sets the maximum serialized snapshot size.</summary>
    public int MaximumSnapshotBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>Gets or sets the maximum retained silo rows.</summary>
    public int MaximumEntries { get; set; } = 10000;

    /// <summary>Gets or sets the retained mutation outcomes used to settle internal retries.</summary>
    public int MaximumOperationHistory { get; set; } = 4096;

    /// <summary>Gets or sets the maximum concurrently admitted peer exchanges.</summary>
    public int MaximumAdmissions { get; set; } = 32;

    internal string AdmissionToken => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(SharedSecret)));

    internal string Identity(ClusterOptions cluster) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
        $"{cluster.ServiceId.Length}:{cluster.ServiceId}{cluster.ClusterId.Length}:{cluster.ClusterId}|{string.Join('|', SeedEndpoints)}")));

    internal void Validate(bool server)
    {
        var valid = SeedEndpoints.Count > 0 && SeedEndpoints.Distinct(StringComparer.Ordinal).Count() == SeedEndpoints.Count
            && SeedEndpoints.All(IsEndpoint) && (!server || IsEndpoint(ListenEndpoint))
            && Encoding.UTF8.GetByteCount(SharedSecret) >= 32
            && OperationTimeout > TimeSpan.Zero && OperationTimeout <= TimeSpan.FromMinutes(5)
            && PeerTimeout > TimeSpan.Zero && PeerTimeout <= OperationTimeout
            && MaximumSnapshotBytes is >= 4096 and <= 67108864 && MaximumEntries > 0
            && MaximumAdmissions is > 0 and <= 1024 && MaximumOperationHistory is > 0 and <= 65536;
        if (!valid || server && SeedEndpoints.Contains(ListenEndpoint, StringComparer.Ordinal) && string.IsNullOrWhiteSpace(StorageDirectory))
        {
            throw new OptionsValidationException(nameof(RapidMembershipOptions), typeof(RapidMembershipOptions),
                ["Provide distinct literal-IP HTTP endpoints, a 32-byte secret, positive bounds and a durable voter directory."]);
        }
    }

    private static bool IsEndpoint(string endpoint) => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
        && uri.Scheme == Uri.UriSchemeHttp && IPAddress.TryParse(uri.Host.Trim('[', ']'), out var ip)
        && !IPAddress.Any.Equals(ip) && !IPAddress.IPv6Any.Equals(ip)
        && uri.Port is > 0 and <= 65535 && uri.AbsolutePath == "/" && uri.Query.Length == 0
        && uri.Fragment.Length == 0 && uri.UserInfo.Length == 0 && endpoint == uri.GetLeftPart(UriPartial.Authority);
}
