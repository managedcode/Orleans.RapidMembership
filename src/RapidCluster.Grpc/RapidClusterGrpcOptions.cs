namespace RapidCluster.Grpc;

/// <summary>
/// Configuration options for RapidCluster gRPC transport.
/// </summary>
public sealed class RapidClusterGrpcOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether to use HTTPS for gRPC communication. Default: false (use HTTP).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When enabled, gRPC clients will connect using https:// scheme instead of http://.
    /// This is required when the server uses TLS and Http1AndHttp2 protocol mode,
    /// which allows both HTTP/1.1 (for health checks) and HTTP/2 (for gRPC) on the same port.
    /// </para>
    /// <para>
    /// In development environments with self-signed certificates, you may need to
    /// configure the HttpClient to trust the development certificate.
    /// </para>
    /// </remarks>
    public bool UseHttps { get; set; }

    internal bool UseLoopbackForLocalListeners { get; set; }

    /// <summary>Gets the metadata sent with every peer request.</summary>
    public Dictionary<string, string> RequestHeaders { get; } = [with(StringComparer.Ordinal)];

    /// <summary>
    /// Gets or sets an optional callback for validating remote HTTPS certificates.
    /// </summary>
    /// <remarks>
    /// The default is <see langword="null"/>, which uses the platform's standard certificate validation.
    /// Only override this for explicitly controlled environments such as end-to-end tests.
    /// </remarks>
    public System.Net.Security.RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

    /// <summary>
    /// Gets or sets the timeout for gRPC requests. Default: 10 seconds.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Gets or sets the deadline for join requests that await membership consensus.</summary>
    public TimeSpan JoinTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
