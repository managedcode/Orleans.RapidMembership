using System.Net;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal sealed record DiscoveredPeer(Guid NodeId, IPAddress Address, int Port, DateTimeOffset LastSeen)
{
    public string Endpoint => new UriBuilder(Uri.UriSchemeHttp, Address.ToString(), Port).Uri.GetLeftPart(UriPartial.Authority);
}
