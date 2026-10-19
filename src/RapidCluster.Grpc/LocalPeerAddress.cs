using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace RapidCluster.Grpc;

internal static class LocalPeerAddress
{
    public static Uri ForLocalListener(Uri endpoint)
    {
        if (endpoint.Scheme == Uri.UriSchemeHttp && IPAddress.TryParse(endpoint.Host, out var address)
            && address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address) && IsAssigned(address))
        {
            return new UriBuilder(endpoint) { Host = IPAddress.Loopback.ToString() }.Uri;
        }

        return endpoint;
    }

    public static bool IsAssigned(IPAddress address) => NetworkInterface.GetAllNetworkInterfaces()
        .Where(network => network.OperationalStatus == OperationalStatus.Up)
        .SelectMany(network => network.GetIPProperties().UnicastAddresses)
        .Any(value => value.Address.Equals(address));
}
