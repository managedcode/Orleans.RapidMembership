using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal static class MulticastInterfaces
{
    public static IPAddress[] Join(UdpClient socket, IPAddress group)
    {
        var addresses = NetworkInterface.GetAllNetworkInterfaces()
            .Where(network => network.OperationalStatus == OperationalStatus.Up && network.SupportsMulticast)
            .SelectMany(network => network.GetIPProperties().UnicastAddresses)
            .Where(address => address.Address.AddressFamily == AddressFamily.InterNetwork)
            .Select(address => address.Address).Append(IPAddress.Loopback).Distinct().ToArray();
        return Join(socket, group, addresses);
    }

    internal static IPAddress[] Join(UdpClient socket, IPAddress group, IReadOnlyList<IPAddress> addresses)
    {
        var joined = new List<IPAddress>();
        foreach (var address in addresses)
        {
            try
            {
                socket.JoinMulticastGroup(group, address);
                joined.Add(address);
            }
            catch (SocketException exception) when (Unavailable(exception, address))
            {
                // Other interfaces and authenticated directed probes remain independently usable.
            }
        }

        return [.. joined];
    }

    public static async Task<bool> SendAsync(UdpClient socket, IPAddress[] interfaces, byte[] packet,
        IPEndPoint destination, CancellationToken cancellationToken)
    {
        var sent = false;
        foreach (var address in interfaces)
        {
            try
            {
                socket.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, address.GetAddressBytes());
                _ = await socket.SendAsync(packet, destination, cancellationToken);
                sent = true;
            }
            catch (SocketException exception) when (Unavailable(exception, address))
            {
                // Failure on one route does not invalidate successful sends on another interface.
            }
        }

        return sent;
    }

    private static bool Unavailable(SocketException exception, IPAddress address) => Unavailable(exception.SocketErrorCode)
        || OperatingSystem.IsLinux() && exception.SocketErrorCode == SocketError.SocketError && !IsAssigned(address);

    internal static bool IsAssigned(IPAddress address) => NetworkInterface.GetAllNetworkInterfaces()
        .Where(network => network.OperationalStatus == OperationalStatus.Up)
        .SelectMany(network => network.GetIPProperties().UnicastAddresses)
        .Any(unicast => unicast.Address.Equals(address));

    internal static bool Unavailable(SocketError error) => error is SocketError.HostUnreachable or SocketError.NetworkUnreachable
        or SocketError.AddressNotAvailable or SocketError.NetworkDown or SocketError.AccessDenied
        or SocketError.ConnectionRefused or SocketError.ConnectionReset;
}
