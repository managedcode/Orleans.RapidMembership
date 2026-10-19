using System.Net;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class DiscoveryPortReservation : IDisposable
{
    private readonly UdpClient _socket = new(AddressFamily.InterNetwork) { ExclusiveAddressUse = false };

    public DiscoveryPortReservation()
    {
        try
        {
            _socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _socket.Client.Bind(new IPEndPoint(IPAddress.Any, 0));
        }
        catch
        {
            _socket.Dispose();
            throw;
        }
    }

    public int Port => ((IPEndPoint)_socket.Client.LocalEndPoint!).Port;

    public void Dispose() => _socket.Dispose();
}
