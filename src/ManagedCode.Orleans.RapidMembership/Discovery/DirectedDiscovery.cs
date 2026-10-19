using System.Net;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal sealed class DirectedDiscovery(IReadOnlyList<IPAddress> addresses, int port) : IDisposable
{
    private readonly UdpClient _socket = new(new IPEndPoint(IPAddress.Any, 0))
    {
        EnableBroadcast = true,
        Client = { ReceiveBufferSize = 262144 },
    };

    private int _cursor;
    private int _scanned;

    public bool InitialScanCompleted => Volatile.Read(ref _scanned) >= addresses.Count;

    public async Task ReceiveAsync(Action<UdpReceiveResult> receive, CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                receive(await _socket.ReceiveAsync(cancellationToken));
            }
            catch (SocketException exception) when (exception.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionRefused)
            {
                // ICMP rejection reports an absent listener and does not stop other probes.
            }
        }
    }

    public async Task<bool> ProbeAsync(byte[] packet, IEnumerable<IPAddress> known, CancellationToken cancellationToken)
    {
        var batch = new List<IPAddress> { IPAddress.Loopback, IPAddress.Parse("127.255.255.255"), IPAddress.Broadcast };
        batch.AddRange(known.Distinct());
        for (var count = 0; count < Math.Min(256, addresses.Count); count++)
        {
            batch.Add(addresses[_cursor]);
            _cursor = (_cursor + 1) % addresses.Count;
        }

        var sent = false;
        foreach (var address in batch.Distinct())
        {
            try
            {
                _ = await _socket.SendAsync(packet, new IPEndPoint(address, port), cancellationToken);
                sent = true;
            }
            catch (SocketException exception) when (MulticastInterfaces.Unavailable(exception))
            {
                // A failed probe is an unavailable address; it grants no membership or quorum acknowledgement.
            }
        }

        Volatile.Write(ref _scanned, Math.Min(addresses.Count, _scanned + Math.Min(256, addresses.Count)));
        return sent;
    }

    public async Task ReplyAsync(byte[] packet, IPEndPoint destination, CancellationToken cancellationToken)
        => _ = await _socket.SendAsync(packet, destination, cancellationToken);

    public void Dispose() => _socket.Dispose();
}
