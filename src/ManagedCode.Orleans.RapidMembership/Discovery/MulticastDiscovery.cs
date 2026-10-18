using System.Collections.Concurrent;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal sealed class MulticastDiscovery : IAsyncDisposable
{
    private static readonly IPAddress Group = IPAddress.Parse("239.192.44.44");
    private readonly ConcurrentDictionary<Guid, DiscoveredPeer> _peers = [];
    private readonly CancellationTokenSource _stopping = new();
    private readonly DiscoveryPacket _packets;
    private readonly int _servicePort;
    private readonly int _discoveryPort;
    private readonly int _maximumPeers;
    private UdpClient? _socket;
    private Task _completion = Task.CompletedTask;
    private int _started;
    private int _disposed;

    public MulticastDiscovery(string identity, string sharedSecret, int servicePort, int discoveryPort = 11113, int maximumPeers = 10000)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(servicePort);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(servicePort, 65535);
        ArgumentOutOfRangeException.ThrowIfLessThan(discoveryPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(discoveryPort, 65535);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumPeers, 1);
        _packets = new DiscoveryPacket(identity, sharedSecret);
        _servicePort = servicePort;
        _discoveryPort = discoveryPort;
        _maximumPeers = maximumPeers;
    }

    public Guid NodeId { get; } = Guid.NewGuid();

    public IReadOnlyList<DiscoveredPeer> Peers
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_completion.IsFaulted)
            {
                throw new InvalidOperationException("Multicast discovery stopped unexpectedly.", _completion.Exception);
            }

            RemoveExpired();
            return [.. _peers.Values.OrderBy(peer => peer.NodeId)];
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("Multicast discovery is already started.");
        }

        var interfaces = NetworkInterface.GetAllNetworkInterfaces()
            .Where(network => network.OperationalStatus == OperationalStatus.Up && network.SupportsMulticast)
            .SelectMany(network => network.GetIPProperties().UnicastAddresses)
            .Where(address => address.Address.AddressFamily == AddressFamily.InterNetwork)
            .Select(address => address.Address).Distinct().ToArray();
        if (interfaces.Length == 0)
        {
            throw new InvalidOperationException("No active IPv4 multicast interface is available.");
        }

        _socket = new UdpClient(AddressFamily.InterNetwork)
        {
            ExclusiveAddressUse = false,
        };
        _socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _socket.Client.Bind(new IPEndPoint(IPAddress.Any, _discoveryPort));
        _socket.MulticastLoopback = true;
        _socket.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
        foreach (var address in interfaces)
        {
            _socket.JoinMulticastGroup(Group, address);
        }

        _completion = Task.WhenAll(RunAsync(ReceiveAsync), RunAsync(token => AnnounceAsync(interfaces, token)));
        return Task.CompletedTask;
    }

    private async Task ReceiveAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var datagram = await _socket!.ReceiveAsync(cancellationToken);
            var now = DateTimeOffset.UtcNow;
            if (!_packets.TryDecode(datagram.Buffer, now, out var nodeId, out var port) || nodeId == NodeId || port == 0)
            {
                continue;
            }

            if (_peers.Count < _maximumPeers || _peers.ContainsKey(nodeId))
            {
                _peers[nodeId] = new DiscoveredPeer(nodeId, datagram.RemoteEndPoint.Address, port, now);
            }
        }
    }

    private async Task AnnounceAsync(IPAddress[] interfaces, CancellationToken cancellationToken)
    {
        var destination = new IPEndPoint(Group, _discoveryPort);
        while (true)
        {
            if (_servicePort > 0)
            {
                var packet = _packets.Encode(NodeId, _servicePort, DateTimeOffset.UtcNow);
                foreach (var address in interfaces)
                {
                    _socket!.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, address.GetAddressBytes());
                    _ = await _socket.SendAsync(packet, destination, cancellationToken);
                }
            }

            RemoveExpired();
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
    }

    private async Task RunAsync(Func<CancellationToken, Task> operation)
    {
        try
        {
            await operation(_stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
        }
        catch
        {
            await _stopping.CancelAsync();
            throw;
        }
    }

    private void RemoveExpired()
    {
        var cutoff = DateTimeOffset.UtcNow.AddSeconds(-5);
        foreach (var peer in _peers)
        {
            if (peer.Value.LastSeen < cutoff)
            {
                _ = _peers.TryRemove(peer);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            await _stopping.CancelAsync();
            await _completion;
        }
        finally
        {
            _socket?.Dispose();
            _stopping.Dispose();
        }
    }
}
