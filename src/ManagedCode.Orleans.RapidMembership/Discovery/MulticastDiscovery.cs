using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal sealed class MulticastDiscovery : IAsyncDisposable
{
    private static readonly IPAddress Group = IPAddress.Parse("239.192.44.44");
    private readonly ConcurrentDictionary<Guid, DiscoveredPeer> _peers = [];
    private readonly Lock _observations = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly DiscoveryPacket _packets;
    private readonly DirectedDiscovery _directed;
    private readonly int _servicePort;
    private readonly int _discoveryPort;
    private readonly int _maximumPeers;
    private UdpClient? _socket;
    private Task _completion = Task.CompletedTask;
    private int _started;
    private int _disposed;

    public MulticastDiscovery(string identity, string sharedSecret, int servicePort, int discoveryPort = 11113, int maximumPeers = 10000,
        IReadOnlyList<string>? discoveryNetworks = null, int maximumAddresses = 4096, Guid? incarnation = null)
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
        NodeId = incarnation ?? Guid.NewGuid();
        _directed = new DirectedDiscovery(DiscoveryAddressRange.Expand(discoveryNetworks ?? [], maximumAddresses), discoveryPort);
    }

    public Guid NodeId { get; }

    public bool InitialScanCompleted => _directed.InitialScanCompleted;

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

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("Multicast discovery is already started.");
        }

        _socket = new UdpClient(AddressFamily.InterNetwork)
        {
            ExclusiveAddressUse = false,
        };
        _socket.Client.ReceiveBufferSize = 262144;
        _socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _socket.Client.Bind(new IPEndPoint(IPAddress.Any, _discoveryPort));
        _socket.MulticastLoopback = true;
        _socket.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
        var interfaces = MulticastInterfaces.Join(_socket, Group);
        await AnnounceRoundAsync(interfaces, cancellationToken);
        _completion = Task.WhenAll(RunAsync(ReceiveAsync), RunAsync(token => AnnounceAsync(interfaces, token)),
            RunAsync(token => _directed.ReceiveAsync(datagram => _ = ObserveDatagram(datagram), token)));
    }

    private async Task ReceiveAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            UdpReceiveResult datagram;
            try
            {
                datagram = await _socket!.ReceiveAsync(cancellationToken);
            }
            catch (SocketException exception) when (exception.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionRefused)
            {
                continue;
            }

            if (ObserveDatagram(datagram) && _servicePort > 0)
            {
                try
                {
                    await _directed.ReplyAsync(_packets.Encode(NodeId, _servicePort, DateTimeOffset.UtcNow),
                        datagram.RemoteEndPoint, cancellationToken);
                }
                catch (SocketException exception) when (MulticastInterfaces.Unavailable(exception))
                {
                    // The probing process or its return route may have disappeared before the response.
                }
            }
        }
    }

    private bool ObserveDatagram(UdpReceiveResult datagram)
    {
        var now = DateTimeOffset.UtcNow;
        if (!_packets.TryDecode(datagram.Buffer, now, out var nodeId, out var port) || nodeId == NodeId)
        {
            return false;
        }

        if (port == 0)
        {
            return true;
        }

        lock (_observations)
        {
            if (_peers.Count < _maximumPeers || _peers.ContainsKey(nodeId))
            {
                var address = datagram.RemoteEndPoint.Address;
                if (_peers.TryGetValue(nodeId, out var previous) && previous.Port == port && IPAddress.IsLoopback(previous.Address))
                {
                    address = previous.Address;
                }

                _peers[nodeId] = new DiscoveredPeer(nodeId, address, port, now);
            }
        }

        return false;
    }

    private async Task AnnounceAsync(IPAddress[] interfaces, CancellationToken cancellationToken)
    {
        while (true)
        {
            await AnnounceRoundAsync(interfaces, cancellationToken);
            RemoveExpired();
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
    }

    private async Task AnnounceRoundAsync(IPAddress[] interfaces, CancellationToken cancellationToken)
    {
        var probed = await _directed.ProbeAsync(_packets.Encode(NodeId, 0, DateTimeOffset.UtcNow),
            _peers.Values.Select(peer => peer.Address), cancellationToken);
        if (_servicePort > 0)
        {
            var announced = await MulticastInterfaces.SendAsync(_socket!, interfaces,
                _packets.Encode(NodeId, _servicePort, DateTimeOffset.UtcNow), new IPEndPoint(Group, _discoveryPort), cancellationToken);
            if (!announced && !probed)
            {
                throw new InvalidOperationException("No active interface can send discovery announcements or probes.");
            }
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
        var cutoff = DateTimeOffset.UtcNow.AddSeconds(-30);
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
            _directed.Dispose();
            _stopping.Dispose();
        }
    }
}
