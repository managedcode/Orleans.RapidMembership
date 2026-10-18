using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal sealed class DiscoveryPacket(string identity, string sharedSecret)
{
    internal const int Size = 96;
    private readonly byte[] _identity = SHA256.HashData(Encoding.UTF8.GetBytes(identity));
    private readonly byte[] _key = SHA256.HashData(Encoding.UTF8.GetBytes(sharedSecret));

    public byte[] Encode(Guid nodeId, int servicePort, DateTimeOffset now)
    {
        var packet = new byte[Size];
        "RMC1"u8.CopyTo(packet);
        _identity.CopyTo(packet, 4);
        _ = nodeId.TryWriteBytes(packet.AsSpan(36, 16));
        BinaryPrimitives.WriteUInt16BigEndian(packet.AsSpan(52, 2), checked((ushort)servicePort));
        BinaryPrimitives.WriteInt64BigEndian(packet.AsSpan(56, 8), now.ToUnixTimeSeconds());
        _ = HMACSHA256.HashData(_key, packet.AsSpan(0, 64), packet.AsSpan(64, 32));
        return packet;
    }

    public bool TryDecode(byte[] packet, DateTimeOffset now, out Guid nodeId, out int servicePort)
    {
        nodeId = default;
        servicePort = 0;
        if (packet.Length != Size || !packet.AsSpan(0, 4).SequenceEqual("RMC1"u8)
            || !CryptographicOperations.FixedTimeEquals(packet.AsSpan(4, 32), _identity))
        {
            return false;
        }

        Span<byte> signature = stackalloc byte[32];
        _ = HMACSHA256.HashData(_key, packet.AsSpan(0, 64), signature);
        var seconds = BinaryPrimitives.ReadInt64BigEndian(packet.AsSpan(56, 8));
        if (!CryptographicOperations.FixedTimeEquals(packet.AsSpan(64, 32), signature)
            || seconds < now.ToUnixTimeSeconds() - 10 || seconds > now.ToUnixTimeSeconds() + 10)
        {
            return false;
        }

        nodeId = new Guid(packet.AsSpan(36, 16));
        servicePort = BinaryPrimitives.ReadUInt16BigEndian(packet.AsSpan(52, 2));
        return nodeId != Guid.Empty;
    }
}
