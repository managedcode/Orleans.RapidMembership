using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace ManagedCode.Orleans.RapidMembership.Discovery;

internal static class DiscoveryAddressRange
{
    public static IReadOnlyList<IPAddress> Expand(IReadOnlyList<string> networks, int maximum)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maximum, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximum, 65536);
        var result = new HashSet<IPAddress>();
        foreach (var network in networks)
        {
            var (first, last) = Parse(network);
            if (last < first || (ulong)last - first + 1 > (ulong)maximum)
            {
                throw new ArgumentException("The discovery address range exceeds its configured bound.", nameof(networks));
            }

            for (ulong value = first; value <= last; value++)
            {
                var bytes = new byte[4];
                BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)value);
                _ = result.Add(new IPAddress(bytes));
                if (result.Count > maximum)
                {
                    throw new ArgumentException("The combined discovery networks exceed the configured address bound.", nameof(networks));
                }
            }
        }

        return [.. result.OrderBy(address => Number(address.ToString()))];
    }

    private static (uint First, uint Last) Parse(string network)
    {
        var range = network.Split('-', StringSplitOptions.TrimEntries);
        if (range.Length == 2)
        {
            return (Number(range[0]), Number(range[1]));
        }

        var subnet = network.Split('/', StringSplitOptions.TrimEntries);
        if (subnet.Length != 2 || !int.TryParse(subnet[1], System.Globalization.CultureInfo.InvariantCulture, out var prefix) || prefix is < 0 or > 32)
        {
            throw new ArgumentException("Use an IPv4 CIDR subnet or an inclusive start-end IPv4 address range.", nameof(network));
        }

        var mask = prefix == 0 ? 0U : uint.MaxValue << (32 - prefix);
        var first = Number(subnet[0]) & mask;
        var last = first | ~mask;
        return prefix < 31 ? (first + 1, last - 1) : (first, last);
    }

    private static uint Number(string value)
    {
        if (!IPAddress.TryParse(value, out var address) || address.AddressFamily != AddressFamily.InterNetwork)
        {
            throw new ArgumentException("Discovery networks require IPv4 addresses.", nameof(value));
        }

        return BinaryPrimitives.ReadUInt32BigEndian(address.GetAddressBytes());
    }
}
