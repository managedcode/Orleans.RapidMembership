using System.Collections.Immutable;
using System.Net;
using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.CoordinationMember")]
internal readonly record struct CoordinationMember([property: Id(0)] Guid Incarnation, [property: Id(1)] string Endpoint);

[GenerateSerializer]
[Alias("ManagedCode.RapidMembership.CoordinationConfiguration")]
internal sealed record CoordinationConfiguration(
    [property: Id(0)] long ClusterId,
    [property: Id(1)] long Version,
    [property: Id(2)] ImmutableArray<CoordinationMember> Members)
{
    public int Quorum => Members.Length / 2 + 1;

    public bool Contains(Guid incarnation) => Members.Any(member => member.Incarnation == incarnation);

    public bool Matches(CoordinationConfiguration? other) => other is not null
        && ClusterId == other.ClusterId && Version == other.Version && Members.SequenceEqual(other.Members);

    public void Validate()
    {
        if (Version < 0 || Members.IsDefaultOrEmpty || Members.Length > 10000)
        {
            throw new InvalidDataException("Invalid coordination configuration bounds.");
        }

        Guid? previous = null;
        var endpoints = new HashSet<string>(StringComparer.Ordinal);
        foreach (var member in Members)
        {
            if (member.Incarnation == Guid.Empty || previous is { } before && before.CompareTo(member.Incarnation) >= 0
                || !ValidEndpoint(member.Endpoint) || !endpoints.Add(member.Endpoint))
            {
                throw new InvalidDataException("Coordination members require unique incarnations and endpoints in canonical order.");
            }

            previous = member.Incarnation;
        }
    }

    private static bool ValidEndpoint(string endpoint) => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
        && uri.Scheme == Uri.UriSchemeHttp && IPAddress.TryParse(uri.Host.Trim('[', ']'), out var address)
        && !IPAddress.Any.Equals(address) && !IPAddress.IPv6Any.Equals(address)
        && uri.Port > 0 && endpoint == uri.GetLeftPart(UriPartial.Authority);
}
