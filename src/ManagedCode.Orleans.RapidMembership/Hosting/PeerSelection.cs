using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class PeerSelection
{
    private readonly Dictionary<Guid, (bool Eligible, bool Joining)> _roles = [];
    private DateTimeOffset _fullScan;

    public void Observe(IReadOnlyList<PeerReply> replies)
    {
        foreach (var reply in replies)
        {
            _roles[reply.Incarnation] = (reply.CanCoordinate, reply.IsJoining);
        }
    }

    public CoordinationMember[] Select(IReadOnlyList<DiscoveredPeer> peers, CoordinationConfiguration? known,
        CoordinationMember? self, bool force, DateTimeOffset now)
    {
        var discovered = peers.Select(peer => new CoordinationMember(peer.NodeId, peer.Endpoint)).OrderBy(member => member.Incarnation).ToArray();
        var visible = discovered.Concat(self is { } local ? [local] : []).DistinctBy(member => member.Incarnation).ToArray();
        var retained = visible.Select(member => member.Incarnation).Concat(known?.Members.Select(member => member.Incarnation) ?? []).ToHashSet();
        foreach (var expired in _roles.Keys.Where(incarnation => !retained.Contains(incarnation)).ToArray())
        {
            _ = _roles.Remove(expired);
        }

        IEnumerable<CoordinationMember> selected;
        if (known is null)
        {
            _fullScan = now;
            if (visible.Length == 0)
            {
                return [];
            }

            var candidate = visible.MinBy(member => member.Incarnation);
            selected = self is { } host && candidate.Incarnation == host.Incarnation ? discovered : visible.OrderBy(member => member.Incarnation).Take(1);
        }
        else if (force || now - _fullScan >= TimeSpan.FromSeconds(30))
        {
            _fullScan = now;
            selected = known.Members.Concat(discovered);
        }
        else
        {
            var joining = discovered.Where(member => _roles.TryGetValue(member.Incarnation, out var role) && role.Joining);
            var candidates = discovered.Where(member => !known.Contains(member.Incarnation)
                && (!_roles.TryGetValue(member.Incarnation, out var role) || role.Eligible)).Take(3);
            selected = known.Members.Concat(joining).Concat(candidates);
        }

        return [.. selected.Concat(self is { } owner ? [owner] : []).DistinctBy(member => member.Incarnation)];
    }
}
