using System.Net;
using ManagedCode.Orleans.RapidMembership.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class PeerSelectionTests
{
    [Fact]
    public void ColdThirtyFollowersContactTheCandidateWhileTheCandidateCanVerifyEveryPeer()
    {
        var peers = Enumerable.Range(1, 30).Select(Discovered).ToArray();
        var selection = new PeerSelection();
        var first = ConfigurationHandoffTests.Member(1);
        var last = ConfigurationHandoffTests.Member(30);
        var now = DateTimeOffset.UtcNow;
        Assert.Equal(30, selection.Select([.. peers.Where(peer => peer.NodeId != first.Incarnation)], null, first, false, now).Length);
        var follower = selection.Select([.. peers.Where(peer => peer.NodeId != last.Incarnation)], null, last, false, now);
        Assert.Equal(2, follower.Length);
        Assert.Contains(follower, member => member.Incarnation == first.Incarnation);
        Assert.Contains(follower, member => member.Incarnation == last.Incarnation);
        Assert.Empty(selection.Select([], null, null, false, now));
        Assert.Equal(first.Incarnation, Assert.Single(selection.Select(peers, null, null, false, now)).Incarnation);
    }

    [Fact]
    public void JoiningAndInstalledPeersStayReachableWhileIneligibleAndExpiredCandidatesReleaseTheirSlots()
    {
        var now = DateTimeOffset.UtcNow;
        var peers = Enumerable.Range(1, 30).Select(Discovered).ToArray();
        var selection = new PeerSelection();
        var self = ConfigurationHandoffTests.Member(30);
        _ = selection.Select(peers, null, self, false, now);
        selection.Observe([
            new PeerReply { Incarnation = peers[3].NodeId, IsJoining = true },
            new PeerReply { Incarnation = peers[4].NodeId },
        ]);
        var known = ConfigurationHandoffTests.Configuration(0, 1, 2, 3);
        var bounded = selection.Select(peers, known, self, false, now.AddSeconds(1));
        Assert.InRange(bounded.Length, 4, 8);
        Assert.All(known.Members, member => Assert.Contains(member, bounded));
        Assert.Contains(bounded, member => member.Incarnation == peers[3].NodeId);
        Assert.DoesNotContain(bounded, member => member.Incarnation == peers[4].NodeId);
        var departed = peers[3];
        peers = [.. peers.Where(peer => peer.NodeId != departed.NodeId)];
        Assert.DoesNotContain(selection.Select(peers, known, self, false, now.AddSeconds(2)), member => member.Incarnation == departed.NodeId);
        Assert.Equal(29, selection.Select(peers, known, self, true, now.AddSeconds(3)).Length);
        Assert.Equal(29, selection.Select(peers, known, self, false, now.AddSeconds(34)).Length);
        Assert.InRange(selection.Select(peers, known, self, false, now.AddSeconds(35)).Length, 4, 7);
    }

    private static DiscoveredPeer Discovered(int index)
    {
        var member = ConfigurationHandoffTests.Member(index);
        var endpoint = new Uri(member.Endpoint);
        return new DiscoveredPeer(member.Incarnation, IPAddress.Parse(endpoint.Host), endpoint.Port, DateTimeOffset.UtcNow);
    }
}
