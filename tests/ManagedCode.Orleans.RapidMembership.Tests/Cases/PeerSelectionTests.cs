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

    [Fact]
    public void UnknownFollowersRescanAllPeersToFindAnAlreadyFormedGroup()
    {
        var peers = Enumerable.Range(1, 30).Select(Discovered).ToArray();
        var selection = new PeerSelection();
        var self = ConfigurationHandoffTests.Member(30);
        var now = DateTimeOffset.UtcNow;
        Assert.Equal(2, selection.Select(peers, null, self, false, now).Length);
        Assert.Equal(2, selection.Select(peers, null, self, false, now.AddSeconds(29)).Length);
        Assert.Equal(30, selection.Select(peers, null, self, false, now.AddSeconds(30)).Length);
        Assert.Equal(2, selection.Select(peers, null, self, false, now.AddSeconds(31)).Length);
        Assert.Equal(30, selection.Select(peers, null, self, false, now.AddSeconds(60)).Length);
        Assert.Equal(30, selection.Select(peers, null, self, true, now.AddSeconds(61)).Length);
    }

    [Fact]
    public async Task RescanFindsAnInstalledQuorumBehindAnUnconfiguredCandidateOverRealHttp2()
    {
        var token = TestContext.Current.CancellationToken;
        await using var network = await ReplicaNetwork.StartAsync(token);
        var members = network.Members;
        var initial = new CoordinationConfiguration(42, 0, [members[1]]);
        var installed = new CoordinationConfiguration(42, 1, [members[1], members[2]]);
        await network.Replicas.BootstrapAsync(initial);
        await network.Replicas.InstallAsync(initial, installed, await network.Replicas.FreezeAsync(initial, installed));
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        var peers = members.Select(member => new DiscoveredPeer(member.Incarnation, IPAddress.Loopback,
            new Uri(member.Endpoint).Port, clock.GetUtcNow())).ToArray();
        var selection = new PeerSelection();
        var resolver = new ConfigurationResolver(network.Transport);
        var first = selection.Select(peers, null, null, false, clock.GetUtcNow());
        Assert.Equal(members[0], Assert.Single(first));
        Assert.Null(await resolver.ResolveAsync(await network.Transport.LocateAsync(first, token), token));
        clock.Advance(TimeSpan.FromSeconds(30));
        var rescanned = selection.Select(peers, null, null, false, clock.GetUtcNow());
        var descriptions = await network.Transport.LocateAsync(rescanned, token);
        Assert.True(installed.Matches(await resolver.ResolveAsync(descriptions, token)));
        var snapshot = MemoryReplicaHandoffTests.Snapshot(1);
        foreach (var action in new[] { PeerAction.Prepare, PeerAction.Accept, PeerAction.Commit })
        {
            var replies = await network.Transport.ExchangeAsync(
                MemoryReplicaHandoffTests.Proposal(installed, action, snapshot, 1), installed, token);
            Assert.Equal(installed.Members.Length, replies.Count(reply => reply.Granted));
        }

        foreach (var member in installed.Members)
        {
            var reply = await network.Transport.SendAsync(member, new PeerRequest { Action = PeerAction.Inspect, Configuration = installed }, token);
            Assert.True(MembershipSnapshotEquality.AreEqual(snapshot, reply!.State.Committed));
        }
    }

    private static DiscoveredPeer Discovered(int index)
    {
        var member = ConfigurationHandoffTests.Member(index);
        var endpoint = new Uri(member.Endpoint);
        return new DiscoveredPeer(member.Incarnation, IPAddress.Parse(endpoint.Host), endpoint.Port, DateTimeOffset.UtcNow);
    }
}
