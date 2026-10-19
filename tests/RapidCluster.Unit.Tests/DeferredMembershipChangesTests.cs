using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class DeferredMembershipChangesTests
{
    [Fact]
    public void CompletedRemovalsCannotBecomeAdmissionsWhenDeferredChangesResume()
    {
        var previous = Members(1, 2, 3);
        var installed = Members(1, 3, 4);
        var changes = new SortedSet<Endpoint>(ProtobufEndpointComparer.Instance)
        {
            Node(1), Node(2), Node(4), Node(5),
        };

        MembershipService.RetainUnappliedChanges(changes, previous, installed);

        Assert.Equal([1, 5], changes.Select(endpoint => endpoint.Port).Order());
        Assert.DoesNotContain(changes, endpoint => endpoint.Port == 2);
        Assert.DoesNotContain(changes, endpoint => endpoint.Port == 4);
        MembershipService.RetainUnappliedChanges(changes, installed, Members(3, 4, 5));
        Assert.Empty(changes);
    }

    [Fact]
    public void ARejoinedEndpointDoesNotInheritThePreviousIncarnationsDeferredRemoval()
    {
        var previous = Members(1, 2, 3);
        var learned = Members(1, 2, 3);
        _ = learned.Remove(Node(2));
        _ = learned.Add(Utils.HostFromParts("127.0.0.1", 2, 99));
        var changes = new SortedSet<Endpoint>(ProtobufEndpointComparer.Instance) { Node(2) };

        MembershipService.RetainUnappliedChanges(changes, previous, learned);

        Assert.Empty(changes);
    }

    private static Endpoint Node(int port) => Utils.HostFromParts("127.0.0.1", port);

    private static HashSet<Endpoint> Members(params int[] ports)
        => new(ports.Select(port => Utils.HostFromParts("127.0.0.1", port, port + 10)), EndpointAddressComparer.Instance);
}
