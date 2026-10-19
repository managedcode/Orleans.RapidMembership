using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class CutDetectorViewScopeTests
{
    [Theory]
    [InlineData(6)]
    [InlineData(5)]
    public void RemovedMembersCannotBePromotedBackFromReportsForThePreviousView(int count)
    {
        var view = View(count);
        var detector = new CutDetector(4, 3, 2);
        detector.UpdateView(view);
        var removed = view.Members[0];
        _ = Report(detector, view, removed, EdgeStatus.Down, 2);
        Assert.True(detector.HasNodesInUnstableMode());
        var builder = view.ToBuilder(4);
        _ = builder.RingDelete(removed);
        var next = builder.Build(view.ConfigurationId);
        detector.UpdateView(next);

        Assert.False(detector.HasNodesInUnstableMode());
        Assert.Empty(detector.ForcePromoteUnstableNodes());
        var joining = Utils.HostFromParts("127.0.0.200", 13200, 200);
        var healthy = Report(detector, next, joining, EdgeStatus.Up, next.RingCount);
        Assert.Contains(healthy, endpoint => EndpointAddressComparer.Instance.Equals(endpoint, joining));
        Assert.DoesNotContain(healthy, endpoint => EndpointAddressComparer.Instance.Equals(endpoint, removed));
    }

    [Theory]
    [InlineData(6)]
    [InlineData(5)]
    public void NewMembershipRequiresFreshObserverReportsForAnUnfinishedAdmission(int count)
    {
        var view = View(count);
        var detector = new CutDetector(4, 3, 2);
        detector.UpdateView(view);
        var joining = Utils.HostFromParts("127.0.0.200", 13200, 200);
        _ = Report(detector, view, joining, EdgeStatus.Up, 2);
        Assert.True(detector.HasNodesInUnstableMode());
        var builder = view.ToBuilder(4);
        _ = builder.RingDelete(view.Members[0]);
        var next = builder.Build(view.ConfigurationId);
        detector.UpdateView(next);

        Assert.False(detector.HasNodesInUnstableMode());
        Assert.Empty(detector.ForcePromoteUnstableNodes());
        Assert.Contains(Report(detector, next, joining, EdgeStatus.Up, next.RingCount),
            endpoint => EndpointAddressComparer.Instance.Equals(endpoint, joining));
    }

    private static MembershipView View(int count)
    {
        var builder = new MembershipViewBuilder(4);
        for (var index = 0; index < count; index++)
        {
            _ = builder.RingAdd(Utils.HostFromParts("127.0.0.1", 13100 + index, index + 1));
        }

        return builder.Build();
    }

    private static List<Endpoint> Report(CutDetector detector, MembershipView view, Endpoint subject,
        EdgeStatus status, int count)
    {
        var observers = status == EdgeStatus.Down ? view.GetObserversOf(subject) : view.GetExpectedObserversOf(subject);
        var proposals = new List<Endpoint>();
        for (var ring = 0; ring < count; ring++)
        {
            var alert = new AlertMessage
            {
                EdgeSrc = observers[ring],
                EdgeDst = subject,
                EdgeStatus = status,
                ConfigurationId = view.ConfigurationId.ToProtobuf(),
            };
            alert.RingNumber.Add(ring);
            proposals.AddRange(detector.AggregateForProposal(alert));
        }

        return proposals;
    }
}
