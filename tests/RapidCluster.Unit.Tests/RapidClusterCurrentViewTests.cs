namespace RapidCluster.Unit.Tests;

public sealed class RapidClusterCurrentViewTests
{
    [Fact]
    public void CurrentViewImmediatelyReflectsAPublishedNativeView()
    {
        using var accessor = new MembershipViewAccessor();
        using var cluster = new RapidClusterImpl(accessor);
        accessor.PublishView(Utils.CreateMembershipView(3));
        Assert.Equal(3, cluster.CurrentView.Members.Length);
    }

    [Fact]
    public void AConsumerCreatedAfterInitializationSeesTheExistingView()
    {
        using var accessor = new MembershipViewAccessor();
        accessor.PublishView(Utils.CreateMembershipView(4));
        using var cluster = new RapidClusterImpl(accessor);
        Assert.Equal(4, cluster.CurrentView.Members.Length);
    }
}
