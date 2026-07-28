using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for the invariant checker.
/// </summary>
public sealed class InvariantCheckerTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private InvariantChecker _checker = null!;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: 11111);
        _checker = new InvariantChecker(_harness);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public void CheckAllPassesWithNoNodes()
    {
        var result = _checker.CheckAll();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckAllPassesWithSingleNode()
    {
        _ = _harness.StartCluster();

        var result = _checker.CheckAll();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckTransitionalMembershipSizeBoundPassesWithSingleNode()
    {
        _ = _harness.StartCluster();

        var result = _checker.CheckAtMostTwoTransitionalMembershipSizes();

        Assert.True(result);
    }

    [Fact]
    public void CheckTransitionalMembershipSizeBoundDetectsThreeSizes()
    {
        var seed = _harness.StartCluster(nodeId: 0);
        _ = _harness.JoinNode(nodeId: 1, seed);
        _ = _harness.StartCluster(nodeId: 10);
        _ = _harness.AddUnstartedNode(nodeId: 20);

        Assert.False(_checker.CheckAtMostTwoTransitionalMembershipSizes());
        Assert.Contains(
            _checker.Violations,
            violation => violation.Type == InvariantType.TransitionalMembershipSizeBound);
    }

    [Fact]
    public void CheckNoSplitBrainPassesWithSingleNode()
    {
        _ = _harness.StartCluster();

        var result = _checker.CheckNoSplitBrain();

        Assert.True(result);
    }

    [Fact]
    public void CheckConfigurationIdMonotonicityPassesWithInitializedNode()
    {
        _ = _harness.StartCluster();

        var result = _checker.CheckConfigurationIdMonotonicity();

        Assert.True(result);
    }

    [Fact]
    public void CheckConfigurationIdMonotonicityDetectsDecreasingVersion()
    {
        var seed = _harness.StartCluster(nodeId: 0);
        var joiner = _harness.JoinNode(nodeId: 1, seed);
        _harness.WaitForConvergence();
        Assert.True(_checker.CheckConfigurationIdMonotonicity());

        _harness.CrashNode(joiner);
        _harness.CrashNode(seed);
        _ = _harness.StartCluster(nodeId: 0);

        Assert.False(_checker.CheckConfigurationIdMonotonicity());
        var violation = Assert.Single(
            _checker.Violations,
            violation => violation.Type == InvariantType.ConfigurationIdMonotonicity);
        Assert.Contains("decreased", violation.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ClearRemovesAllViolations()
    {
        // Force some violations by manually adding them (would need internal access)
        // For now, just verify Clear doesn't throw
        _checker.Clear();

        Assert.False(_checker.HasViolations);
        Assert.Empty(_checker.Violations);
    }

    [Fact]
    public void ViolationsListIsImmutableCopy()
    {
        var violations1 = _checker.Violations;
        var violations2 = _checker.Violations;

        Assert.NotSame(violations1, violations2);
    }
}
