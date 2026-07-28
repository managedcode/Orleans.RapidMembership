using Clockwork;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for the simulation test harness.
/// </summary>
public sealed class SimulationTestHarnessTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;

    public ValueTask InitializeAsync()
    {
        // Use a fixed seed for reproducibility
        _harness = new RapidSimulationCluster(seed: 12345);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public void SimulationRandomWithSameSeedProducesSameSequence()
    {
        var random1 = new SimulationRandom(42);
        var random2 = new SimulationRandom(42);

        for (var i = 0; i < 100; i++)
        {
#pragma warning disable CA5394 // Do not use insecure randomness
            Assert.Equal(random1.Next(), random2.Next());
#pragma warning restore CA5394 // Do not use insecure randomness
        }
    }

    [Fact]
    public void SimulationRandomShuffleIsReproducible()
    {
        var random1 = new SimulationRandom(42);
        var random2 = new SimulationRandom(42);

        var list1 = new List<int> { 1, 2, 3, 4, 5 };
        var list2 = new List<int> { 1, 2, 3, 4, 5 };

        random1.Shuffle(list1);
        random2.Shuffle(list2);

        Assert.Equal(list1, list2);
    }

    [Fact]
    public void CreateSeedNodeInitializesWithOneNode()
    {
        var seedNode = _harness.CreateSeedNode();

        Assert.NotNull(seedNode);
        Assert.True(seedNode.IsInitialized);
        Assert.Equal(1, seedNode.MembershipSize);
    }

    [Fact]
    public void JoinNodeIncreasesMembershipSize()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.NotNull(joiner);
        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void CreateClusterCreatesCorrectNumberOfNodes()
    {
        var nodes = _harness.CreateCluster(size: 3);

        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, node => Assert.True(node.IsInitialized));

        // All nodes should see 3 members
        foreach (var node in nodes)
        {
            Assert.Equal(3, node.MembershipSize);
        }
    }

    [Fact]
    public void NetworkPartitionBlocksMessages()
    {
        var seedNode = _harness.CreateSeedNode();

        // Isolate the node
        _harness.IsolateNode(seedNode);

        // Verify we can reconnect
        _harness.ReconnectNode(seedNode);
    }

    [Fact]
    public void SimulationEnvironmentCreatesDerivedRandom()
    {
        var derived1 = _harness.CreateDerivedRandom();
        var derived2 = _harness.CreateDerivedRandom();

        // Different derived randoms should produce different sequences
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq1 = Enumerable.Range(0, 10).Select(_ => derived1.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq2 = Enumerable.Range(0, 10).Select(_ => derived2.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness

        Assert.NotEqual(seq1, seq2);
    }

    [Fact]
    public async Task NodeRandomStreamDependsOnStableAddressNotConstructionOrder()
    {
        await using var firstHarness = new RapidSimulationCluster(seed: 24680);
        await using var reorderedHarness = new RapidSimulationCluster(seed: 24680);

        var firstNode = firstHarness.CreateUninitializedNode(nodeId: 7);
        _ = reorderedHarness.CreateUninitializedNode(nodeId: 99);
        var reorderedNode = reorderedHarness.CreateUninitializedNode(nodeId: 7);

#pragma warning disable CA5394 // Do not use insecure randomness
        var firstSequence = Enumerable.Range(0, 10).Select(_ => firstNode.Context.Random.Next()).ToList();
        var reorderedSequence = Enumerable.Range(0, 10).Select(_ => reorderedNode.Context.Random.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness

        Assert.Equal(firstSequence, reorderedSequence);
    }

    [Fact]
    public void SimulationNetworkCanSetMessageDropRate()
    {
        _harness.Network.MessageDropRate = 0.5;
        Assert.Equal(0.5, _harness.Network.MessageDropRate);
    }

    [Fact]
    public void SimulationNetworkCanSetDelays()
    {
        _harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);
        _harness.Network.MaxJitter = TimeSpan.FromMilliseconds(20);

        Assert.Equal(TimeSpan.FromMilliseconds(10), _harness.Network.BaseMessageDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(20), _harness.Network.MaxJitter);
    }

    [Fact]
    public void SeedIsAccessible() => Assert.Equal(12345, _harness.Seed);

    [Fact]
    public void SubscribeToViewChangesReceivesNotifications()
    {
        var viewChanges = new List<MembershipView>();
        var seedNode = _harness.CreateSeedNode();

        var subscription = _harness.InvokeInSimulation(
            seedNode,
            () => seedNode.ViewAccessor.Updates.Subscribe(new ViewObserver(viewChanges.Add)));

        try
        {
            _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);
            Assert.Contains(viewChanges, view => view.Size == 2);
        }
        finally
        {
            _harness.ExecuteInSimulation(seedNode, subscription.Dispose);
        }
    }

    private sealed class ViewObserver(Action<MembershipView> onNext) : IObserver<MembershipView>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error) => throw error;

        public void OnNext(MembershipView value) => onNext(value);
    }
}
