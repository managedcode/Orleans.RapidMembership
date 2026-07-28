using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for determinism verification using the simulation harness.
/// Ensures that simulation runs are reproducible with the same seed.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class DeterminismTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 89012;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task SameSeedProducesSameRandomSequence()
    {
        await using var harness1 = new RapidSimulationCluster(seed: 11111);
        await using var harness2 = new RapidSimulationCluster(seed: 11111);

#pragma warning disable CA5394 // Do not use insecure randomness
        var seq1 = Enumerable.Range(0, 100).Select(_ => harness1.Random.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq2 = Enumerable.Range(0, 100).Select(_ => harness2.Random.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness

        Assert.Equal(seq1, seq2);
    }

    [Fact]
    public async Task DifferentSeedsProduceDifferentSequences()
    {
        await using var harness1 = new RapidSimulationCluster(seed: 11111);
        await using var harness2 = new RapidSimulationCluster(seed: 22222);

#pragma warning disable CA5394 // Do not use insecure randomness
        var seq1 = Enumerable.Range(0, 100).Select(_ => harness1.Random.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq2 = Enumerable.Range(0, 100).Select(_ => harness2.Random.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness

        Assert.NotEqual(seq1, seq2);
    }

    [Fact]
    public void ForkedRandomIsDeterministic()
    {
        var random = _harness.Random;

        // Fork at same state should produce same results
        var fork1 = random.Fork();
#pragma warning disable CA5394 // Do not use insecure randomness
        var state = random.Next(); // Consume one value
#pragma warning restore CA5394 // Do not use insecure randomness
        var fork2 = random.Fork();

        // fork1 and fork2 should produce different sequences (since base random advanced)
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq1 = Enumerable.Range(0, 10).Select(_ => fork1.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness
#pragma warning disable CA5394 // Do not use insecure randomness
        var seq2 = Enumerable.Range(0, 10).Select(_ => fork2.Next()).ToList();
#pragma warning restore CA5394 // Do not use insecure randomness

        Assert.NotEqual(seq1, seq2);
    }

    [Fact]
    public void TimeAdvancementIsDeterministic()
    {
        var initialTime = _harness.TimeProvider.GetUtcNow();

        _ = _harness.RunFor(TimeSpan.FromMinutes(5));

        var newTime = _harness.TimeProvider.GetUtcNow();
        Assert.Equal(initialTime + TimeSpan.FromMinutes(5), newTime);
    }

    [Fact]
    public void RunUntilReturnsWhenConditionMet()
    {
        var conditionMet = false;
        var task = new Task(() => conditionMet = true);
        task.Start(_harness.TaskScheduler);

        var result = _harness.RunUntil(() => conditionMet);

        Assert.True(result.ConditionMet, result.ToDetailedString());
        Assert.True(conditionMet);
    }

    [Fact]
    public void RunUntilReturnsFalseWhenMaxStepsReached()
    {
        var result = _harness.RunUntil(() => false, maxIterations: 10);

        Assert.False(result.ConditionMet);
    }

    [Fact]
    public void WaitForMembershipSizeWorks()
    {
        _ = _harness.StartCluster();

        // The condition should already be met since the single node is converged.
        _harness.WaitForMembershipSize(expectedSize: 1);
    }

    [Fact]
    public void SeedIsAccessible() => Assert.Equal(TestSeed, _harness.Seed);
}
