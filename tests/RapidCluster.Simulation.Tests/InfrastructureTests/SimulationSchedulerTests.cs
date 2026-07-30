using Clockwork;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for the simulation scheduler and synchronization context.
/// </summary>
public sealed class SimulationSchedulerTests
{
    [Fact]
    public async Task QueuedTasksAreNotExecutedAutomatically()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UtcNow);
        var executed = false;
        var task = new Task(() => executed = true);

        task.Start(cluster.TaskScheduler);

        Assert.False(executed);
        _ = Assert.Single(cluster.TaskScheduler.Tasks);
    }

    [Fact]
    public async Task StepExecutesSingleTask()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UtcNow);
        var executed = false;
        var task = new Task(() => executed = true);
        task.Start(cluster.TaskScheduler);

        var result = cluster.SchedulerLane.RunOnce();

        Assert.True(result);
        Assert.True(executed);
        Assert.Empty(cluster.TaskScheduler.Tasks);
    }

    [Fact]
    public async Task RunUntilIdleExecutesTasksInFifoOrder()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UtcNow);
        var order = new List<int>();
        for (var i = 0; i < 5; i++)
        {
            var index = i;
            var task = new Task(() => order.Add(index));
            task.Start(cluster.TaskScheduler);
        }

        var executed = cluster.SchedulerLane.RunUntilIdle();

        Assert.Equal(5, executed);
        Assert.Equal([0, 1, 2, 3, 4], order);
    }

    [Fact]
    public async Task SynchronizationContextPostRoutesToScheduler()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UtcNow);
        var executed = false;

        cluster.SynchronizationContext.Post(_ => executed = true, state: null);

        Assert.False(executed);
        Assert.True(cluster.SchedulerLane.RunOnce());
        Assert.True(executed);
    }

    [Fact]
    public async Task RunForExecutesDelayedWorkAtVirtualTime()
    {
        var start = DateTimeOffset.UnixEpoch;
        await using var cluster = new SimulationCluster(seed: 1, start);
        var executed = false;
        cluster.SchedulerLane.EnqueueAfter(() => executed = true, TimeSpan.FromSeconds(5));

        _ = cluster.RunFor(TimeSpan.FromSeconds(4));
        Assert.False(executed);

        _ = cluster.RunFor(TimeSpan.FromSeconds(1));
        Assert.True(executed);
        Assert.Equal(start + TimeSpan.FromSeconds(5), cluster.TimeProvider.GetUtcNow());
    }
}
