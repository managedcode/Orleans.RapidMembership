using Clockwork;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for <see cref="SimulationTimeProvider"/> through the public cluster API.
/// </summary>
public sealed class SimulationTimeProviderTests
{
    [Fact]
    public async Task TimeQueriesUseConfiguredVirtualTimeline()
    {
        var start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using var cluster = new SimulationCluster(seed: 1, start);

        Assert.Equal(start, cluster.TimeProvider.GetUtcNow());
        Assert.Equal(TimeSpan.TicksPerSecond, cluster.TimeProvider.TimestampFrequency);

        _ = cluster.RunFor(TimeSpan.FromMilliseconds(1250));

        Assert.Equal(start + TimeSpan.FromMilliseconds(1250), cluster.TimeProvider.GetUtcNow());
    }

    [Fact]
    public async Task OneShotTimerFiresAtDueTime()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UnixEpoch);
        var fireCount = 0;
        using var timer = cluster.TimeProvider.CreateTimer(
            _ => fireCount++,
            state: null,
            TimeSpan.FromSeconds(5),
            Timeout.InfiniteTimeSpan);

        _ = cluster.RunFor(TimeSpan.FromSeconds(4));
        Assert.Equal(0, fireCount);

        _ = cluster.RunFor(TimeSpan.FromSeconds(1));
        Assert.Equal(1, fireCount);
    }

    [Fact]
    public async Task PeriodicTimerReschedulesItself()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UnixEpoch);
        var fireCount = 0;
        using var timer = cluster.TimeProvider.CreateTimer(
            _ => fireCount++,
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2));

        _ = cluster.RunFor(TimeSpan.FromSeconds(5));

        Assert.Equal(3, fireCount);
    }

    [Fact]
    public async Task TimerChangeReplacesPendingSchedule()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UnixEpoch);
        var fireCount = 0;
        using var timer = cluster.TimeProvider.CreateTimer(
            _ => fireCount++,
            state: null,
            TimeSpan.FromSeconds(10),
            Timeout.InfiniteTimeSpan);

        Assert.True(timer.Change(TimeSpan.FromSeconds(2), Timeout.InfiniteTimeSpan));
        _ = cluster.RunFor(TimeSpan.FromSeconds(2));

        Assert.Equal(1, fireCount);
    }

    [Fact]
    public async Task DisposedTimerDoesNotFire()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UnixEpoch);
        var fired = false;
        var timer = cluster.TimeProvider.CreateTimer(
            _ => fired = true,
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        timer.Dispose();
        _ = cluster.RunFor(TimeSpan.FromSeconds(1));

        Assert.False(fired);
    }

    [Fact]
    public async Task NegativeAdvanceIsRejected()
    {
        await using var cluster = new SimulationCluster(seed: 1, DateTimeOffset.UnixEpoch);

        _ = Assert.Throws<ArgumentOutOfRangeException>(() => cluster.RunFor(TimeSpan.FromTicks(-1)));
    }
}
