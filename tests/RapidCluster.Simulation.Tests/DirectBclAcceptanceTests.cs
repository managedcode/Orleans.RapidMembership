using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Fixtures;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Acceptance coverage for ordinary BCL call sites in the separately instrumented fixture assembly.
/// The xUnit/MTP runner and this test assembly intentionally remain uninstrumented.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class DirectBclAcceptanceTests
{
    private static readonly DateTimeOffset VirtualTimeOrigin =
        new(2040, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private static readonly TimeSpan VirtualDelay = TimeSpan.FromMilliseconds(125);

    [Fact]
    public void SameSeed_ReplaysIdentityRandomWorkOrderAndVirtualTimeExactly()
    {
        DirectBclSnapshot first = Capture(seed: 73);
        DirectBclSnapshot replay = Capture(seed: 73);

        Assert.Equal(first, replay);
        AssertSnapshotContract(first);
    }

    [Fact]
    public void DifferentSeeds_VaryIdentityAndRandomWhilePreservingVirtualTimeContract()
    {
        DirectBclSnapshot first = Capture(seed: 73);
        DirectBclSnapshot second = Capture(seed: 74);

        Assert.NotEqual(first.FirstGuid, second.FirstGuid);
        Assert.NotEqual(first.SecondGuid, second.SecondGuid);
        Assert.NotEqual(first.UnseededRandomValue, second.UnseededRandomValue);
        Assert.NotEqual(first.SharedRandomValue, second.SharedRandomValue);
        Assert.Equal(first.DelayUtcAfter - first.DelayUtcBefore, second.DelayUtcAfter - second.DelayUtcBefore);
        Assert.Equal(first.EventOrder, Capture(seed: 73).EventOrder);
        AssertSnapshotContract(first);
        AssertSnapshotContract(second);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(17)]
    [InlineData(101)]
    [InlineData(12345)]
    [InlineData(2147483646)]
    public void RepeatedSeedStress_EachScenarioReplaysTwice(int seed)
    {
        DirectBclSnapshot first = Capture(seed);
        DirectBclSnapshot replayOne = Capture(seed);
        DirectBclSnapshot replayTwo = Capture(seed);

        Assert.Equal(first, replayOne);
        Assert.Equal(first, replayTwo);
        AssertSnapshotContract(first);
    }

    private static DirectBclSnapshot Capture(int seed)
    {
        var harness = new RapidSimulationCluster(seed, VirtualTimeOrigin);
        try
        {
            Task<DirectBclSnapshot>? capture = null;

            _ = harness.RunToCompletion(
                () => capture = DirectBclAcceptanceFixture.CaptureAsync(VirtualDelay));

            Assert.NotNull(capture);
            return capture.GetAwaiter().GetResult();
        }
        finally
        {
            harness.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private static void AssertSnapshotContract(DirectBclSnapshot snapshot)
    {
        Assert.NotEqual(Guid.Empty, snapshot.FirstGuid);
        Assert.NotEqual(snapshot.FirstGuid, snapshot.SecondGuid);
        Assert.Equal(VirtualTimeOrigin.UtcDateTime, snapshot.UtcBefore);
        Assert.Equal(VirtualTimeOrigin, snapshot.OffsetBefore);
        Assert.Equal(snapshot.UtcBefore.Ticks, snapshot.OffsetBefore.UtcTicks);
        Assert.Equal(0, snapshot.TimestampBefore);
        Assert.Equal(0, snapshot.TickCountBefore);
        Assert.Equal(0L, snapshot.TickCount64Before);

        Assert.Equal(17, snapshot.TaskRunValue);
        Assert.True(snapshot.ThreadPoolQueued);
        Assert.Equal(23, snapshot.ThreadPoolValue);
        Assert.Equal(31, snapshot.ConfiguredAwaitValue);
        Assert.Equal(3, snapshot.LockCounter);
        Assert.True(snapshot.MonitorEnteredInLock);
        Assert.Equal(3, snapshot.EventOrder.Split(',').Length);
        Assert.Contains("task-run", snapshot.EventOrder, StringComparison.Ordinal);
        Assert.Contains("thread-pool", snapshot.EventOrder, StringComparison.Ordinal);
        Assert.Contains("caller", snapshot.EventOrder, StringComparison.Ordinal);

        Assert.Equal(snapshot.EntryThreadId, snapshot.TaskRunThreadId);
        Assert.Equal(snapshot.EntryThreadId, snapshot.ThreadPoolThreadId);
        Assert.Equal(snapshot.EntryThreadId, snapshot.ConfiguredAwaitThreadId);
        Assert.Equal(snapshot.EntryThreadId, snapshot.ExitThreadId);

        Assert.Equal(VirtualDelay, snapshot.RequestedDelay);
        Assert.Equal(VirtualDelay, snapshot.DelayUtcAfter - snapshot.DelayUtcBefore);
        Assert.Equal(VirtualDelay, snapshot.DelayOffsetAfter - snapshot.DelayOffsetBefore);
        Assert.Equal(VirtualDelay, Stopwatch.GetElapsedTime(
            snapshot.DelayTimestampBefore,
            snapshot.DelayTimestampAfter));
        Assert.Equal((int)VirtualDelay.TotalMilliseconds, unchecked(snapshot.DelayTickCountAfter - snapshot.DelayTickCountBefore));
        Assert.Equal((long)VirtualDelay.TotalMilliseconds, snapshot.DelayTickCount64After - snapshot.DelayTickCount64Before);
    }
}
