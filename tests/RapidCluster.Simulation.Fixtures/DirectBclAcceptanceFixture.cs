using System.Diagnostics;

namespace RapidCluster.Simulation.Fixtures;

/// <summary>
/// Ordinary BCL call sites which are rewritten in the staged simulation application closure.
/// This assembly intentionally has no test-framework or Clockwork dependency.
/// </summary>
public static class DirectBclAcceptanceFixture
{
    /// <summary>Captures deterministic identity, time, randomness, and controlled-work observations.</summary>
    public static async Task<DirectBclSnapshot> CaptureAsync(TimeSpan delay)
    {
        var events = new List<string>();
        var gate = new object();
        var lockCounter = 0;
        var monitorEnteredInLock = false;
        var entryThreadId = Environment.CurrentManagedThreadId;

        var firstGuid = Guid.NewGuid();
        var secondGuid = Guid.NewGuid();
        var utcBefore = DateTime.UtcNow;
        var offsetBefore = DateTimeOffset.UtcNow;
        var timestampBefore = Stopwatch.GetTimestamp();
        var tickCountBefore = Environment.TickCount;
        var tickCount64Before = Environment.TickCount64;

#pragma warning disable CA5394 // These call sites verify deterministic Random instrumentation.
        var unseededRandom = new Random();
        var unseededRandomValue = unseededRandom.Next();
        var sharedRandomValue = Random.Shared.Next();
#pragma warning restore CA5394

        var taskRunThreadId = 0;
        var taskRun = Task.Run(() =>
        {
            taskRunThreadId = Environment.CurrentManagedThreadId;
            lock (gate)
            {
                events.Add("task-run");
                lockCounter++;
            }

            return 17;
        });

        var threadPoolThreadId = 0;
        var threadPoolCompletion = new TaskCompletionSource<int>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var threadPoolQueued = ThreadPool.QueueUserWorkItem(_ =>
        {
            threadPoolThreadId = Environment.CurrentManagedThreadId;
            lock (gate)
            {
                events.Add("thread-pool");
                lockCounter++;
            }

            threadPoolCompletion.SetResult(23);
        });

        var configuredThreadId = 0;
        var configuredValue = await Task.Run(() => 31).ConfigureAwait(false);
        configuredThreadId = Environment.CurrentManagedThreadId;
        var taskRunValue = await taskRun.ConfigureAwait(false);
        var threadPoolValue = await threadPoolCompletion.Task.ConfigureAwait(false);

        lock (gate)
        {
            monitorEnteredInLock = Monitor.IsEntered(gate);
            events.Add("caller");
            lockCounter++;
        }

        var delayUtcBefore = DateTime.UtcNow;
        var delayOffsetBefore = DateTimeOffset.UtcNow;
        var delayTimestampBefore = Stopwatch.GetTimestamp();
        var delayTickCountBefore = Environment.TickCount;
        var delayTickCount64Before = Environment.TickCount64;

        await Task.Delay(delay).ConfigureAwait(false);

        var delayUtcAfter = DateTime.UtcNow;
        var delayOffsetAfter = DateTimeOffset.UtcNow;
        var delayTimestampAfter = Stopwatch.GetTimestamp();
        var delayTickCountAfter = Environment.TickCount;
        var delayTickCount64After = Environment.TickCount64;

        return new DirectBclSnapshot(
            firstGuid,
            secondGuid,
            utcBefore,
            offsetBefore,
            timestampBefore,
            tickCountBefore,
            tickCount64Before,
            unseededRandomValue,
            sharedRandomValue,
            taskRunValue,
            threadPoolQueued,
            threadPoolValue,
            configuredValue,
            lockCounter,
            monitorEnteredInLock,
            string.Join(",", events),
            entryThreadId,
            taskRunThreadId,
            threadPoolThreadId,
            configuredThreadId,
            Environment.CurrentManagedThreadId,
            delay,
            delayUtcBefore,
            delayUtcAfter,
            delayOffsetBefore,
            delayOffsetAfter,
            delayTimestampBefore,
            delayTimestampAfter,
            delayTickCountBefore,
            delayTickCountAfter,
            delayTickCount64Before,
            delayTickCount64After);
    }
}

/// <summary>Structured observations returned from the rewritten direct-BCL fixture.</summary>
public sealed record DirectBclSnapshot(
    Guid FirstGuid,
    Guid SecondGuid,
    DateTime UtcBefore,
    DateTimeOffset OffsetBefore,
    long TimestampBefore,
    int TickCountBefore,
    long TickCount64Before,
    int UnseededRandomValue,
    int SharedRandomValue,
    int TaskRunValue,
    bool ThreadPoolQueued,
    int ThreadPoolValue,
    int ConfiguredAwaitValue,
    int LockCounter,
    bool MonitorEnteredInLock,
    string EventOrder,
    int EntryThreadId,
    int TaskRunThreadId,
    int ThreadPoolThreadId,
    int ConfiguredAwaitThreadId,
    int ExitThreadId,
    TimeSpan RequestedDelay,
    DateTime DelayUtcBefore,
    DateTime DelayUtcAfter,
    DateTimeOffset DelayOffsetBefore,
    DateTimeOffset DelayOffsetAfter,
    long DelayTimestampBefore,
    long DelayTimestampAfter,
    int DelayTickCountBefore,
    int DelayTickCountAfter,
    long DelayTickCount64Before,
    long DelayTickCount64After);
