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
        int entryThreadId = Environment.CurrentManagedThreadId;

        Guid firstGuid = Guid.NewGuid();
        Guid secondGuid = Guid.NewGuid();
        DateTime utcBefore = DateTime.UtcNow;
        DateTimeOffset offsetBefore = DateTimeOffset.UtcNow;
        long timestampBefore = Stopwatch.GetTimestamp();
        int tickCountBefore = Environment.TickCount;
        long tickCount64Before = Environment.TickCount64;

        var unseededRandom = new Random();
        int unseededRandomValue = unseededRandom.Next();
        int sharedRandomValue = Random.Shared.Next();

        int taskRunThreadId = 0;
        Task<int> taskRun = Task.Run(() =>
        {
            taskRunThreadId = Environment.CurrentManagedThreadId;
            lock (gate)
            {
                events.Add("task-run");
                lockCounter++;
            }

            return 17;
        });

        int threadPoolThreadId = 0;
        var threadPoolCompletion = new TaskCompletionSource<int>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        bool threadPoolQueued = ThreadPool.QueueUserWorkItem(_ =>
        {
            threadPoolThreadId = Environment.CurrentManagedThreadId;
            lock (gate)
            {
                events.Add("thread-pool");
                lockCounter++;
            }

            threadPoolCompletion.SetResult(23);
        });

        int configuredThreadId = 0;
        int configuredValue = await Task.Run(() => 31).ConfigureAwait(false);
        configuredThreadId = Environment.CurrentManagedThreadId;
        int taskRunValue = await taskRun.ConfigureAwait(false);
        int threadPoolValue = await threadPoolCompletion.Task.ConfigureAwait(false);

        lock (gate)
        {
            monitorEnteredInLock = Monitor.IsEntered(gate);
            events.Add("caller");
            lockCounter++;
        }

        DateTime delayUtcBefore = DateTime.UtcNow;
        DateTimeOffset delayOffsetBefore = DateTimeOffset.UtcNow;
        long delayTimestampBefore = Stopwatch.GetTimestamp();
        int delayTickCountBefore = Environment.TickCount;
        long delayTickCount64Before = Environment.TickCount64;

        await Task.Delay(delay).ConfigureAwait(false);

        DateTime delayUtcAfter = DateTime.UtcNow;
        DateTimeOffset delayOffsetAfter = DateTimeOffset.UtcNow;
        long delayTimestampAfter = Stopwatch.GetTimestamp();
        int delayTickCountAfter = Environment.TickCount;
        long delayTickCount64After = Environment.TickCount64;

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
