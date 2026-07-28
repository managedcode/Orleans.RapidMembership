namespace RapidCluster;

/// <summary>
/// Holds all resources that are shared across a single instance of Rapid.
/// </summary>
public sealed class SharedResources
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SharedResources"/> class.
    /// </summary>
    /// <param name="timeProvider">Optional TimeProvider for time-related operations.</param>
    /// <param name="taskScheduler">Ignored compatibility parameter. Use <see cref="System.Threading.Tasks.TaskScheduler.Default"/> directly.</param>
    /// <param name="random">Ignored compatibility parameter. Use <see cref="System.Random.Shared"/> directly.</param>
    /// <param name="guidFactory">Ignored compatibility parameter. Use <see cref="System.Guid.NewGuid"/> directly.</param>
    /// <param name="shuttingDownToken">Cancellation token that signals when shutdown begins.</param>
    public SharedResources(
        TimeProvider? timeProvider = null,
        TaskScheduler? taskScheduler = null,
        Random? random = null,
        Func<Guid>? guidFactory = null,
        CancellationToken shuttingDownToken = default)
    {
        TimeProvider = timeProvider ?? TimeProvider.System;
        ShuttingDownToken = shuttingDownToken;
        _ = taskScheduler;
        _ = random;
        _ = guidFactory;
    }

    /// <summary>
    /// Gets the TimeProvider used for all time-related operations.
    /// </summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>
    /// Gets the default task scheduler.
    /// </summary>
    [Obsolete("Use TaskScheduler.Default directly. This compatibility member ignores the constructor's taskScheduler argument.")]
    public TaskScheduler TaskScheduler => global::System.Threading.Tasks.TaskScheduler.Default;

    /// <summary>
    /// Generates a random double using the process-wide shared random source.
    /// </summary>
    [Obsolete("Use Random.Shared.NextDouble() directly. This compatibility member ignores the constructor's random argument.")]
#pragma warning disable CA5394 // Protocol randomness is not security-sensitive.
    public double NextRandomDouble() => Random.Shared.NextDouble();
#pragma warning restore CA5394

    /// <summary>
    /// Generates a new GUID using the BCL GUID generator.
    /// </summary>
    [Obsolete("Use Guid.NewGuid() directly. This compatibility member ignores the constructor's guidFactory argument.")]
    public Guid NewGuid() => Guid.NewGuid();

    /// <summary>
    /// Gets a cancellation token that is cancelled when shutdown begins.
    /// </summary>
    public CancellationToken ShuttingDownToken { get; }

    /// <summary>
    /// Gets a value indicating whether gets whether shutdown has been initiated.
    /// </summary>
    public bool IsShuttingDown => ShuttingDownToken.IsCancellationRequested;
}
