namespace RapidCluster;

/// <summary>
/// Holds all resources that are shared across a single instance of Rapid.
/// </summary>
/// <remarks>
/// Creates a new SharedResources instance.
/// </remarks>
/// <param name="timeProvider">Optional TimeProvider for time-related operations.</param>
/// <param name="shuttingDownToken">Cancellation token that signals when shutdown begins.</param>
public sealed class SharedResources(
    TimeProvider? timeProvider = null,
    CancellationToken shuttingDownToken = default)
{
    /// <summary>
    /// Gets the TimeProvider used for all time-related operations.
    /// </summary>
    public TimeProvider TimeProvider { get; } = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Gets a cancellation token that is cancelled when shutdown begins.
    /// </summary>
    public CancellationToken ShuttingDownToken { get; } = shuttingDownToken;

    /// <summary>
    /// Gets a value indicating whether gets whether shutdown has been initiated.
    /// </summary>
    public bool IsShuttingDown => ShuttingDownToken.IsCancellationRequested;
}
