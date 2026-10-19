using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Grpc;

/// <summary>
/// gRPC-based messaging client for Rapid.
/// Implements IHostedService to ensure proper shutdown ordering.
/// </summary>
internal sealed partial class GrpcClient(
    IOptions<RapidClusterGrpcOptions> grpcOptions,
    SharedResources sharedResources,
    RapidClusterMetrics metrics,
    ILogger<GrpcClient> logger) : IMessagingClient, IHostedService
{
    private readonly RapidClusterGrpcOptions _grpcOptions = grpcOptions.Value;
    private readonly SharedResources _sharedResources = sharedResources;
    private readonly RapidClusterMetrics _metrics = metrics;
#pragma warning disable CA1823 // Avoid unused private fields
    private readonly ILogger<GrpcClient> _logger = logger;
#pragma warning restore CA1823 // Avoid unused private fields
    private readonly Lock _clientGate = new();
    private readonly Dictionary<string, (GrpcChannel Channel, Pb.MembershipService.MembershipServiceClient Client)> _clients = [with(StringComparer.Ordinal)];
    private readonly ConcurrentDictionary<int, Task> _pendingTasks = new();
    private readonly CancellationTokenSource _stoppingCts = new();
    private int _taskIdCounter;
    private bool _disposed;

    private const string SendRequestMethod = "SendRequest";

    /// <summary>
    /// Wrapper for logging a single Endpoint address.
    /// </summary>
    private readonly struct LoggableEndpoint(Endpoint endpoint)
    {
        private readonly string _display = endpoint.GetNetworkAddressString();

        public override readonly string ToString() => _display;
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "RPC failed to {Remote}")]
    private partial void LogRpcFailed(Exception ex, LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RPC unavailable to {Remote} (node may be shutting down)")]
    private partial void LogRpcUnavailable(LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GrpcClient stopping, waiting for {Count} pending tasks")]
    private partial void LogStopping(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: RPC error {StatusCode} - {Error}")]
    private partial void LogOneWayDeliveryFailedRpc(LoggableEndpoint Remote, StatusCode StatusCode, string Error);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: Timeout after {Timeout}")]
    private partial void LogOneWayDeliveryFailedTimeout(LoggableEndpoint Remote, TimeSpan Timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}")]
    private partial void LogOneWayDeliveryFailedUnexpected(Exception exception, LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Trace, Message = "One-way message delivered successfully to {Remote}")]
    private partial void LogOneWayDeliverySucceeded(LoggableEndpoint Remote);

    private global::Grpc.Core.Metadata CreateHeaders()
    {
        var headers = new global::Grpc.Core.Metadata();
        foreach (var (key, value) in _grpcOptions.RequestHeaders)
        {
            headers.Add(key, value);
        }

        return headers;
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_clientGate)
        {
            if (_disposed)
            {
                return;
            }
        }

        // Cancel all pending operations immediately
        await _stoppingCts.CancelAsync();

        // Wait for all pending tasks to complete (they should finish quickly now)
        var pendingTasks = _pendingTasks.Values.ToArray();
        if (pendingTasks.Length > 0)
        {
            LogStopping(pendingTasks.Length);
            try
            {
                await Task.WhenAll(pendingTasks).WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Timeout waiting for pending tasks
            }
        }
    }

    public async Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request,
        CancellationToken cancellationToken)
    {
        var client = GetOrCreateClient(remote);
        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordGrpcCallStarted(SendRequestMethod);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stoppingCts.Token);
            using var call = client.SendRequestAsync(request, headers: CreateHeaders(), cancellationToken: cts.Token);
            var response = await call.ResponseAsync
                .WaitAsync(request.JoinMessage is not null ? _grpcOptions.JoinTimeout : _grpcOptions.Timeout, _sharedResources.TimeProvider, cts.Token);
            stopwatch.Stop();
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "OK");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "OK", stopwatch);
            return response;
        }
        catch (RpcException ex)
        {
            stopwatch.Stop();

            // Log at Debug level for expected shutdown-related errors, Error level for others
            if (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
            {
                LogRpcUnavailable(new LoggableEndpoint(remote));
            }
            else
            {
                LogRpcFailed(ex, new LoggableEndpoint(remote));
            }

            _metrics.RecordGrpcCallCompleted(SendRequestMethod, ex.StatusCode.ToString());
            _metrics.RecordGrpcCallDuration(SendRequestMethod, ex.StatusCode.ToString(), stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Network);
            throw;
        }
    }

    public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        var taskId = Interlocked.Increment(ref _taskIdCounter);
        var task = SendOneWayMessageInternalAsync(remote, request, rank, taskId, onDeliveryFailure, cancellationToken);
        _ = _pendingTasks.TryAdd(taskId, task);
        if (task.IsCompleted)
        {
            _ = _pendingTasks.TryRemove(taskId, out _);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "One-way messages ignore failures but may invoke callback")]
    private async Task SendOneWayMessageInternalAsync(Endpoint remote, RapidClusterRequest request, Rank? rank, int taskId, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stoppingCts.Token);

        var client = GetOrCreateClient(remote);
        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordGrpcCallStarted(SendRequestMethod);

        try
        {
            using var call = client.SendRequestAsync(request, headers: CreateHeaders(), cancellationToken: cts.Token);
            _ = await call.ResponseAsync
                .WaitAsync(request.JoinMessage is not null ? _grpcOptions.JoinTimeout : _grpcOptions.Timeout, _sharedResources.TimeProvider, cts.Token);
            stopwatch.Stop();
            LogOneWayDeliverySucceeded(new LoggableEndpoint(remote));
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "OK");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "OK", stopwatch);
        }
        catch (RpcException ex)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedRpc(new LoggableEndpoint(remote), ex.StatusCode, ex.Message);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, ex.StatusCode.ToString());
            _metrics.RecordGrpcCallDuration(SendRequestMethod, ex.StatusCode.ToString(), stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Network);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        catch (TimeoutException)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedTimeout(new LoggableEndpoint(remote), _grpcOptions.Timeout);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "DeadlineExceeded");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "DeadlineExceeded", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Timeout);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();

            // User cancellation - don't invoke callback but still record metrics
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "Cancelled");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "Cancelled", stopwatch);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedUnexpected(ex, new LoggableEndpoint(remote));
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "Unknown");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "Unknown", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Unknown);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        finally
        {
            _ = _pendingTasks.TryRemove(taskId, out _);
        }
    }

    private Pb.MembershipService.MembershipServiceClient GetOrCreateClient(Endpoint remote)
    {
        var key = string.Create(CultureInfo.InvariantCulture, $"{remote.Hostname.ToStringUtf8()}:{remote.Port}");
        lock (_clientGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_clients.TryGetValue(key, out var existing))
            {
                return existing.Client;
            }

            var scheme = _grpcOptions.UseHttps ? "https" : "http";
            var handler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
            };
            handler.SslOptions.RemoteCertificateValidationCallback = _grpcOptions.RemoteCertificateValidationCallback;
            var destination = new Uri($"{scheme}://{key}");
            if (_grpcOptions.UseLoopbackForLocalListeners)
            {
                destination = LocalPeerAddress.ForLocalListener(destination);
            }

            var channel = GrpcChannel.ForAddress(destination, new GrpcChannelOptions
            {
                HttpHandler = handler,
                DisposeHttpClient = true,
                HttpVersion = new Version(2, 0),
                HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact,
                ThrowOperationCanceledOnCancellation = true,
            });
            var client = new Pb.MembershipService.MembershipServiceClient(channel);
            _clients.Add(key, (channel, client));
            return client;
        }
    }

    public async ValueTask DisposeAsync()
    {
        GrpcChannel[] channels;
        lock (_clientGate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            channels = [.. _clients.Values.Select(connection => connection.Channel)];
            _clients.Clear();
        }

        try
        {
            await _stoppingCts.CancelAsync();
            var pendingTasks = _pendingTasks.Values.ToArray();
            if (pendingTasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(pendingTasks).WaitAsync(TimeSpan.FromSeconds(5), _sharedResources.TimeProvider);
                }
                catch (TimeoutException)
                {
                    // Channel disposal below terminates any remaining native RPCs.
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        finally
        {
            foreach (var channel in channels)
            {
                channel.Dispose();
            }

            _stoppingCts.Dispose();
        }
    }
}
