using Clockwork;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// <para>
/// In-memory messaging client for simulation testing.
/// Routes messages through the SimulationNetwork instead of gRPC.
/// </para>
/// <para>
/// Messages are delivered by scheduling work on the target node's SimulationTaskQueue.
/// This provides deterministic execution where messages are processed when the simulation steps.
/// </para>
/// </summary>
internal sealed class InMemoryMessagingClient(
    RapidSimulationCluster harness,
    RapidSimulationNode sourceNode,
    Endpoint localEndpoint,
    RapidClusterProtocolOptions protocolOptions,
    PingPongFailureDetectorOptions failureDetectorOptions) : IMessagingClient, IDisposable
{
    private readonly RapidClusterProtocolOptions _protocolOptions = protocolOptions;
    private readonly PingPongFailureDetectorOptions _failureDetectorOptions = failureDetectorOptions;
    private readonly CancellationTokenSource _disposeCts = new();
    private bool _disposed;

    /// <summary>
    /// Default timeout for general RPC operations in the simulation.
    /// </summary>
    private static readonly TimeSpan DefaultRpcTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Default timeout for join operations in the simulation.
    /// </summary>
    private static readonly TimeSpan JoinTimeout = TimeSpan.FromSeconds(5);

    internal TimeSpan ProbeTimeout => _failureDetectorOptions.ProbeTimeout;

    private TimeSpan GetTimeout(RapidClusterRequest request)
    {
#pragma warning disable IDE0072 // Protobuf enums can contain unnamed numeric values.
        return request.ContentCase switch
        {
            RapidClusterRequest.ContentOneofCase.ProbeMessage => _failureDetectorOptions.ProbeTimeout,
            RapidClusterRequest.ContentOneofCase.PreJoinMessage => JoinTimeout,
            RapidClusterRequest.ContentOneofCase.JoinMessage => JoinTimeout,
            RapidClusterRequest.ContentOneofCase.LeaveMessage => _protocolOptions.LeaveMessageTimeout,
            _ => DefaultRpcTimeout,
        };
#pragma warning restore IDE0072
    }

    public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        var sendTask = StartSend(remote, request, cancellationToken);
        sendTask.Ignore();

        if (onDeliveryFailure is not null)
        {
            sendTask.ContinueWith(
                static (t, state) =>
                {
                    var (callback, endpoint, rank) = ((DeliveryFailureCallback, Endpoint, Rank))state!;
                    if (t.IsCanceled)
                    {
                        return;
                    }

                    if (t.IsFaulted)
                    {
                        _ = t.Exception;
                        callback(endpoint, rank);
                    }
                    else if (!t.Result.Delivered)
                    {
                        callback(endpoint, rank);
                    }
                },
                (onDeliveryFailure, remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided.")),
                CancellationToken.None,
                TaskContinuationOptions.None,
                sourceNode.Context.TaskScheduler).Ignore();
        }
    }

    public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(remote);
        ArgumentNullException.ThrowIfNull(request);
        ObjectDisposedException.ThrowIf(_disposed, this);

        return StartSend(remote, request, cancellationToken).ContinueWith(
            static (task, state) =>
            {
                var result = task.GetAwaiter().GetResult();
                return result.Delivered
                    ? result.Response!
                    : CreateFailureResponse((RapidClusterRequest)state!);
            },
            request,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            sourceNode.Context.TaskScheduler);
    }

    private Task<MessageDeliveryResult> StartSend(
        Endpoint remote,
        RapidClusterRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(remote);
        ArgumentNullException.ThrowIfNull(request);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var localAddr = RapidClusterUtils.Loggable(localEndpoint);
        var remoteAddr = RapidClusterUtils.Loggable(remote);
        var targetNode = harness.FindNode(remoteAddr);
        var messageTimeout = GetTimeout(request);
        if (targetNode is null ||
            harness.Network.CheckDelivery(localAddr, remoteAddr) is not DeliveryStatus.Success)
        {
            return CreateFailureTask(messageTimeout, cancellationToken);
        }

        return Task.Factory.StartNew(
            () => SendMessageCoreAsync(targetNode, request, localAddr, remoteAddr, cancellationToken),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            sourceNode.Context.TaskScheduler).Unwrap();
    }

    private async Task<MessageDeliveryResult> SendMessageCoreAsync(
        RapidSimulationNode targetNode,
        RapidClusterRequest request,
        string localAddr,
        string remoteAddr,
        CancellationToken cancellationToken)
    {
        using var sourceCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        var sourceContext = sourceNode.Context;
        var messageTimeout = GetTimeout(request);
        var targetContext = targetNode.Context;
        var network = harness.Network;
        var delay = network.GetMessageDelay();

        // Schedule delivery on the target node.
        var targetCancellation = targetNode.TeardownCancellationToken;
        var deliveryTask = Task.Factory.StartNew(
            DeliverMessageAsync,
            targetCancellation,
            TaskCreationOptions.None,
            targetContext.TaskScheduler)
            .Unwrap();

        // Ignore the task in case the source is terminated before it resolves.
        deliveryTask.Ignore();

        try
        {
            return await deliveryTask.WaitAsync(messageTimeout, sourceContext.TimeProvider, sourceCts.Token);
        }
        catch (TimeoutException)
        {
            return MessageDeliveryResult.Failed;
        }

        async Task<MessageDeliveryResult> DeliverMessageAsync()
        {
            try
            {
                await Task.Delay(delay, targetContext.TimeProvider, targetCancellation);
                return network.CheckDelivery(localAddr, remoteAddr) switch
                {
                    DeliveryStatus.Dropped => MessageDeliveryResult.Failed,
                    DeliveryStatus.Partitioned => MessageDeliveryResult.Failed,
                    DeliveryStatus.Success => MessageDeliveryResult.Succeeded(await targetNode.HandleRequestAsync(request, targetCancellation)),
                    _ => throw new InvalidOperationException("Unrecognized delivery status."),
                };
            }
            catch (OperationCanceledException) when (targetCancellation.IsCancellationRequested)
            {
                return MessageDeliveryResult.Failed;
            }
        }
    }

    private Task<MessageDeliveryResult> CreateFailureTask(TimeSpan timeout, CancellationToken cancellationToken)
    {
#pragma warning disable CA2000 // The completion continuation disposes the linked source.
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
#pragma warning restore CA2000
        var resultTask = Task.Delay(timeout, sourceNode.Context.TimeProvider, linkedCts.Token).ContinueWith(
            static task =>
            {
                task.GetAwaiter().GetResult();
                return MessageDeliveryResult.Failed;
            },
            linkedCts.Token,
            TaskContinuationOptions.ExecuteSynchronously,
            sourceNode.Context.TaskScheduler);
        resultTask.ContinueWith(
            static (_, state) => ((CancellationTokenSource)state!).Dispose(),
            linkedCts,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            sourceNode.Context.TaskScheduler).Ignore();
        return resultTask;
    }

    private static RapidClusterResponse CreateFailureResponse(RapidClusterRequest request)
    {
#pragma warning disable IDE0072 // Protobuf enums can contain unnamed numeric values.
        return request.ContentCase switch
        {
            RapidClusterRequest.ContentOneofCase.PreJoinMessage or
            RapidClusterRequest.ContentOneofCase.JoinMessage => new RapidClusterResponse
            {
                JoinResponse = new JoinResponse { StatusCode = JoinStatusCode.MembershipRejected },
            },
            _ => new RapidClusterResponse(),
        };
#pragma warning restore IDE0072
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _disposeCts.Cancel();
        _disposeCts.Dispose();
    }

    private readonly record struct MessageDeliveryResult(bool Delivered, RapidClusterResponse? Response)
    {
        public static MessageDeliveryResult Failed => default;

        public static MessageDeliveryResult Succeeded(RapidClusterResponse response) => new(true, response);
    }
}
