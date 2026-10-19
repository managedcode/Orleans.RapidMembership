using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class ProposerStageGate(PeerAction action, Serializer<PeerRequest> requests, Serializer<PeerReply> replies) : Interceptor
{
    private readonly Lock _gate = new();
    private readonly HashSet<Guid> _held = [];
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _armed;
    private int _rejections;

    public int Rejections => Volatile.Read(ref _rejections);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Arm() => Volatile.Write(ref _armed, 1);

    public void Release()
    {
        Volatile.Write(ref _armed, 0);
        _ = _release.TrySetResult();
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var decoded = request is PeerPayload payload ? requests.Deserialize(payload.Content.Span) : null;
        if (action != PeerAction.Inspect)
        {
            await HoldAsync(decoded, context.CancellationToken);
        }

        var response = await continuation(request, context);
        var preempted = action == PeerAction.Inspect ? PeerAction.Prepare : PeerAction.Accept;
        if (decoded?.Action == preempted && response is PeerPayload payloadResponse
            && replies.Deserialize(payloadResponse.Content.Span) is { Granted: false })
        {
            _ = Interlocked.Increment(ref _rejections);
        }

        if (action == PeerAction.Inspect)
        {
            await HoldAsync(decoded, context.CancellationToken);
        }

        return response;
    }

    private async Task HoldAsync(PeerRequest? request, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _armed) == 0 || request?.Action != action)
        {
            return;
        }

        lock (_gate)
        {
            if (!_held.Add(request.Recipient))
            {
                return;
            }

            if (_held.Count == 3)
            {
                _ = Entered.TrySetResult();
            }
        }

        await _release.Task.WaitAsync(cancellationToken);
    }
}
