using Grpc.Core;
using Grpc.Core.Interceptors;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class QuorumReadGate(Serializer<PeerRequest> requests) : Interceptor
{
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _inspectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Guid _blocked;
    private int _armed;
    private int _blockedCalls;

    public int BlockedCalls => Volatile.Read(ref _blockedCalls);

    public void Arm(Guid incarnation)
    {
        _blocked = incarnation;
        Volatile.Write(ref _armed, 1);
    }

    public void Release()
    {
        Volatile.Write(ref _armed, 0);
        _ = _release.TrySetResult();
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var decoded = request is PeerPayload payload ? requests.Deserialize(payload.Content.Span) : null;
        TaskCompletionSource? entered = null;
        if (decoded?.Action == PeerAction.Inspect)
        {
            entered = _inspectEntered;
        }
        else if (decoded?.Action == PeerAction.Commit)
        {
            entered = _commitEntered;
        }

        if (Volatile.Read(ref _armed) != 0 && entered is not null)
        {
            if (decoded!.Recipient == _blocked)
            {
                _ = Interlocked.Increment(ref _blockedCalls);
                _ = entered.TrySetResult();
                await _release.Task.WaitAsync(context.CancellationToken);
            }
            else
            {
                await entered.Task.WaitAsync(context.CancellationToken);
            }
        }

        return await continuation(request, context);
    }
}
