using Grpc.Core;
using Grpc.Core.Interceptors;
using RapidCluster.Pb;

namespace RapidCluster.Integration.Tests;

internal sealed class LeaveMessageDeliveryGate : Interceptor
{
    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _holdNext;

    public Task Entered => _entered.Task;

    public void HoldNext() => Volatile.Write(ref _holdNext, 1);

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        if (request is RapidClusterRequest { LeaveMessage: not null } &&
            Interlocked.Exchange(ref _holdNext, 0) != 0)
        {
            _ = _entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
        }

        return await continuation(request, context);
    }
}
