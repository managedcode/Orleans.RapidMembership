using Grpc.Core;
using Grpc.Core.Interceptors;
using RapidCluster.Pb;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class RpcCancellationObservation : Interceptor
{
    public TaskCompletionSource<bool> Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        finally
        {
            if (request is RapidClusterRequest { JoinMessage: not null })
            {
                _ = Finished.TrySetResult(context.CancellationToken.IsCancellationRequested);
            }
        }
    }
}
