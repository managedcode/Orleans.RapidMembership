using Google.Protobuf;
using Grpc.Core;
using ManagedCode.Orleans.RapidMembership.Transport;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Transport;

internal sealed class MembershipPeerService(MembershipAcceptor acceptor, Serializer<PeerRequest> requests, Serializer<PeerReply> replies)
    : MembershipPeer.MembershipPeerBase
{
    public override async Task<PeerPayload> Exchange(PeerPayload request, ServerCallContext context)
    {
        context.CancellationToken.ThrowIfCancellationRequested();
        var reply = await acceptor.ExchangeAsync(requests.Deserialize(request.Content.Span) ?? throw new InvalidDataException("Empty peer request."), replies, context.CancellationToken);
        return new PeerPayload { Content = ByteString.CopyFrom(reply) };
    }
}
