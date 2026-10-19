using Google.Protobuf;
using Grpc.Core;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Transport;

internal sealed class MembershipPeerService(
    MemoryMembershipReplica replica, GroupKnowledge knowledge, HostParticipation participation,
    Serializer<PeerRequest> requests, Serializer<PeerReply> replies) : MembershipPeer.MembershipPeerBase
{
    public override async Task<PeerPayload> Exchange(PeerPayload request, ServerCallContext context)
    {
        var decoded = requests.Deserialize(request.Content.Span) ?? throw new InvalidDataException("Empty peer request.");
        var reply = await replica.ExchangeAsync(decoded, context.CancellationToken);
        reply.KnownConfiguration = knowledge.Configuration;
        reply.IsJoining = participation.IsJoining;
        reply.CanCoordinate = participation.CanCoordinate;
        reply.BootstrapCandidate = participation.BootstrapCandidate ?? string.Empty;
        reply.IsDeparting = participation.IsDeparting;
        return new PeerPayload { Content = ByteString.CopyFrom(replies.SerializeToArray(reply)) };
    }
}
