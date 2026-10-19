using System.Collections.Concurrent;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Transport;

internal sealed class PeerTransport(
    RapidMembershipOptions options, string identity, Serializer<PeerRequest> requests, Serializer<PeerReply> replies) : IDisposable
{
    internal const string IdentityHeader = "x-rapid-cluster";
    internal const string SecretHeader = "x-rapid-secret";
    private readonly Dictionary<CoordinationMember, GrpcChannel> _channels = [];
    private readonly System.Threading.Lock _channelGate = new();
    private readonly Metadata _headers = new() { { IdentityHeader, identity }, { SecretHeader, options.AdmissionToken } };

    public async Task<List<PeerReply>> ExchangeAsync(PeerRequest request, CoordinationConfiguration configuration, CancellationToken cancellationToken)
    {
        using var group = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pending = configuration.Members.Select(member => SendAsync(member, request, group.Token)).ToList();
        var result = new List<PeerReply>();
        try
        {
            while (pending.Count > 0)
            {
                var completed = await Task.WhenAny(pending);
                _ = pending.Remove(completed);
                if (await completed is { } reply)
                {
                    result.Add(reply);
                }

                if (result.Count(reply => reply.Granted) >= configuration.Quorum)
                {
                    break;
                }
            }
        }
        finally
        {
            await group.CancelAsync();
            _ = await Task.WhenAll(pending);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }

    public async Task<List<PeerReply>> LocateAsync(IEnumerable<CoordinationMember> members, CancellationToken cancellationToken)
    {
        var result = new ConcurrentBag<PeerReply>();
        await Parallel.ForEachAsync(members.Distinct().Take(options.MaximumPeers), new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = 16,
        }, async (member, token) =>
        {
            if (await SendAsync(member, new PeerRequest { Action = PeerAction.Locate }, token) is { Granted: true } reply)
            {
                result.Add(reply);
            }
        });
        return [.. result];
    }

    public async Task<PeerReply?> SendAsync(CoordinationMember member, PeerRequest request, CancellationToken cancellationToken)
    {
        var channel = GetChannel(member);
        var addressed = new PeerRequest
        {
            Action = request.Action,
            Index = request.Index,
            Ballot = request.Ballot,
            Snapshot = request.Snapshot,
            Configuration = request.Configuration,
            PreviousConfiguration = request.PreviousConfiguration,
            FrozenReplicas = request.FrozenReplicas,
            Recipient = member.Incarnation,
        };
        var bytes = requests.SerializeToArray(addressed);
        if (bytes.Length > options.MaximumExchangeBytes)
        {
            throw new InvalidDataException("The outgoing peer exchange exceeds its byte bound.");
        }

        try
        {
            using var call = new MembershipPeer.MembershipPeerClient(channel).ExchangeAsync(
                new PeerPayload { Content = ByteString.CopyFrom(bytes) }, _headers, DateTime.UtcNow + options.PeerTimeout, cancellationToken);
            var payload = await call.ResponseAsync;
            var reply = replies.Deserialize(payload.Content.Span);
            return reply?.Incarnation == member.Incarnation ? reply : null;
        }
        catch (RpcException)
        {
            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    private GrpcChannel GetChannel(CoordinationMember member)
    {
        lock (_channelGate)
        {
            if (_channels.TryGetValue(member, out var existing))
            {
                return existing;
            }

            if (_channels.Count >= options.MaximumPeers)
            {
                throw new InvalidOperationException("The peer channel bound was reached.");
            }

            var channel = GrpcChannel.ForAddress(member.Endpoint, new GrpcChannelOptions
            {
                MaxReceiveMessageSize = options.MaximumExchangeBytes,
                MaxSendMessageSize = options.MaximumExchangeBytes,
            });
            _channels.Add(member, channel);
            return channel;
        }
    }

    public void Dispose()
    {
        lock (_channelGate)
        {
            foreach (var channel in _channels.Values)
            {
                channel.Dispose();
            }
        }
    }
}
