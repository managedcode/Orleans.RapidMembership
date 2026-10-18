using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using ManagedCode.Orleans.RapidMembership.Transport;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Transport;

internal sealed class PeerTransport : IDisposable
{
    internal const string IdentityHeader = "x-rapid-cluster";
    internal const string SecretHeader = "x-rapid-secret";
    private readonly RapidMembershipOptions _options;
    private readonly Serializer<PeerRequest> _requests;
    private readonly Serializer<PeerReply> _replies;
    private readonly Metadata _headers;
    private readonly GrpcChannel[] _channels;
    private readonly MembershipPeer.MembershipPeerClient[] _clients;

    public PeerTransport(RapidMembershipOptions options, string identity, Serializer<PeerRequest> requests, Serializer<PeerReply> replies)
    {
        _options = options;
        _requests = requests;
        _replies = replies;
        _headers = new Metadata { { IdentityHeader, identity }, { SecretHeader, options.AdmissionToken } };
        _channels = [.. options.SeedEndpoints.Select(endpoint => GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = checked(options.MaximumSnapshotBytes * 3),
            MaxSendMessageSize = checked(options.MaximumSnapshotBytes * 3),
        }))];
        _clients = [.. _channels.Select(channel => new MembershipPeer.MembershipPeerClient(channel))];
    }

    public int Quorum => _clients.Length / 2 + 1;

    public async Task<List<PeerReply>> ExchangeAsync(PeerRequest request, CancellationToken cancellationToken)
    {
        using var group = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var payload = new PeerPayload { Content = ByteString.CopyFrom(_requests.SerializeToArray(request)) };
        var pending = _clients.Select(client => SendAsync(client, payload, group.Token)).ToList();
        var replies = new List<PeerReply>();
        try
        {
            while (pending.Count > 0)
            {
                var completed = await Task.WhenAny(pending);
                _ = pending.Remove(completed);
                if (await completed is { } reply)
                {
                    replies.Add(reply);
                }

                if (replies.Count(reply => reply.Granted) >= Quorum)
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
        return replies;
    }

    private async Task<PeerReply?> SendAsync(MembershipPeer.MembershipPeerClient client, PeerPayload payload, CancellationToken cancellationToken)
    {
        try
        {
            using var call = client.ExchangeAsync(payload, _headers, DateTime.UtcNow + _options.PeerTimeout, cancellationToken);
            var response = await call.ResponseAsync;
            return _replies.Deserialize(response.Content.Span);
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

    public void Dispose()
    {
        foreach (var channel in _channels)
        {
            channel.Dispose();
        }
    }
}
