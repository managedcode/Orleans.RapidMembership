using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Configuration;

namespace ManagedCode.Orleans.RapidMembership.Tests.Cases;

public sealed class NativeRejoinTests
{
    [Fact]
    public async Task RemovedLiveCoordinatorRejoinsWithItsConfiguredMetadataAndRetainedRows()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(2));
        await using var cluster = new NetworkCluster();
        await cluster.StartAsync(timeout.Token);
        var initial = cluster.VoterViews.First();
        var member = cluster.Configuration.Members[0];
        var endpoint = new Uri(member.Endpoint);
        var proposer = cluster.Proposer();
        Assert.True((await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(1), null, table.Version.Next()), timeout.Token)).Applied);
        using var channel = GrpcChannel.ForAddress(cluster.Configuration.Members[1].Endpoint);
        var options = new RapidMembershipOptions { SharedSecret = "0123456789abcdef0123456789abcdef" };
        var identity = RapidMembershipOptions.Identity(new ClusterOptions { ClusterId = cluster.ClusterId, ServiceId = "network-provider-tests" });
        using var call = new global::RapidCluster.Pb.MembershipService.MembershipServiceClient(channel).SendRequestAsync(
            new global::RapidCluster.Pb.RapidClusterRequest
            {
                LeaveMessage = new global::RapidCluster.Pb.LeaveMessage
                {
                    Sender = new global::RapidCluster.Pb.Endpoint { Hostname = ByteString.CopyFromUtf8(endpoint.Host), Port = endpoint.Port },
                },
            }, new Metadata { { PeerTransport.IdentityHeader, identity }, { PeerTransport.SecretHeader, options.AdmissionToken } },
            DateTime.UtcNow.AddSeconds(5), timeout.Token);
        _ = await call.ResponseAsync;
        while (true)
        {
            var views = cluster.VoterViews.ToArray();
            if (views.All(view => view.Members.Length == 3 && view.ConfigurationId.Version >= initial.ConfigurationId.Version + 2)
                && views.All(view => view.ConfigurationId == views[0].ConfigurationId))
            {
                foreach (var view in views)
                {
                    var returned = view.Members.Single(value => value.EndPoint.ToString() == endpoint.Authority);
                    Assert.Equal(member.Incarnation, new Guid(returned.Metadata["membership-incarnation"].Span));
                }

                break;
            }

            await Task.Delay(20, timeout.Token);
        }

        var recovered = await proposer.ExecuteAsync(null, timeout.Token);
        _ = Assert.Single(recovered.Table.Members);
        Assert.True((await proposer.ExecuteAsync(table => MembershipMutations.Write(table,
            MutationTests.Entry(2), null, table.Version.Next()), timeout.Token)).Applied);
        Assert.Equal(2, (await proposer.ExecuteAsync(null, timeout.Token)).Table.Members.Count);
    }
}
