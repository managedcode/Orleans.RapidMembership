using System.Net;
using Orleans.Messaging;
using Orleans.Runtime;

namespace ManagedCode.Orleans.RapidMembership.Operations;

internal sealed class RapidGatewayListProvider(MembershipProposer proposer) : IGatewayListProvider
{
    public TimeSpan MaxStaleness => TimeSpan.FromSeconds(5);

    public bool IsUpdatable => true;

    public Task InitializeGatewayListProvider() => Task.CompletedTask;

    public async Task<IList<Uri>> GetGateways()
    {
        var snapshot = await proposer.ExecuteAsync(null, CancellationToken.None);
        return [.. snapshot.Table.Members.Where(row => row.Item1.Status == SiloStatus.Active && row.Item1.ProxyPort > 0)
            .Select(row => SiloAddress.New(new IPEndPoint(row.Item1.SiloAddress.Endpoint.Address, row.Item1.ProxyPort),
                row.Item1.SiloAddress.Generation).ToGatewayUri()),];
    }
}
