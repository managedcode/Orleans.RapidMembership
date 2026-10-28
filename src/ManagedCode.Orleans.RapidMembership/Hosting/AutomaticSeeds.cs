using System.Net;
using RapidCluster.Discovery;

namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class AutomaticSeeds : ISeedProvider
{
    public IReadOnlyList<EndPoint> Endpoints { get; set; } = [];

    public ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(Endpoints);
    }

    public void Set(CoordinationConfiguration configuration) => Endpoints = [.. configuration.Members.Select(member =>
    {
        var uri = new Uri(member.Endpoint);
        return (EndPoint)new IPEndPoint(IPAddress.Parse(uri.Host.Trim('[', ']')), uri.Port);
    }),];
}
