using Orleans;
using Orleans.Placement;

namespace ManagedCode.Orleans.RapidMembership.Node.Grains;

[RandomPlacement]
public sealed class ProbeGrain(global::Orleans.Runtime.ILocalSiloDetails details) : Grain, IProbeGrain
{
    private readonly Guid _activation = Guid.NewGuid();
    private long _sequence;

    public Task<string> Echo(string value) => Task.FromResult(value);

    public Task<ProbeReply> Observe(string value) => Task.FromResult(new ProbeReply(value, details.SiloAddress.ToString(), _activation, ++_sequence));
}
