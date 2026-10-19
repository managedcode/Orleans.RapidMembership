using Orleans;

namespace ManagedCode.Orleans.RapidMembership.Node.Contracts;

[Alias("ManagedCode.RapidMembership.TestProbe")]
public interface IProbeGrain : IGrainWithStringKey
{
    [Alias("Echo")]
    Task<string> Echo(string value);

    [Alias("CrashProcess")]
    Task CrashProcess(string expectedSilo);

    [Alias("Observe")]
    Task<ProbeReply> Observe(string value);
}

[GenerateSerializer, Alias("ManagedCode.RapidMembership.TestProbeReply")]
public sealed record ProbeReply([property: Id(0)] string Value, [property: Id(1)] string Silo,
    [property: Id(2)] Guid Activation, [property: Id(3)] long Sequence);
