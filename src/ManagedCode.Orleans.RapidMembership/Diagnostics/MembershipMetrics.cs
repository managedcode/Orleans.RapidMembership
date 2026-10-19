using System.Diagnostics.Metrics;

namespace ManagedCode.Orleans.RapidMembership.Diagnostics;

internal static class MembershipMetrics
{
    private static readonly Meter Meter = new("ManagedCode.Orleans.RapidMembership");
    private static readonly Counter<long> Exchanges = Meter.CreateCounter<long>("membership.peer.exchanges");
    private static readonly Counter<long> Proposals = Meter.CreateCounter<long>("membership.proposals");

    public static void Exchange(PeerAction action, bool granted) => Exchanges.Add(1,
        new KeyValuePair<string, object?>("action", action switch
        {
            PeerAction.Inspect => "inspect",
            PeerAction.Prepare => "prepare",
            PeerAction.Accept => "accept",
            PeerAction.Commit => "commit",
            PeerAction.Describe => "describe",
            PeerAction.Locate => "locate",
            PeerAction.FetchTransfer => "fetch-transfer",
            PeerAction.FreezeConfiguration => "freeze-configuration",
            PeerAction.InstallConfiguration => "install-configuration",
            _ => "invalid",
        }), new KeyValuePair<string, object?>("granted", granted));

    public static void Proposal(bool completed) => Proposals.Add(1, new KeyValuePair<string, object?>("completed", completed));
}
