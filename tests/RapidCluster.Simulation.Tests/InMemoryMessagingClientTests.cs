using System.Diagnostics.CodeAnalysis;
using RapidCluster.Monitoring;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class InMemoryMessagingClientTests
{
    [Fact]
    public async Task SendOneWayMessage_UnavailableTarget_ReportsDeliveryFailure()
    {
        await using var harness = new RapidSimulationCluster(seed: 7341);
        var source = harness.StartCluster(
            failureDetectorOptions: new PingPongFailureDetectorOptions { ProbeTimeout = TimeSpan.FromSeconds(1) });
        var unavailable = RapidClusterUtils.HostFromParts("node", 999);
        var failureReported = false;

        harness.RunOnNode(
            source,
            () => source.MessagingClient.SendOneWayMessage(
                unavailable,
                new ProbeMessage { Sender = source.Address }.ToRapidClusterRequest(),
                new Rank { Round = 1, NodeIndex = 0 },
                (_, _) => failureReported = true,
                TestContext.Current.CancellationToken));

        _ = harness.RunForDuration(TimeSpan.FromSeconds(2));

        Assert.True(failureReported);
    }

    [Fact]
    public async Task SendOneWayMessage_CanceledSend_DoesNotReportDeliveryFailure()
    {
        await using var harness = new RapidSimulationCluster(seed: 7342);
        var source = harness.StartCluster(
            failureDetectorOptions: new PingPongFailureDetectorOptions { ProbeTimeout = TimeSpan.FromSeconds(1) });
        var unavailable = RapidClusterUtils.HostFromParts("node", 999);
        var failureReported = false;
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        harness.RunOnNode(
            source,
            () => source.MessagingClient.SendOneWayMessage(
                unavailable,
                new ProbeMessage { Sender = source.Address }.ToRapidClusterRequest(),
                new Rank { Round = 1, NodeIndex = 0 },
                (_, _) => failureReported = true,
                cancellation.Token));

        _ = harness.RunForDuration(TimeSpan.FromSeconds(2));

        Assert.False(failureReported);
    }
}
