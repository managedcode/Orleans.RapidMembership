using System.Diagnostics.CodeAnalysis;
using Clockwork.Runtime.Execution;
using Clockwork.Runtime.Replay;
using Clockwork.Runtime.Scheduling;
using Clockwork.Testing;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>Acceptance coverage for canonical Clockwork artifact record, persistence, and exact replay.</summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ClusterReplayAcceptanceTests
{
    [Fact]
    public void ArtifactControlsOperationsThatDriveThreeNodeFormationAndGracefulRemoval()
    {
        string artifactDirectory = Path.Combine(
            Path.GetTempPath(),
            "rapidcluster-clockwork-replay-" + Guid.NewGuid().ToString("N"));

        try
        {
            var recordedObservation = new ClusterScenarioObservation();
            var fixture = new ReplayTestFixture(new ReplayTestConfiguration
            {
                TestClassName = nameof(ClusterReplayAcceptanceTests),
                TestMethodName = nameof(ArtifactControlsOperationsThatDriveThreeNodeFormationAndGracefulRemoval),
                RootSeed = 88031,
                ScheduleSeed = 5519,
                ArtifactDirectory = artifactDirectory,
                RecordSuccessfulRuns = true,
            });

            ReplayTestResult recorded = fixture.Run(
                scheduler => ScheduleClusterScenario(scheduler, recordedObservation),
                TestContext.Current.CancellationToken);

            Assert.True(
                recorded.IsSuccessful,
                recorded.ToFailureMessage() + Environment.NewLine + recordedObservation.Failure);
            Assert.False(recorded.Execution.Reproduced);
            string artifactPath = Assert.IsType<string>(recorded.ArtifactPath);
            Assert.True(File.Exists(artifactPath));

            ReplayArtifact artifact = ReplayArtifactSerializer.Read(artifactPath);
            string persistedArtifactId = ReplayArtifactSerializer.ComputeId(artifact);
            Assert.Equal(recorded.Execution.ArtifactId, persistedArtifactId);
            Assert.Equal(ReplayRecordingState.Complete, artifact.RecordingState);
            Assert.Equal(ReplayTerminationKind.Completed, artifact.Outcome.Kind);
            Assert.Null(artifact.Outcome.FailureIdentity);
            Assert.NotEmpty(artifact.Decisions);
            Assert.Equal(artifact.Decisions.Count - 1, artifact.Decisions[^1].Sequence);

            var replayedObservation = new ClusterScenarioObservation();
            ReplayExecutionResult replayed = ReplayRunner.Replay(
                artifact,
                ReplayCompatibilityRequirements.Current(),
                scheduler => ScheduleClusterScenario(scheduler, replayedObservation),
                maxSteps: 1_000_000,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(replayed.Reproduced);
            Assert.Equal(persistedArtifactId, replayed.ArtifactId);
            Assert.Equal(recorded.Execution.Steps, replayed.Steps);
            Assert.Equal(recordedObservation.Events, replayedObservation.Events);
            Assert.Equal(["formed:3", "removed:1", "reconverged:2"], replayedObservation.ClusterEvents);
        }
        finally
        {
            if (Directory.Exists(artifactDirectory))
            {
                Directory.Delete(artifactDirectory, recursive: true);
            }
        }
    }

    private static void ScheduleClusterScenario(
        ControlledOperationScheduler scheduler,
        ClusterScenarioObservation observation)
    {
        scheduler.Schedule(
            "drive-three-node-cluster-through-graceful-removal",
            () =>
            {
                observation.Events.Add("cluster:start");
                try
                {
                    RunClusterScenarioWithoutReplayAmbient(observation);
                    observation.Events.Add("cluster:end");
                }
                catch (Exception exception)
                {
                    observation.Failure = exception.ToString();
                    throw;
                }
            });
        scheduler.Schedule(
            "artifact-controlled-companion-operation",
            () =>
            {
                observation.Events.Add("companion:start");
                scheduler.Yield();
                observation.Events.Add("companion:end");
            });
    }

    private static void RunClusterScenarioWithoutReplayAmbient(ClusterScenarioObservation observation)
    {
        Task scenario;
        using (SimulationExecutionContext.SuppressFlow("Run a fresh RapidSimulationCluster from a replay-controlled operation."))
        {
            scenario = Task.Run(() => RunClusterScenario(observation));
        }

        scenario.GetAwaiter().GetResult();
    }

    private static void RunClusterScenario(ClusterScenarioObservation observation)
    {
        var harness = new RapidSimulationCluster(
            seed: 42117,
            startDateTime: new DateTimeOffset(2041, 5, 6, 7, 8, 9, TimeSpan.Zero));

        try
        {
            IReadOnlyList<RapidSimulationNode> nodes = harness.CreateCluster(size: 3);
            harness.WaitForConvergence();
            Ensure(nodes.Count == 3 && nodes.All(static node => node.MembershipSize == 3), "Three-node formation did not converge.");
            observation.ClusterEvents.Add("formed:3");

            harness.RemoveNodeGracefully(nodes[2]);
            observation.ClusterEvents.Add("removed:1");
            harness.WaitForConvergence();
            Ensure(harness.Nodes.Count == 2, "Graceful removal did not leave two registered nodes.");
            Ensure(harness.Nodes.All(static node => node.MembershipSize == 2), "Remaining nodes did not reconverge to two members.");
            harness.AssertNoWarningsOrErrors();
            observation.ClusterEvents.Add("reconverged:2");
        }
        finally
        {
            harness.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private static void Ensure(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private sealed class ClusterScenarioObservation
    {
        public List<string> Events { get; } = [];

        public List<string> ClusterEvents { get; } = [];

        public string? Failure { get; set; }
    }
}
