using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Clockwork.Runtime.Decisions;
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
    private const string FormationPhase = "cluster-phase:three-node-formation";
    private const string RemovalPhase = "cluster-phase:graceful-removal";
    private const string ReconvergencePhase = "cluster-phase:two-node-reconvergence";

    [Fact]
    public void ArtifactControlsOperationsThatDriveThreeNodeFormationAndGracefulRemoval()
    {
        var artifactDirectory = Path.Combine(
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

            var recorded = fixture.Run(
                scheduler => ScheduleClusterScenario(scheduler, recordedObservation),
                TestContext.Current.CancellationToken);

            Assert.True(
                recorded.IsSuccessful,
                recorded.ToFailureMessage() + Environment.NewLine + recordedObservation.Failure);
            Assert.False(recorded.Execution.Reproduced);
            var artifactPath = Assert.IsType<string>(recorded.ArtifactPath);
            Assert.True(File.Exists(artifactPath));

            var artifact = ReplayArtifactSerializer.Read(artifactPath);
            var persistedArtifactId = ReplayArtifactSerializer.ComputeId(artifact);
            Assert.Equal(recorded.Execution.ArtifactId, persistedArtifactId);
            Assert.Equal(ReplayRecordingState.Complete, artifact.RecordingState);
            Assert.Equal(ReplayTerminationKind.Completed, artifact.Outcome.Kind);
            Assert.Null(artifact.Outcome.FailureIdentity);
            Assert.Equal(artifact.Decisions.Count - 1, artifact.Decisions[^1].Sequence);
            Assert.Equal([FormationPhase, RemovalPhase, ReconvergencePhase], recordedObservation.PhaseSources);

            var recordedPhaseDecisionSources = GetPhaseDecisionSources(artifact, recordedObservation);
            Assert.Equal([FormationPhase, RemovalPhase, ReconvergencePhase], recordedPhaseDecisionSources);

            var replayedObservation = new ClusterScenarioObservation();
            var replayed = ReplayRunner.Replay(
                artifact,
                ReplayCompatibilityRequirements.Current(),
                scheduler => ScheduleClusterScenario(scheduler, replayedObservation),
                maxSteps: 1_000_000,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.True(replayed.Reproduced);
            Assert.Equal(persistedArtifactId, replayed.ArtifactId);
            Assert.Equal(recorded.Execution.Steps, replayed.Steps);
            Assert.Equal(recordedObservation.Events, replayedObservation.Events);
            Assert.Equal(recordedObservation.PhaseOperationIds, replayedObservation.PhaseOperationIds);
            Assert.Equal(recordedPhaseDecisionSources, GetPhaseDecisionSources(artifact, replayedObservation));
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

    private static string[] GetPhaseDecisionSources(
        ReplayArtifact artifact,
        ClusterScenarioObservation observation)
    {
        var phaseIds = observation.PhaseOperationIds
            .Select(static id => id.ToString(CultureInfo.InvariantCulture))
            .ToArray();
        var phaseNamesById = phaseIds
            .Zip(observation.PhaseSources, static (id, source) => (id, source))
            .ToDictionary(static pair => pair.id, static pair => pair.source, StringComparer.Ordinal);
        var phaseDecisions = artifact.Decisions
            .Where(decision =>
                decision.Kind == SimulationDecisionKind.SchedulingOrder &&
                phaseNamesById.ContainsKey(decision.SelectedResult))
            .ToArray();

        Assert.Equal(phaseIds, phaseDecisions.Select(static decision => decision.SelectedResult));
        Assert.All(phaseDecisions, static decision => Assert.False(string.IsNullOrWhiteSpace(decision.SourceId)));
        return [.. phaseDecisions.Select(decision => phaseNamesById[decision.SelectedResult])];
    }

    private static void ScheduleClusterScenario(
        ControlledOperationScheduler scheduler,
        ClusterScenarioObservation observation)
    {
#pragma warning disable CA2000 // The controlled companion operation owns and disposes the scenario.
        var scenario = new ControlledClusterScenario(scheduler, observation);
#pragma warning restore CA2000
        scenario.ScheduleFormation();
        _ = scheduler.Schedule("cluster-phase:decision-companion", scenario.RunDecisionCompanion);
    }

    private sealed class ControlledClusterScenario(
        ControlledOperationScheduler scheduler,
        ClusterScenarioObservation observation) : IDisposable
    {
        private RapidSimulationCluster? _harness;
        private IReadOnlyList<RapidSimulationNode>? _nodes;
        private int _completed;

        public void ScheduleFormation() => SchedulePhase(FormationPhase, RunFormation);

        public void RunDecisionCompanion()
        {
            try
            {
                observation.Events.Add("companion:start");
                while (Volatile.Read(ref _completed) == 0)
                {
                    scheduler.Yield();
                }

                observation.Events.Add("companion:end");
            }
            finally
            {
                Dispose();
            }
        }

        private void RunFormation()
        {
            RunPhase(
                FormationPhase,
                () => RunWithoutReplayAmbient(
                    "Run the formation phase in a fresh RapidSimulationCluster runtime.",
                    () =>
                    {
                        _harness = new RapidSimulationCluster(
                            seed: 42117,
                            startDateTime: new DateTimeOffset(2041, 5, 6, 7, 8, 9, TimeSpan.Zero));
                        _nodes = _harness.CreateCluster(size: 3);
                        _harness.WaitForConvergence();
                        Ensure(
                            _nodes.Count == 3 && _nodes.All(static node => node.MembershipSize == 3),
                            "Three-node formation did not converge.");
                        observation.ClusterEvents.Add("formed:3");
                    }),
                () => SchedulePhase(RemovalPhase, RunRemoval));
        }

        private void RunRemoval()
        {
            RunPhase(
                RemovalPhase,
                () => RunWithoutReplayAmbient(
                    "Run graceful removal in the RapidSimulationCluster runtime.",
                    () =>
                    {
                        _harness!.RemoveNodeGracefully(_nodes![2]);
                        observation.ClusterEvents.Add("removed:1");
                    }),
                () => SchedulePhase(ReconvergencePhase, RunReconvergence));
        }

        private void RunReconvergence()
        {
            try
            {
                RunPhase(
                    ReconvergencePhase,
                    () => RunWithoutReplayAmbient(
                        "Run reconvergence in the RapidSimulationCluster runtime.",
                        () =>
                        {
                            _harness!.WaitForConvergence();
                            Ensure(_harness.Nodes.Count == 2, "Graceful removal did not leave two registered nodes.");
                            Ensure(
                                _harness.Nodes.All(static node => node.MembershipSize == 2),
                                "Remaining nodes did not reconverge to two members.");
                            _harness.AssertNoWarningsOrErrors();
                            observation.ClusterEvents.Add("reconverged:2");
                        }),
                    nextPhase: null);
            }
            finally
            {
                DisposeHarness();
                Volatile.Write(ref _completed, 1);
            }
        }

        private void RunPhase(string source, Action phase, Action? nextPhase)
        {
            observation.Events.Add(source + ":start");
            observation.PhaseSources.Add(source);
            try
            {
                phase();
                observation.Events.Add(source + ":end");
                nextPhase?.Invoke();
            }
            catch (Exception exception)
            {
                observation.Failure = exception.ToString();
                DisposeHarness();
                Volatile.Write(ref _completed, 1);
                throw;
            }
        }

        private void SchedulePhase(string source, Action phase)
        {
            var operation = scheduler.Schedule(source, phase);
            observation.PhaseOperationIds.Add(operation.Id.Value);
        }

        private void DisposeHarness()
        {
            if (_harness is null)
            {
                return;
            }

            RunWithoutReplayAmbient(
                "Dispose the RapidSimulationCluster outside the replay scheduler runtime.",
                () => _harness.DisposeAsync().AsTask().GetAwaiter().GetResult());
            _harness = null;
        }

        private static void RunWithoutReplayAmbient(string reason, Action action)
        {
            Task phase;
            using (SimulationExecutionContext.SuppressFlow(reason))
            {
                phase = Task.Run(action);
            }

            phase.GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            DisposeHarness();
            GC.SuppressFinalize(this);
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

        public List<string> PhaseSources { get; } = [];

        public List<long> PhaseOperationIds { get; } = [];

        public string? Failure { get; set; }
    }
}
