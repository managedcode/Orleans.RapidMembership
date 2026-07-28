using System.Diagnostics;
using System.Globalization;
using Clockwork;
using Clockwork.Runtime.Execution;
using Clockwork.Runtime.Random;
using Microsoft.Extensions.Logging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// <para>
/// Unified simulation harness for fully deterministic testing of Rapid clusters.
/// Extends <see cref="SimulationCluster{TNode}"/> with RapidCluster-specific functionality.
/// </para>
/// <para>
/// Provides:
/// - Deterministic task scheduling via per-node <see cref="SimulationTaskScheduler"/> instances
/// - Controlled time via shared <see cref="SimulationClock"/>
/// - Seeded random number generation via <see cref="SimulationRandom"/>
/// - Simulated network with partition injection via <see cref="SimulationNetwork"/>
/// - Node lifecycle management (create, join, crash, leave)
/// - Per-node execution control (suspend, resume, step)
/// - Simulation driving APIs (Step, RunUntil, Run).
/// </para>
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class RapidSimulationCluster : SimulationCluster<RapidSimulationNode>
{
    internal static readonly DateTimeOffset DefaultStartTime = DateTimeOffset.UnixEpoch;

    private readonly SimulationLogManager _logManager;
    private readonly ILogger<RapidSimulationCluster> _logger;
    private readonly SimulationHarnessLogger _log;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationCluster"/> class.
    /// Creates a new simulation harness with the specified seed.
    /// Logs are written to a unique file per simulation and attached to the test context.
    /// </summary>
    /// <param name="seed">The seed for deterministic random number generation.</param>
    /// <param name="startTime">
    /// Optional virtual-time origin. Defaults to <see cref="DateTimeOffset.UnixEpoch"/>
    /// so identical seeds start from identical time.
    /// </param>
    public RapidSimulationCluster(int seed, DateTimeOffset? startTime = null)
        : base(seed, startTime ?? DefaultStartTime, cancellationToken: TestContext.Current.CancellationToken)
    {
        Network = new SimulationNetwork(
            this,
            new SimulationRandom(SeedAuthority.GetDomainSeed(SimulationSeedDomain.Network)));

        // Create log manager for logger factory and log attachment
        _logManager = new SimulationLogManager(TimeProvider, seed);
        LoggerFactory = _logManager.LoggerFactory;

        _logger = LoggerFactory.CreateLogger<RapidSimulationCluster>();
        _log = new SimulationHarnessLogger(_logger);
        Network.SetLogger(LoggerFactory.CreateLogger<SimulationNetwork>());

        _log.HarnessCreated(seed);
    }

    /// <summary>
    /// Gets the logger factory.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

    /// <summary>
    /// Gets the in-memory log buffer for inspecting log entries during tests.
    /// </summary>
    public InMemoryLogBuffer LogBuffer => _logManager.LogBuffer;

    /// <summary>
    /// Gets the simulation network.
    /// </summary>
    public SimulationNetwork Network { get; }

    internal IDisposable EnterSimulationRuntime()
    {
        var ambient = CreateNodeAmbientContext("rapidcluster-harness");
        return SimulationExecutionContext.EnterRuntime(ambient.ActivationToken, ambient.Runtime);
    }

    internal SimulationAmbientContextConfiguration CreateRapidNodeAmbientContext(Endpoint address) =>
        CreateNodeAmbientContext(RapidClusterUtils.Loggable(address));

    internal IDisposable EnterSimulationNodeRuntime(Endpoint address)
    {
        var ambient = CreateRapidNodeAmbientContext(address);
        var runtimeScope = SimulationExecutionContext.EnterRuntime(ambient.ActivationToken, ambient.Runtime);
        var nodeScope = SimulationExecutionContext.EnterNode(ambient.Node!);
        return new SimulationRuntimeScope(nodeScope, runtimeScope);
    }

    internal T StartInSimulationNode<T>(Endpoint address, Func<T> action)
    {
        using var _ = EnterSimulationNodeRuntime(address);
        return action();
    }

    /// <summary>
    /// Executes a synchronous boundary call within the specified node's simulation runtime.
    /// </summary>
    public void ExecuteInSimulation(RapidSimulationNode node, Action action)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(action);
        _ = InvokeInSimulation(
            node,
            () =>
            {
                action();
                return true;
            });
    }

    /// <summary>
    /// Invokes a synchronous boundary call within the specified node's simulation runtime.
    /// </summary>
    public T InvokeInSimulation<T>(RapidSimulationNode node, Func<T> action)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(action);
        using var runtimeScope = EnterSimulationNodeRuntime(node.Address);
        using var guardScope = Guard.Enter();
        return action();
    }

    /// <summary>
    /// Gets a node by its network address.
    /// </summary>
    /// <param name="address">The network address of the node.</param>
    /// <returns>The node with the specified address, or null if not found.</returns>
    public new RapidSimulationNode? GetNode(string address) => base.GetNode(address);

    /// <summary>
    /// Adds a node without starting it (for testing edge cases).
    /// The node is registered but not started or joined to any cluster.
    /// </summary>
    /// <param name="nodeId">The node ID.</param>
    /// <param name="seed">Optional seed node for joining. If null, the node will start its own cluster when initialized.</param>
    /// <param name="options">Optional protocol options.</param>
    public RapidSimulationNode AddUnstartedNode(int nodeId, RapidSimulationNode? seed = null, RapidClusterProtocolOptions? options = null)
    {
        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seed?.Address, metadata: null, options, failureDetectorOptions: null, LoggerFactory);
        RegisterNode(node);
        _log.UninitializedNodeCreated(nodeId);
        return node;
    }

    /// <summary>
    /// Starts a new cluster with a seed node.
    /// </summary>
    public RapidSimulationNode StartCluster(int nodeId = 0, RapidClusterProtocolOptions? options = null, PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seedAddress: null, metadata: null, options, failureDetectorOptions, LoggerFactory);
        RegisterNode(node);

        _ = RunToCompletion(() => node.StartAsync(TestContext.Current.CancellationToken));
        node.CompleteInitialization();

        _log.SeedNodeCreated(nodeId);
        return node;
    }

    /// <summary>
    /// Creates and joins a new node to the cluster through the specified seed.
    /// Drives the simulation to complete the join.
    /// </summary>
    public RapidSimulationNode JoinNode(
        int nodeId,
        RapidSimulationNode seed,
        RapidClusterProtocolOptions? options = null,
        PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        ArgumentNullException.ThrowIfNull(seed);
        return JoinNode(nodeId, [seed], options, failureDetectorOptions);
    }

    /// <summary>
    /// Creates and joins a new node to the cluster with multiple seed addresses.
    /// The node will try seeds in round-robin order until join succeeds.
    /// Drives the simulation to complete the join.
    /// </summary>
    public RapidSimulationNode JoinNode(
        int nodeId,
        IReadOnlyList<RapidSimulationNode> seeds,
        RapidClusterProtocolOptions? options = null,
        PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        ArgumentNullException.ThrowIfNull(seeds);
        if (seeds.Count == 0)
        {
            throw new ArgumentException("At least one seed node is required.", nameof(seeds));
        }

        if (seeds.Select(static seed => RapidClusterUtils.Loggable(seed.Address)).Distinct(StringComparer.Ordinal).Count() != seeds.Count)
        {
            throw new ArgumentException("Seed nodes must be unique.", nameof(seeds));
        }

        return JoinNode(nodeId, seeds.Select(static seed => seed.Address).ToList(), options, failureDetectorOptions);
    }

    /// <summary>
    /// Creates and joins a new node to the cluster with raw seed addresses.
    /// Useful for testing duplicate seed handling and other edge cases.
    /// </summary>
    public RapidSimulationNode JoinNode(
        int nodeId,
        IReadOnlyList<Endpoint> seedAddresses,
        RapidClusterProtocolOptions? options = null,
        PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        ArgumentNullException.ThrowIfNull(seedAddresses);
        if (seedAddresses.Count == 0)
        {
            throw new ArgumentException("At least one seed address is required.", nameof(seedAddresses));
        }

        if (seedAddresses.Select(static seed => RapidClusterUtils.Loggable(seed)).Distinct(StringComparer.Ordinal).Count() != seedAddresses.Count)
        {
            throw new ArgumentException("Seed addresses must be unique.", nameof(seedAddresses));
        }

        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seedAddresses, metadata: null, options, failureDetectorOptions, LoggerFactory);
        RegisterNode(node);

        _log.NodeJoining(nodeId);

        _ = RunToCompletion(() => node.StartAsync(TestContext.Current.CancellationToken));
        node.CompleteInitialization();

        _log.NodeJoined(nodeId);
        return node;
    }

    /// <summary>
    /// Creates a cluster of the specified size using sequential joins.
    /// Each node joins and waits for consensus before the next node joins.
    /// This results in O(N) consensus rounds but guarantees deterministic behavior.
    /// </summary>
    /// <remarks>
    /// For large clusters (50+ nodes), consider using <see cref="CreateClusterInBatches"/>
    /// which batches joins together for O(log N) consensus rounds.
    /// </remarks>
    public IReadOnlyList<RapidSimulationNode> CreateCluster(int size, RapidClusterProtocolOptions? options = null, PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "Cluster size must be at least 1");
        }

        var result = new List<RapidSimulationNode>(size);

        // Create seed node
        var seedNode = StartCluster(0, options, failureDetectorOptions);
        result.Add(seedNode);

        // Create joiner nodes
        for (var i = 1; i < size; i++)
        {
            var joiner = JoinNode(i, seedNode, options, failureDetectorOptions);
            result.Add(joiner);

            // Ensure the cluster catches up between sequential joins.
            // This avoids subsequent joins using stale configuration IDs.
            WaitForConvergence();
        }

        WaitForConvergence();
        return result;
    }

    /// <summary>
    /// Creates a cluster of the specified size using concurrent joins in stable batches.
    /// Multiple nodes join concurrently, allowing the multi-node cut detection
    /// to batch them into fewer consensus rounds (O(log N) instead of O(N)).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This matches the behavior described in the Rapid paper where 2000 nodes
    /// were bootstrapped with only 8 configuration changes. The multi-node cut
    /// detection aggregates pending JOIN alerts and proposes them together.
    /// </para>
    /// <para>
    /// For small clusters or when deterministic single-node-at-a-time behavior
    /// is needed, use <see cref="CreateCluster"/> instead.
    /// </para>
    /// </remarks>
    /// <param name="size">The number of nodes in the cluster.</param>
    /// <param name="options">Optional protocol options.</param>
    /// <param name="failureDetectorOptions">Optional failure detector options applied to every node.</param>
    /// <param name="batchSize">
    /// Number of nodes to initiate joining simultaneously. The default joins all remaining nodes at once.
    /// Smaller batch sizes provide more control over join ordering while still enabling batching.
    /// </param>
    /// <returns>List of all nodes in the cluster.</returns>
    public IReadOnlyList<RapidSimulationNode> CreateClusterInBatches(
        int size,
        RapidClusterProtocolOptions? options = null,
        PingPongFailureDetectorOptions? failureDetectorOptions = null,
        int? batchSize = null)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "Cluster size must be at least 1");
        }

        var result = new List<RapidSimulationNode>(size);

        // Create seed node first
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero when specified.");
        }

        var seedNode = StartCluster(0, options, failureDetectorOptions);
        result.Add(seedNode);

        if (size == 1)
        {
            return result;
        }

        var effectiveBatchSize = batchSize ?? size - 1;

        // Process nodes in batches
        var remainingNodes = size - 1;
        var nodeId = 1;

        while (remainingNodes > 0)
        {
            var currentBatchSize = Math.Min(effectiveBatchSize, remainingNodes);
            var batchNodes = new List<RapidSimulationNode>(currentBatchSize);

            // Create all nodes in this batch first (with seedAddress so MembershipService is ready)
            for (var i = 0; i < currentBatchSize; i++)
            {
                var address = RapidClusterUtils.HostFromParts("node", nodeId++);
                var node = new RapidSimulationNode(this, address, seedNode.Address, metadata: null, options, failureDetectorOptions, LoggerFactory);
                RegisterNode(node);

                _log.NodeJoiningParallel(RapidClusterUtils.Loggable(node.Address));

                batchNodes.Add(node);
            }

            _ = RunToCompletion(
                () => Task.WhenAll(batchNodes.Select(node => node.StartAsync(TestContext.Current.CancellationToken))));

            // Log completion
            foreach (var node in batchNodes)
            {
                node.CompleteInitialization();
                _log.NodeJoinedParallel(RapidClusterUtils.Loggable(node.Address));
            }

            result.AddRange(batchNodes);
            remainingNodes -= currentBatchSize;

            // Complete the current view change before handing the stable membership to the next batch.
            var expectedSize = result.Count;
            WaitForMembershipSize(expectedSize, result);
        }

        return result;
    }

    /// <summary>
    /// Crashes a node (simulates sudden failure).
    /// Disposes and unregisters the node - no cleanup or leave messages are sent.
    /// The disposal cancels in-flight tasks and releases resources to prevent memory leaks.
    /// </summary>
    public void CrashNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);

        // Dispose the node while it is still registered so the simulation can drive its task queue.
        // This cancels in-flight tasks and releases resources.
        _ = RunToCompletion(() => node.DisposeAsync().AsTask());

        // Unregister after disposal - no new messages will be delivered
        UnregisterNode(node);

        _log.NodeCrashed();
    }

    /// <summary>
    /// Gracefully removes a node from the cluster.
    /// The leaving node must participate in the consensus round that removes it,
    /// so we keep it alive until consensus completes and all remaining nodes converge.
    /// </summary>
    public void RemoveNodeGracefully(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        _log.NodeLeaving();

        var remainingNodes = Nodes.Where(n => n != node).ToList();
        var targetSize = remainingNodes.Count;

        // Drive the stop operation to completion (sends LeaveMessages to observers)
        // Node can still receive messages after this
        _ = RunToCompletion(() => node.StopAsync(TestContext.Current.CancellationToken));
        node.CompleteStop();

        // The leaving node must remain active to participate in consensus.
        // Run the simulation until all remaining nodes converge to the new size.
        WaitForMembershipSize(targetSize, remainingNodes);

        // Dispose the node's resources while it's still registered.
        // This ensures the simulation can drive the node's task queue during disposal.
        _ = RunToCompletion(() => node.DisposeAsync().AsTask());

        // Unregister the node from the network (no more messages will be delivered)
        UnregisterNode(node);

        _log.NodeLeft();
    }

    /// <summary>
    /// <para>
    /// Gracefully removes multiple nodes from the cluster in parallel.
    /// This allows the multi-node cut detection to batch multiple leaves into
    /// fewer consensus rounds, similar to how parallel joins work.
    /// </para>
    /// <para>
    /// All leaving nodes initiate their leave concurrently, enabling the
    /// batching mechanism to combine their alerts into single view changes.
    /// </para>
    /// </summary>
    /// <param name="nodesToRemove">The nodes to remove from the cluster.</param>
    /// <returns>The number of configuration changes that occurred during the parallel leave.</returns>
    public int RemoveNodesGracefullyParallel(IReadOnlyList<RapidSimulationNode> nodesToRemove)
    {
        ArgumentNullException.ThrowIfNull(nodesToRemove);
        if (nodesToRemove.Count == 0)
        {
            return 0;
        }

        // Get the starting configuration version to measure changes
        var remainingNodes = Nodes.Where(n => !nodesToRemove.Contains(n)).ToList();
        var startingConfigVersion = remainingNodes[0].CurrentView.ConfigurationId.Version;
        var targetSize = remainingNodes.Count;

        _log.NodesLeavingParallel(nodesToRemove.Count);

        foreach (var node in nodesToRemove)
        {
            _log.NodeLeavingParallel(RapidClusterUtils.Loggable(node.Address));
        }

        _ = RunToCompletion(() =>
        {
            var leaveTasks = nodesToRemove.Select(node => node.StopAsync(TestContext.Current.CancellationToken));
            return Task.WhenAll(leaveTasks);
        });
        foreach (var node in nodesToRemove)
        {
            node.CompleteStop();
        }

        WaitForMembershipSize(targetSize, remainingNodes);

        // Dispose all leaving nodes' resources while they're still registered.
        // This ensures the simulation can drive each node's task queue during disposal.
        _ = RunToCompletion(() =>
        {
            var disposeTasks = nodesToRemove.Select(node => node.DisposeAsync().AsTask());
            return Task.WhenAll(disposeTasks);
        });

        // Unregister leaving nodes from the network (no more messages will be delivered)
        foreach (var node in nodesToRemove)
        {
            UnregisterNode(node);
            _log.NodeLeftParallel(RapidClusterUtils.Loggable(node.Address));
        }

        // Calculate configuration changes
        var endingConfigVersion = remainingNodes[0].CurrentView.ConfigurationId.Version;
        var configChanges = (int)(endingConfigVersion - startingConfigVersion);

        _log.ParallelLeaveCompleted(nodesToRemove.Count, configChanges);

        return configChanges;
    }

    /// <summary>
    /// Isolates a node from the cluster.
    /// </summary>
    public void IsolateNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var addr = RapidClusterUtils.Loggable(node.Address);
        Network.IsolateNode(addr);
        _log.NodeIsolated();
    }

    /// <summary>
    /// Reconnects an isolated node.
    /// </summary>
    public void ReconnectNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var addr = RapidClusterUtils.Loggable(node.Address);
        Network.ReconnectNode(addr);
        _log.NodeReconnected();
    }

    /// <summary>
    /// Creates a partition between two nodes.
    /// </summary>
    public void PartitionNodes(RapidSimulationNode node1, RapidSimulationNode node2)
    {
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);
        Network.CreateBidirectionalPartition(addr1, addr2);
        _log.PartitionCreated();
    }

    /// <summary>
    /// Heals a partition between two nodes.
    /// </summary>
    public void HealPartition(RapidSimulationNode node1, RapidSimulationNode node2)
    {
        using var _ = Guard.Enter();
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);
        Network.HealBidirectionalPartition(addr1, addr2);
        _log.PartitionHealed();
    }

    /// <summary>
    /// Waits until the selected nodes have the expected membership size.
    /// When <paramref name="nodes"/> is omitted, suspended and departed nodes are excluded.
    /// </summary>
    public void WaitForMembershipSize(
        int expectedSize,
        IEnumerable<RapidSimulationNode>? nodes = null,
        SimulationAdaptiveBudget? budget = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(expectedSize);
        var participants = nodes?.ToList() ??
            ActiveNodes.Where(static node => node.CurrentView.IsHostPresent(node.Address)).ToList();
        var outcome = RunUntilConverged(
            () => participants.TrueForAll(node => node.MembershipSize == expectedSize),
            budget);
        if (!outcome.ConditionMet)
        {
            var nodeDetails = participants.Count == 0
                ? "<none>"
                : string.Join(
                    ", ",
                    participants.Select(node => string.Create(
                        CultureInfo.InvariantCulture,
                        $"{RapidClusterUtils.Loggable(node.Address)}=size:{node.MembershipSize},suspended:{node.IsSuspended},present:{node.CurrentView.IsHostPresent(node.Address)}")));
            throw new TimeoutException(
                string.Create(CultureInfo.InvariantCulture, $"Nodes did not reach membership size {expectedSize}. Participants: [{nodeDetails}].") +
                Environment.NewLine +
                outcome.ToDetailedString());
        }
    }

    /// <summary>
    /// Starts a task normally and adaptively drives the simulation until it completes.
    /// </summary>
    public SimulationExecutionResult RunToCompletion(
        Func<Task> taskFactory,
        SimulationAdaptiveBudget? budget = null)
    {
        ArgumentNullException.ThrowIfNull(taskFactory);
        Task task;
        using (EnterSimulationRuntime())
        using (Guard.Enter())
        {
            task = taskFactory();
        }

        var outcome = RunUntilConverged(() => task.IsCompleted, budget);
        if (!outcome.ConditionMet)
        {
            throw new TimeoutException(
                $"Task did not complete.{Environment.NewLine}{outcome.ToDetailedString()}");
        }

        task.GetAwaiter().GetResult();
        return outcome;
    }

    /// <summary>
    /// Starts a result-producing task normally and adaptively drives the simulation until it completes.
    /// </summary>
    public T RunToCompletion<T>(
        Func<Task<T>> taskFactory,
        SimulationAdaptiveBudget? budget = null)
    {
        ArgumentNullException.ThrowIfNull(taskFactory);
        Task<T> task;
        using (EnterSimulationRuntime())
        using (Guard.Enter())
        {
            task = taskFactory();
        }

        var outcome = RunUntilConverged(() => task.IsCompleted, budget);
        if (!outcome.ConditionMet)
        {
            throw new TimeoutException(
                $"Task did not complete.{Environment.NewLine}{outcome.ToDetailedString()}");
        }

        return task.GetAwaiter().GetResult();
    }

    /// <summary>
    /// Waits for all active cluster members to converge to a consistent view.
    /// Suspended nodes and nodes whose current view no longer contains themselves do not
    /// participate in convergence.
    /// </summary>
    public void WaitForConvergence()
    {
        var outcome = RunUntilConverged(() =>
        {
            var participants = ActiveNodes.Where(n => n.CurrentView.IsHostPresent(n.Address)).ToList();
            return participants.Count > 0 && participants.TrueForAll(n => n.MembershipSize == participants.Count);
        });

        if (!outcome.ConditionMet)
        {
            var suspendedNodes = Nodes.Where(n => n.IsSuspended).ToList();
            throw new TimeoutException("Nodes did not converge. " +
                $"Active node count: {ActiveNodes.Count}, " +
                $"Active node sizes: [{string.Join(", ", ActiveNodes.Select(n => n.MembershipSize))}], " +
                $"Suspended nodes: {suspendedNodes.Count}.{Environment.NewLine}" +
                outcome.ToDetailedString());
        }
    }

    /// <summary>
    /// Logs the seed to the test output for reproduction.
    /// </summary>
    public void LogSeedForReproduction() => _logger.LogInformation("[SEED FOR REPRODUCTION] {Seed}", Seed);

    /// <summary>
    /// Asserts that no log entries at Warning level or above were recorded.
    /// Throws an exception with details of any warning/error logs found.
    /// </summary>
    public void AssertNoWarningsOrErrors()
    {
        var warningsAndErrors = LogBuffer.GetEntries(LogLevel.Warning).ToList();
        if (warningsAndErrors.Count > 0)
        {
            var formattedEntries = string.Join(Environment.NewLine,
                warningsAndErrors.Select(e => $"  [{e.LogLevel}] {e.Category}: {e.Message}"));
            throw new Xunit.Sdk.XunitException(
                $"Expected no warnings or errors, but found {warningsAndErrors.Count}:{Environment.NewLine}{formattedEntries}");
        }
    }

    /// <inheritdoc />
    protected override void OnConditionMet(int iterations) => _log.ConditionMet(iterations);

    /// <inheritdoc />
    protected override void OnSimulationIdleNoPendingWork(int iterations) => _log.SimulationIdleNoPendingWork(iterations, $"{TimeProvider.GetUtcNow():O}");

    /// <inheritdoc />
    protected override void OnSimulationStuckMaxTime(TimeSpan timeDelta)
    {
        _log.SimulationStuckMaxTime(
            $"{MaxSimulatedTimeAdvance}",
            $"{StartDateTime:O}",
            $"{TimeProvider.GetUtcNow():O}",
            $"{timeDelta}");
    }

    /// <inheritdoc />
    protected override void OnSimulationStuckConsecutiveTimeAdvances(int count) => _log.SimulationStuckConsecutiveTimeAdvances(count);

    /// <inheritdoc />
    protected override void OnMaxIterationsReached(int maxIterations)
    {
        _log.MaxIterationsReached(maxIterations);

        // Emit diagnostics at Information level so they show up
        // even when the full debug log gets truncated.
        var diagnostics = BuildMaxIterationDiagnostics(maxItemsPerQueue: 10);
        _log.MaxIterationsReachedWithDiagnostics(maxIterations, diagnostics);
    }

    private string BuildMaxIterationDiagnostics(int maxItemsPerQueue)
    {
        var sb = new System.Text.StringBuilder();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"SimulatedTime={TimeProvider.GetUtcNow():O}");

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"ClusterQueue: Items={TaskQueue.ScheduledItems.Count}, NextWaiting={TaskQueue.NextWaitingDueTime:O}");
        AppendQueueHead(sb, TaskQueue, maxItemsPerQueue);

        foreach (var node in Nodes)
        {
            var ctx = node.Context;
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Node={node.NetworkAddress} State={ctx.State} Items={ctx.TaskQueue.ScheduledItems.Count} HasReady={ctx.HasReadyTasks} NextWaiting={ctx.NextWaitingDueTime:O}");
            AppendQueueHead(sb, ctx.TaskQueue, maxItemsPerQueue);
        }

        return sb.ToString();
    }

    private static void AppendQueueHead(System.Text.StringBuilder sb, SimulationTaskQueue queue, int maxItems)
    {
        var count = 0;
        foreach (var item in queue.ScheduledItems)
        {
            if (count++ >= maxItems)
            {
                _ = sb.AppendLine("  ...");
                break;
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  {item.GetType().Name} Due={item.DueTime:O}");
        }
    }

    private sealed class SimulationRuntimeScope(IDisposable nodeScope, IDisposable runtimeScope) : IDisposable
    {
        public void Dispose()
        {
            nodeScope.Dispose();
            runtimeScope.Dispose();
        }
    }

    /// <inheritdoc />
    protected override void OnTeardownCancellationRequested() => _log.TeardownCancellationRequested();

    /// <inheritdoc />
    protected override void OnSimulationReachedIdleState() => _log.SimulationReachedIdleState();

    /// <inheritdoc />
    protected override void OnTimeAdvancing(TimeSpan delta) => _log.TimeAdvancing($"{delta}");

    /// <summary>
    /// Disposes nodes through the adaptive simulation driver before releasing the base runtime.
    /// </summary>
    public new async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        var nodes = Nodes.ToList();
        try
        {
            foreach (var node in nodes)
            {
                if (node.IsSuspended)
                {
                    node.Resume();
                }
            }

            _ = RunToCompletion(() => Task.WhenAll(nodes.Select(node => node.DisposeAsync().AsTask())));

            foreach (var node in nodes)
            {
                UnregisterNode(node);
            }
        }
        finally
        {
            await base.DisposeAsync();
        }
    }

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        // This fallback covers disposal through an IAsyncDisposable/base-typed reference.
        var nodes = Nodes.ToList();
        foreach (var node in nodes)
        {
            if (node.IsSuspended)
            {
                node.Resume();
            }
        }

        if (nodes.Count > 0)
        {
            await Task.WhenAll(nodes.Select(node => node.DisposeAsync().AsTask()));
            foreach (var node in nodes)
            {
                UnregisterNode(node);
            }
        }

        // Attach logs to test context BEFORE disposing the provider
        _logManager.AttachLogsToTestContext(TestContext.Current);

        // Dispose the log manager (disposes logger factory and provider)
        _logManager.Dispose();
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"SimulationHarness(Seed={Seed}, Nodes={Nodes.Count}, Time={Clock.CurrentTime:hh\\:mm\\:ss\\.fff})");
}
