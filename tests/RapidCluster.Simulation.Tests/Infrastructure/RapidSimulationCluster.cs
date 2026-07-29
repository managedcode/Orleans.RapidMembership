using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Clockwork;
using Clockwork.Runtime.Random;
using Clockwork.Runtime.Scheduling;
using Clockwork.Testing;
using Microsoft.Extensions.Logging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// <para>
/// Unified simulation harness for fully deterministic testing of Rapid clusters.
/// Composes <see cref="SimulationCluster"/> with RapidCluster-specific functionality.
/// </para>
/// <para>
/// Provides:
/// - Deterministic task scheduling via per-node <see cref="SimulationTaskScheduler"/> instances
/// - Controlled time via the shared <see cref="SimulationScheduler"/>
/// - Seeded random number generation via <see cref="SimulationRandom"/>
/// - Simulated network with partition injection via <see cref="SimulationNetwork"/>
/// - Node lifecycle management (create, join, crash, leave)
/// - Per-node execution control (suspend, resume, step)
/// - Simulation driving APIs (Step, RunUntil, Run).
/// </para>
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class RapidSimulationCluster : IAsyncDisposable
{
    internal static readonly DateTimeOffset DefaultStartTime = DateTimeOffset.UnixEpoch;

    private readonly SimulationCluster _cluster;
    private readonly List<RapidSimulationNode> _nodes = [];
    private readonly HashSet<string> _attachedAddresses = new(StringComparer.Ordinal);
    private readonly SimulationLogManager _logManager;
    private readonly SimulationHarnessLogger _log;
    private int _nextAttachmentId;
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
    {
        _cluster = new SimulationCluster(
            seed,
            startTime ?? DefaultStartTime,
            cancellationToken: TestContext.Current.CancellationToken);
        Network = new SimulationNetwork(
            this,
            new SimulationRandom(SeedAuthority.GetDomainSeed(SimulationSeedDomain.Network)));

        // Create log manager for logger factory and log attachment
        _logManager = new SimulationLogManager(TimeProvider, seed);
        LoggerFactory = _logManager.LoggerFactory;

        _log = new SimulationHarnessLogger(LoggerFactory.CreateLogger<RapidSimulationCluster>());
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

    internal SimulationCluster Cluster => _cluster;

    public int Seed => _cluster.Seed;

    public SimulationSeedAuthority SeedAuthority => _cluster.SeedAuthority;

    public SimulationRandom Random => _cluster.Random;

    public TimeProvider TimeProvider => _cluster.TimeProvider;

    public DateTimeOffset StartDateTime => _cluster.StartDateTime;

    public SimulationScheduler Scheduler => _cluster.Scheduler;

    public SimulationSchedulerLane TaskQueue => _cluster.SchedulerLane;

    public SimulationTaskScheduler TaskScheduler => _cluster.TaskScheduler;

    public SingleThreadedGuard Guard => _cluster.Guard;

    public IReadOnlyList<RapidSimulationNode> Nodes => [.. _nodes];

    public IReadOnlyList<RapidSimulationNode> ActiveNodes => [.. _nodes.Where(static node => !node.IsSuspended)];

    public TimeSpan MaxSimulatedTimeAdvance
    {
        get => _cluster.MaxSimulatedTimeAdvance;
        set => _cluster.MaxSimulatedTimeAdvance = value;
    }

    /// <summary>
    /// Gets the simulation network.
    /// </summary>
    public SimulationNetwork Network { get; }

    [SuppressMessage("Reliability", "CA2025:Ensure tasks using IDisposable instances complete before the instances are disposed",
        Justification = "The caller immediately drives the returned task to completion through the simulation scheduler.")]
    internal Task RunInNodeContext(Endpoint address, Func<Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var node = FindNode(RapidClusterUtils.Loggable(address))
            ?? throw new InvalidOperationException($"Node '{RapidClusterUtils.Loggable(address)}' is not registered.");
        Task<Task> task;
        using (ExecutionContext.SuppressFlow())
        {
            task = new Task<Task>(action);
        }

        task.Start(node.Context.TaskScheduler);
        return task.Unwrap();
    }

    internal ValueTask RunInNodeContext(Endpoint address, Func<ValueTask> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return new ValueTask(RunInNodeContext(address, new Func<Task>(() => action().AsTask())));
    }

    /// <summary>
    /// Executes a synchronous boundary call within the specified node's simulation runtime.
    /// </summary>
    public void RunOnNode(RapidSimulationNode node, Action action)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(action);
        var task = ScheduleOnNode(node, action);
        RunToCompletion(async () => await task);
    }

    private static Task ScheduleOnNode(RapidSimulationNode node, Action action)
    {
        Task task;
        using (ExecutionContext.SuppressFlow())
        {
            task = new Task(action);
        }

        task.Start(node.Context.TaskScheduler);
        return task;
    }

    /// <summary>
    /// Invokes a synchronous boundary call within the specified node's simulation runtime.
    /// </summary>
    public T RunOnNode<T>(RapidSimulationNode node, Func<T> action)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(action);
        T? result = default;
        RunOnNode(node, new Action(() => result = action()));
        return result!;
    }

    /// <summary>
    /// Finds a node by its network address.
    /// </summary>
    /// <param name="address">The network address of the node.</param>
    /// <returns>The node with the specified address, or null if not found.</returns>
    public RapidSimulationNode? FindNode(string address) =>
        _nodes.FirstOrDefault(node => string.Equals(
            RapidClusterUtils.Loggable(node.Address),
            address,
            StringComparison.Ordinal));

    public SimulationNodeContext GetNodeContext(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        return _nodes.Contains(node)
            ? node.Context
            : throw new InvalidOperationException($"Node '{node.NetworkAddress}' is not registered with this simulation.");
    }

    public SimulationRandom ForkRandom() => _cluster.ForkRandom();

    public SimulationExecutionResult RunUntil(Func<bool> condition, int maxIterations = 100_000) =>
        _cluster.RunUntil(condition, maxIterations);

    public SimulationExecutionResult RunUntil(Func<bool> condition, AdaptiveExecutionBudget budget) =>
        _cluster.RunUntil(condition, budget);

    public SimulationExecutionResult RunUntilIdle(TimeSpan? maxTimeAdvance = null, int maxIterations = 100_000) =>
        maxTimeAdvance is { } duration
            ? _cluster.RunFor(duration, maxIterations)
            : _cluster.RunUntilIdle(maxIterations: maxIterations);

    public SimulationExecutionResult RunUntilIdle(AdaptiveExecutionBudget budget, TimeSpan? maxTimeAdvance = null) =>
        maxTimeAdvance is { } duration
            ? _cluster.RunFor(duration, budget.MaxTotalIterations)
            : _cluster.RunUntilIdle(budget);

    public void RunToCompletion(Func<Task> taskFactory, int maxIterations = 1_000_000) =>
        _cluster.RunToCompletion(taskFactory, maxIterations);

    public void RunToCompletion(Func<Task> taskFactory, AdaptiveExecutionBudget budget) =>
        _cluster.RunToCompletion(taskFactory, budget);

    public T RunToCompletion<T>(Func<Task<T>> taskFactory, int maxIterations = 1_000_000) =>
        _cluster.RunToCompletion(taskFactory, maxIterations);

    public T RunToCompletion<T>(Func<Task<T>> taskFactory, AdaptiveExecutionBudget budget) =>
        _cluster.RunToCompletion(taskFactory, budget);

    public SimulationExecutionResult RunFor(TimeSpan duration, int maxIterations = 100_000) =>
        _cluster.RunFor(duration, maxIterations);

    private RapidSimulationNode CreateNode(
        Endpoint address,
        Endpoint? seedAddress,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        bool initialize = true) =>
        CreateNode(
            address,
            seedAddress is null ? null : [seedAddress],
            metadata,
            protocolOptions,
            failureDetectorOptions,
            initialize);

    private RapidSimulationNode CreateNode(
        Endpoint address,
        IReadOnlyList<Endpoint>? seedAddresses,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        bool initialize = true)
    {
        var networkAddress = RapidClusterUtils.Loggable(address);
        var attachmentAddress = _attachedAddresses.Add(networkAddress)
            ? networkAddress
            : string.Create(CultureInfo.InvariantCulture, $"{networkAddress}#{_nextAttachmentId++}");
        var node = _cluster.AddCustomNode(
            attachmentAddress,
            context => new RapidSimulationNode(
                this,
                context,
                attachmentAddress,
                address,
                seedAddresses,
                metadata,
                protocolOptions,
                failureDetectorOptions,
                LoggerFactory));
        _nodes.Add(node);
        if (initialize)
        {
            RunOnNode(node, node.Initialize);
        }

        return node;
    }

    private void UnregisterNode(RapidSimulationNode node)
    {
        if (_nodes.Remove(node) && !node.IsSuspended)
        {
            node.Suspend();
        }
    }

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
        var node = CreateNode(address, seed?.Address, metadata: null, options, failureDetectorOptions: null);
        _log.UninitializedNodeCreated(nodeId);
        return node;
    }

    /// <summary>
    /// Starts a new cluster with a seed node.
    /// </summary>
    public RapidSimulationNode StartCluster(int nodeId = 0, RapidClusterProtocolOptions? options = null, PingPongFailureDetectorOptions? failureDetectorOptions = null)
    {
        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = CreateNode(address, seedAddress: null, metadata: null, options, failureDetectorOptions);

        RunToCompletion(() => node.StartAsync(TestContext.Current.CancellationToken));
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
        if (!seed.IsInitialized)
        {
            throw new InvalidOperationException($"Seed node '{seed.NetworkAddress}' is not initialized.");
        }

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

        if (seeds.Any(static seed => !seed.IsInitialized))
        {
            throw new InvalidOperationException("All seed nodes must be initialized.");
        }

        if (seeds.Select(static seed => RapidClusterUtils.Loggable(seed.Address)).Distinct(StringComparer.Ordinal).Count() != seeds.Count)
        {
            throw new ArgumentException("Seed nodes must be unique.", nameof(seeds));
        }

        return JoinNode(nodeId, seeds.Select(static seed => seed.Address).ToList(), options, failureDetectorOptions);
    }

    /// <summary>
    /// Creates and joins a new node to the cluster with raw seed addresses.
    /// Useful for validating raw seed lists and other edge cases.
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
        var node = CreateNode(address, seedAddresses, metadata: null, options, failureDetectorOptions);

        _log.NodeJoining(nodeId);

        RunToCompletion(() => node.StartAsync(TestContext.Current.CancellationToken));
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
                var node = CreateNode(
                    address,
                    seedNode.Address,
                    metadata: null,
                    options,
                    failureDetectorOptions,
                    initialize: false);

                _log.NodeJoiningParallel(RapidClusterUtils.Loggable(node.Address));

                batchNodes.Add(node);
            }

            RunToCompletion(
                () => Task.WhenAll(batchNodes.Select(async node =>
                {
                    await ScheduleOnNode(node, node.Initialize);
                    await node.StartAsync(TestContext.Current.CancellationToken);
                })));

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
        RunToCompletion(() => node.DisposeAsync().AsTask());

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

        var remainingNodes = Nodes.Where(candidate => candidate != node).ToList();

        // Drive the stop operation to completion (sends LeaveMessages to observers)
        // Node can still receive messages after this
        RunToCompletion(() => node.StopAsync(TestContext.Current.CancellationToken));
        node.CompleteStop();

        // The leaving node must remain active to participate in consensus.
        // Run the simulation until active remaining nodes agree that this node left.
        WaitForStableMembership(remainingNodes, [node.Address]);

        // Dispose the node's resources while it's still registered.
        // This ensures the simulation can drive the node's task queue during disposal.
        RunToCompletion(() => node.DisposeAsync().AsTask());

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
        _log.NodesLeavingParallel(nodesToRemove.Count);

        foreach (var node in nodesToRemove)
        {
            _log.NodeLeavingParallel(RapidClusterUtils.Loggable(node.Address));
        }

        RunToCompletion(() =>
        {
            var leaveTasks = nodesToRemove.Select(node => node.StopAsync(TestContext.Current.CancellationToken));
            return Task.WhenAll(leaveTasks);
        });
        foreach (var node in nodesToRemove)
        {
            node.CompleteStop();
        }

        WaitForStableMembership(
            remainingNodes,
            nodesToRemove.Select(static node => node.Address).ToList());

        // Dispose all leaving nodes' resources while they're still registered.
        // This ensures the simulation can drive each node's task queue during disposal.
        RunToCompletion(() =>
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
        AdaptiveExecutionBudget? budget = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(expectedSize);
        var candidates = nodes?.ToList() ?? Nodes.ToList();
        var participants = candidates
            .Where(static node => !node.IsSuspended && node.CurrentView.IsHostPresent(node.Address))
            .ToList();
        var outcome = RunUntil(
            () => participants.Count > 0 &&
                participants.TrueForAll(node => node.MembershipSize == expectedSize),
            budget ?? AdaptiveExecutionBudget.Default);
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
    /// Waits for all active cluster members to converge to a consistent view.
    /// Suspended nodes and nodes whose current view no longer contains themselves do not
    /// participate in convergence.
    /// </summary>
    public void WaitForConvergence()
    {
        var outcome = RunUntil(() =>
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

    private void WaitForStableMembership(
        IEnumerable<RapidSimulationNode> nodes,
        IReadOnlyList<Endpoint> departedNodes)
    {
        var candidates = nodes.ToList();
        var outcome = RunUntil(() =>
        {
            var participants = candidates
                .Where(static node => !node.IsSuspended && node.CurrentView.IsHostPresent(node.Address))
                .ToList();
            if (participants.Count == 0)
            {
                return false;
            }

            var membershipSize = participants[0].MembershipSize;
            return participants.TrueForAll(node =>
                node.MembershipSize == membershipSize &&
                departedNodes.All(departed => !node.CurrentView.IsHostPresent(departed)));
        });

        if (!outcome.ConditionMet)
        {
            throw new TimeoutException(
                $"Active remaining nodes did not reach a stable membership after departure.{Environment.NewLine}" +
                outcome.ToDetailedString());
        }
    }

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

    /// <summary>
    /// Disposes the simulation runtime and attaches captured logs.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            foreach (var node in _nodes)
            {
                if (node.IsSuspended)
                {
                    node.Resume();
                }
            }

            RunToCompletion(() => Task.WhenAll(_nodes.Select(node => node.DisposeAsync().AsTask())));
            try
            {
                await _cluster.DisposeAsync();
            }
            catch (AggregateException exception) when (IsAttachmentBookkeepingTimeout(exception))
            {
                // Node resources and scheduler teardown completed; only Clockwork's stale shared-lane
                // attachment bookkeeping remained after a suspended node was resumed for disposal.
            }

            _nodes.Clear();
        }
        finally
        {
            _logManager.AttachLogsToTestContext(TestContext.Current);
            _logManager.Dispose();
        }
    }

    private static bool IsAttachmentBookkeepingTimeout(AggregateException exception) =>
        exception.Flatten().InnerExceptions.All(static inner =>
            inner is TimeoutException &&
            inner.Message.StartsWith(
                "Node attachment cleanup did not reach quiescence.",
                StringComparison.Ordinal));

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"SimulationHarness(Seed={Seed}, Nodes={Nodes.Count}, Time={TimeProvider.GetUtcNow():O})");
}
