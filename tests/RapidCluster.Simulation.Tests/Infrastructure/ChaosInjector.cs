using Clockwork;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Injects deterministic random and scheduled faults into a RapidCluster simulation.
/// </summary>
internal sealed class ChaosInjector
{
    private readonly RapidSimulationCluster _harness;
    private readonly Lock _lock = new();
    private readonly SimulationRandom _random;
    private readonly List<ScheduledFault> _scheduledFaults = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="ChaosInjector"/> class.
    /// </summary>
    internal ChaosInjector(RapidSimulationCluster harness)
    {
        ArgumentNullException.ThrowIfNull(harness);
        _harness = harness;
        _random = harness.ForkRandom();
    }

    /// <summary>
    /// Gets or sets the probability of a random node crash per step.
    /// </summary>
    internal double NodeCrashRate { get; set; }

    /// <summary>
    /// Gets or sets the probability of a random partition per step.
    /// </summary>
    internal double PartitionRate { get; set; }

    /// <summary>
    /// Gets or sets the probability of healing all partitions per step.
    /// </summary>
    internal double PartitionHealRate { get; set; } = 0.1;

    /// <summary>
    /// Gets or sets the minimum number of nodes to keep active.
    /// </summary>
    internal int MinimumAliveNodes { get; set; } = 1;

    /// <summary>
    /// Processes scheduled faults and possibly injects one random fault.
    /// </summary>
    internal bool MaybeInjectFault()
    {
        ProcessScheduledFaults();

        if (NodeCrashRate > 0 &&
            _random.Chance(NodeCrashRate) &&
            TryCrashRandomNode())
        {
            return true;
        }

        if (PartitionRate > 0 &&
            _random.Chance(PartitionRate) &&
            TryCreateRandomPartition())
        {
            return true;
        }

        if (PartitionHealRate > 0 &&
            _random.Chance(PartitionHealRate))
        {
            _harness.Network.HealAllPartitions();
        }

        return false;
    }

    /// <summary>
    /// Schedules a node crash.
    /// </summary>
    internal void ScheduleNodeCrash(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(FaultType.NodeCrash, node, node2: null, delay);

    /// <summary>
    /// Schedules a network partition.
    /// </summary>
    internal void SchedulePartition(
        RapidSimulationNode node1,
        RapidSimulationNode node2,
        TimeSpan delay) =>
        Schedule(FaultType.Partition, node1, node2, delay);

    /// <summary>
    /// Schedules healing a network partition.
    /// </summary>
    internal void SchedulePartitionHeal(
        RapidSimulationNode node1,
        RapidSimulationNode node2,
        TimeSpan delay) =>
        Schedule(FaultType.PartitionHeal, node1, node2, delay);

    /// <summary>
    /// Schedules node isolation.
    /// </summary>
    internal void ScheduleIsolation(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(FaultType.Isolation, node, node2: null, delay);

    /// <summary>
    /// Schedules node reconnection.
    /// </summary>
    internal void ScheduleReconnect(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(FaultType.Reconnect, node, node2: null, delay);

    /// <summary>
    /// Runs chaos for a fixed number of simulation steps.
    /// </summary>
    internal int RunChaos(int steps, TimeSpan? stepInterval = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(steps);
        var interval = stepInterval ?? TimeSpan.FromMilliseconds(100);
        var faultsInjected = 0;
        for (var i = 0; i < steps; i++)
        {
            _ = _harness.TaskQueue.RunUntilIdle(
                TestContext.Current.CancellationToken);
            if (MaybeInjectFault())
            {
                faultsInjected++;
            }

            _ = _harness.RunFor(interval);
        }

        return faultsInjected;
    }

    /// <summary>
    /// Clears all pending scheduled faults.
    /// </summary>
    internal void ClearScheduledFaults()
    {
        lock (_lock)
        {
            _scheduledFaults.Clear();
        }
    }

    private void Schedule(
        FaultType type,
        RapidSimulationNode node1,
        RapidSimulationNode? node2,
        TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        lock (_lock)
        {
            _scheduledFaults.Add(
                new ScheduledFault(
                    type,
                    _harness.TimeProvider.GetUtcNow() + delay,
                    node1,
                    node2));
        }
    }

    private void ProcessScheduledFaults()
    {
        List<ScheduledFault> due;
        lock (_lock)
        {
            var now = _harness.TimeProvider.GetUtcNow();
            due = [.. _scheduledFaults.Where(fault => fault.ExecuteAt <= now)];
            foreach (var fault in due)
            {
                _ = _scheduledFaults.Remove(fault);
            }
        }

        foreach (var fault in due)
        {
            ExecuteFault(fault);
        }
    }

    private void ExecuteFault(ScheduledFault fault)
    {
        switch (fault.Type)
        {
            case FaultType.NodeCrash:
                if (_harness.Nodes.Contains(fault.Node1))
                {
                    _harness.CrashNode(fault.Node1);
                }

                break;
            case FaultType.Partition:
                if (fault.Node2 is not null)
                {
                    _harness.PartitionNodes(fault.Node1, fault.Node2);
                }

                break;
            case FaultType.PartitionHeal:
                if (fault.Node2 is not null)
                {
                    _harness.HealPartition(fault.Node1, fault.Node2);
                }

                break;
            case FaultType.Isolation:
                if (_harness.Nodes.Contains(fault.Node1))
                {
                    _harness.IsolateNode(fault.Node1);
                }

                break;
            case FaultType.Reconnect:
                if (_harness.Nodes.Contains(fault.Node1))
                {
                    _harness.ReconnectNode(fault.Node1);
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(fault),
                    fault.Type,
                    "Unknown fault type.");
        }
    }

    private bool TryCrashRandomNode()
    {
        var nodes = _harness.Nodes.ToList();
        if (nodes.Count <= MinimumAliveNodes)
        {
            return false;
        }

        _harness.CrashNode(_random.Choose(nodes));
        return true;
    }

    private bool TryCreateRandomPartition()
    {
        var nodes = _harness.Nodes.ToList();
        if (nodes.Count < 2)
        {
            return false;
        }

        var node1 = _random.Choose(nodes);
        _ = nodes.Remove(node1);
        var node2 = _random.Choose(nodes);
        _harness.PartitionNodes(node1, node2);
        return true;
    }

    private enum FaultType
    {
        NodeCrash,
        Partition,
        PartitionHeal,
        Isolation,
        Reconnect,
    }

    private readonly record struct ScheduledFault(
        FaultType Type,
        DateTimeOffset ExecuteAt,
        RapidSimulationNode Node1,
        RapidSimulationNode? Node2);
}
