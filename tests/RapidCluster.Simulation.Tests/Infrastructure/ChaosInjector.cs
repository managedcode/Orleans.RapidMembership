using Clockwork;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Injects deterministic random and scheduled faults into a RapidCluster simulation.
/// </summary>
internal sealed class ChaosInjector
{
    private readonly RapidSimulationCluster _harness;
    private readonly SimulationRandom _random;
    private readonly List<ScheduledFault> _scheduledFaults = [];
    private readonly Lock _lock = new();

    public ChaosInjector(RapidSimulationCluster harness)
    {
        _harness = harness ?? throw new ArgumentNullException(nameof(harness));
        _random = harness.ForkRandom();
    }

    public double NodeCrashRate { get; set; }

    public double PartitionRate { get; set; }

    public double PartitionHealRate { get; set; } = 0.1;

    public int MinimumAliveNodes { get; set; } = 1;

    public bool MaybeInjectFault()
    {
        ProcessScheduledFaults();

        if (NodeCrashRate > 0 && _random.Chance(NodeCrashRate) && TryCrashRandomNode())
        {
            return true;
        }

        if (PartitionRate > 0 && _random.Chance(PartitionRate) && TryCreateRandomPartition())
        {
            return true;
        }

        if (PartitionHealRate > 0 && _random.Chance(PartitionHealRate))
        {
            _harness.Network.HealAllPartitions();
        }

        return false;
    }

    public void ScheduleNodeCrash(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(new ScheduledFault(FaultType.NodeCrash, GetExecutionTime(delay), node, null));

    public void SchedulePartition(RapidSimulationNode node1, RapidSimulationNode node2, TimeSpan delay) =>
        Schedule(new ScheduledFault(FaultType.Partition, GetExecutionTime(delay), node1, node2));

    public void SchedulePartitionHeal(RapidSimulationNode node1, RapidSimulationNode node2, TimeSpan delay) =>
        Schedule(new ScheduledFault(FaultType.PartitionHeal, GetExecutionTime(delay), node1, node2));

    public void ScheduleIsolation(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(new ScheduledFault(FaultType.Isolation, GetExecutionTime(delay), node, null));

    public void ScheduleReconnect(RapidSimulationNode node, TimeSpan delay) =>
        Schedule(new ScheduledFault(FaultType.Reconnect, GetExecutionTime(delay), node, null));

    public int RunChaos(int steps, TimeSpan? stepInterval = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(steps);
        var interval = stepInterval ?? TimeSpan.FromMilliseconds(100);
        var faultsInjected = 0;
        for (var i = 0; i < steps; i++)
        {
            _ = _harness.TaskQueue.RunUntilIdle();
            if (MaybeInjectFault())
            {
                faultsInjected++;
            }

            _ = _harness.RunFor(interval);
        }

        return faultsInjected;
    }

    public void ClearScheduledFaults()
    {
        lock (_lock)
        {
            _scheduledFaults.Clear();
        }
    }

    private DateTimeOffset GetExecutionTime(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return _harness.TimeProvider.GetUtcNow() + delay;
    }

    private void Schedule(ScheduledFault fault)
    {
        ArgumentNullException.ThrowIfNull(fault.Node1);
        lock (_lock)
        {
            _scheduledFaults.Add(fault);
        }
    }

    private void ProcessScheduledFaults()
    {
        var now = _harness.TimeProvider.GetUtcNow();
        List<ScheduledFault> due;
        lock (_lock)
        {
            due = [.. _scheduledFaults.Where(fault => fault.ExecuteAt <= now)];
            foreach (var fault in due)
            {
                _scheduledFaults.Remove(fault);
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
                throw new InvalidOperationException($"Unsupported fault type '{fault.Type}'.");
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
        nodes.Remove(node1);
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
