using System.Globalization;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Checks cluster invariants during simulation testing.
/// Used to detect split-brain scenarios, membership inconsistencies, and other safety violations.
/// </summary>
/// <remarks>
/// Creates a new invariant checker for the specified harness.
/// </remarks>
internal sealed class InvariantChecker(RapidSimulationCluster harness)
{
    private readonly RapidSimulationCluster _harness = harness ?? throw new ArgumentNullException(nameof(harness));
    private readonly List<InvariantViolation> _violations = [];
    private readonly Dictionary<string, long> _configurationVersions = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    /// <summary>
    /// Gets all recorded violations.
    /// </summary>
    public IReadOnlyList<InvariantViolation> Violations
    {
        get
        {
            lock (_lock)
            {
                return [.. _violations];
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether gets whether any violations have been recorded.
    /// </summary>
    public bool HasViolations
    {
        get
        {
            lock (_lock)
            {
                return _violations.Count > 0;
            }
        }
    }

    /// <summary>
    /// Checks all invariants and returns true if all pass.
    /// </summary>
    public bool CheckAll()
    {
        var passed = true;

        passed &= CheckAtMostTwoTransitionalMembershipSizes();
        passed &= CheckNoSplitBrain();
        passed &= CheckConfigurationIdMonotonicity();
        passed &= CheckNodeIdValidity();
        passed &= CheckNodeIdUniqueness();
        passed &= CheckMaxNodeIdConsistency();

        return passed;
    }

    /// <summary>
    /// Checks that an in-progress transition exposes at most two distinct membership sizes.
    /// </summary>
    public bool CheckAtMostTwoTransitionalMembershipSizes()
    {
        var nodes = _harness.Nodes;
        if (nodes.Count < 2)
        {
            return true;
        }

        // Get all membership sizes
        var sizes = nodes.Select(n => n.MembershipSize).Distinct().ToList();

        // In a stable cluster, all sizes should be the same
        // Allow for temporary inconsistency during membership changes
        if (sizes.Count > 2)
        {
            RecordViolation(
                InvariantType.TransitionalMembershipSizeBound,
                $"More than 2 distinct membership sizes: [{string.Join(", ", sizes)}]");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks for split-brain scenarios where two nodes both think they're in separate clusters.
    /// </summary>
    public bool CheckNoSplitBrain()
    {
        var nodes = _harness.Nodes;
        if (nodes.Count < 2)
        {
            return true;
        }

        // A split-brain occurs when:
        // 1. Two nodes have non-overlapping membership views
        // 2. Both nodes are initialized and think they're operational
        for (var i = 0; i < nodes.Count; i++)
        {
            var node1 = nodes[i];
            if (!node1.IsInitialized) continue;

            var members1 = node1.CurrentView?.Members ?? [];

            for (var j = i + 1; j < nodes.Count; j++)
            {
                var node2 = nodes[j];
                if (!node2.IsInitialized) continue;

                var members2 = node2.CurrentView?.Members ?? [];

                // Check if there's any overlap
                var overlap = members1.Intersect(members2, new EndpointComparer()).Any();

                if (!overlap && members1.Length > 0 && members2.Length > 0)
                {
                    RecordViolation(
                        InvariantType.NoSplitBrain,
                        "Split-brain detected: Node views have no overlap");
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that each node's observed configuration version never decreases.
    /// </summary>
    public bool CheckConfigurationIdMonotonicity()
    {
        var passed = true;
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var address = RapidClusterUtils.Loggable(node.Address);
            var version = node.CurrentView.ConfigurationId.Version;
            lock (_lock)
            {
                if (_configurationVersions.TryGetValue(address, out var previousVersion) &&
                    version < previousVersion)
                {
                    _violations.Add(new InvariantViolation(
                        _harness.TimeProvider.GetUtcNow(),
                        InvariantType.ConfigurationIdMonotonicity,
                        string.Create(
                            CultureInfo.InvariantCulture,
                            $"Node {address} configuration version decreased from {previousVersion} to {version}.")));
                    passed = false;
                }

                _configurationVersions[address] = version;
            }
        }

        return passed;
    }

    /// <summary>
    /// Checks that all endpoints in each membership view have valid monotonic node IDs (greater than 0).
    /// </summary>
    public bool CheckNodeIdValidity()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            foreach (var member in view.Members)
            {
                if (member.NodeId <= 0)
                {
                    RecordViolation(
                        InvariantType.NodeIdValidity,
                        string.Create(CultureInfo.InvariantCulture, $"Node {RapidClusterUtils.Loggable(node.Address)} has member {RapidClusterUtils.Loggable(member)} with invalid NodeId={member.NodeId}"));
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that monotonic node IDs are unique within each membership view.
    /// </summary>
    public bool CheckNodeIdUniqueness()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            var nodeIds = view.Members.Select(m => m.NodeId).ToList();
            var uniqueIds = nodeIds.Distinct().ToList();

            if (nodeIds.Count != uniqueIds.Count)
            {
                var duplicates = nodeIds.GroupBy(id => id)
                    .Where(g => g.Skip(1).Any())
                    .Select(g => g.Key);
                RecordViolation(
                    InvariantType.NodeIdUniqueness,
                    $"Node {RapidClusterUtils.Loggable(node.Address)} has duplicate NodeIds: [{string.Join(", ", duplicates)}]");
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that MaxNodeId in each view is greater than or equal to the maximum NodeId of all members.
    /// </summary>
    public bool CheckMaxNodeIdConsistency()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            var maxMemberId = view.Members.Length > 0 ? view.Members.Max(m => m.NodeId) : 0;

            if (view.MaxNodeId < maxMemberId)
            {
                RecordViolation(
                    InvariantType.MaxNodeIdConsistency,
                    string.Create(CultureInfo.InvariantCulture, $"Node {RapidClusterUtils.Loggable(node.Address)} has MaxNodeId={view.MaxNodeId} but has member with NodeId={maxMemberId}"));
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Verifies that the cluster can make progress (liveness check).
    /// This is a soft check - it doesn't guarantee liveness but can detect obvious deadlocks.
    /// </summary>
    public bool CheckLiveness(int maxSteps = 1000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSteps);
        var initialNodeWork = _harness.Nodes.Sum(static node => node.Context.TaskQueue.ScheduledItems.Count);
        if (initialNodeWork == 0)
        {
            RecordViolation(
                InvariantType.Liveness,
                "No node work was pending, so node progress could not be demonstrated.");
            return false;
        }

        var outcome = _harness.RunUntil(
            () => _harness.Nodes.Sum(static node => node.Context.TaskQueue.ScheduledItems.Count) < initialNodeWork,
            maxSteps);
        if (outcome.ConditionMet)
        {
            return true;
        }

        RecordViolation(
            InvariantType.Liveness,
            string.Create(
                CultureInfo.InvariantCulture,
                $"No node-queue progress was made from {initialNodeWork} pending items.{Environment.NewLine}{outcome.ToDetailedString()}"));
        return false;
    }

    /// <summary>
    /// Clears all recorded violations.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _violations.Clear();
            _configurationVersions.Clear();
        }
    }

    private void RecordViolation(InvariantType type, string message)
    {
        var violation = new InvariantViolation(
            _harness.TimeProvider.GetUtcNow(),
            type,
            message);

        lock (_lock)
        {
            _violations.Add(violation);
        }
    }

    private sealed class EndpointComparer : IEqualityComparer<Pb.Endpoint>
    {
        public bool Equals(Pb.Endpoint? x, Pb.Endpoint? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x is null || y is null) return false;
            return x.Hostname == y.Hostname && x.Port == y.Port;
        }

        public int GetHashCode(Pb.Endpoint obj) => HashCode.Combine(obj.Hostname, obj.Port);
    }
}

/// <summary>
/// Types of invariants that can be checked.
/// </summary>
internal enum InvariantType
{
    TransitionalMembershipSizeBound,
    NoSplitBrain,
    ConfigurationIdMonotonicity,
    Liveness,
    ConsensusSafety,
    NodeIdValidity,
    NodeIdUniqueness,
    MaxNodeIdConsistency,
}

/// <summary>
/// Represents a detected invariant violation.
/// </summary>
internal readonly record struct InvariantViolation(
    DateTimeOffset SimulatedTime,
    InvariantType Type,
    string Message);
