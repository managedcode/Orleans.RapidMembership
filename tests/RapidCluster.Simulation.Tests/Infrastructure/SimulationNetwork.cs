using System.Diagnostics;
using System.Globalization;
using Clockwork;
using Microsoft.Extensions.Logging;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Simulates a network for in-memory transport between nodes in a RapidCluster simulation.
/// Extends <see cref="Clockwork.SimulationNetwork"/> with RapidCluster-specific logging.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class SimulationNetwork : Clockwork.SimulationNetwork
{
    private readonly RapidSimulationCluster _harness;
    private SimulationNetworkLogger _log;

    internal SimulationNetwork(RapidSimulationCluster harness, SimulationRandom random)
        : base(() => harness.Nodes, random)
    {
        _harness = harness;
        _log = new SimulationNetworkLogger(Microsoft.Extensions.Logging.Abstractions.NullLogger<SimulationNetwork>.Instance);
    }

    internal void SetLogger(ILogger<SimulationNetwork> logger)
    {
        _log = new SimulationNetworkLogger(logger);
        base.SetLogger(logger);
    }

    public bool EnableDelays { get; set; }

    public new TimeSpan GetMessageDelay() => EnableDelays ? base.GetMessageDelay() : TimeSpan.Zero;

    public new void IsolateNode(string nodeAddress)
    {
        OnNodeIsolating(nodeAddress);
        foreach (var node in _harness.Nodes)
        {
            var address = RapidClusterUtils.Loggable(node.Address);
            if (!string.Equals(address, nodeAddress, StringComparison.Ordinal))
            {
                CreateBidirectionalPartition(nodeAddress, address);
            }
        }
    }

    public new void ReconnectNode(string nodeAddress)
    {
        OnNodeReconnecting(nodeAddress);
        foreach (var node in _harness.Nodes)
        {
            var address = RapidClusterUtils.Loggable(node.Address);
            if (!string.Equals(address, nodeAddress, StringComparison.Ordinal))
            {
                HealBidirectionalPartition(nodeAddress, address);
            }
        }
    }

    public new bool IsNodeIsolated(string nodeAddress) =>
        _harness.Nodes
            .Select(static node => RapidClusterUtils.Loggable(node.Address))
            .Where(address => !string.Equals(address, nodeAddress, StringComparison.Ordinal))
            .All(address => !CanDeliver(nodeAddress, address));

    /// <inheritdoc />
    protected override void OnPartitionCreated(string source, string target) => _log.PartitionCreated(source, target);

    /// <inheritdoc />
    protected override void OnBidirectionalPartitionCreating(string node1, string node2) => _log.BidirectionalPartitionCreating(node1, node2);

    /// <inheritdoc />
    protected override void OnPartitionHealed(string source, string target) => _log.PartitionHealed(source, target);

    /// <inheritdoc />
    protected override void OnBidirectionalPartitionHealing(string node1, string node2) => _log.BidirectionalPartitionHealing(node1, node2);

    /// <inheritdoc />
    protected override void OnAllPartitionsHealed(int count) => _log.AllPartitionsHealed(count);

    /// <inheritdoc />
    protected override void OnNodeIsolating(string nodeAddress) => _log.NodeIsolating(nodeAddress);

    /// <inheritdoc />
    protected override void OnNodeReconnecting(string nodeAddress) => _log.NodeReconnecting(nodeAddress);

    /// <inheritdoc />
    protected override void OnMessageBlockedByPartition(string source, string target) => _log.MessageBlockedByPartition(source, target);

    /// <inheritdoc />
    protected override void OnMessageDroppedRandom(string source, string target) => _log.MessageDroppedRandom(source, target);

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"SimulationNetwork(DropRate={MessageDropRate:P0})");
}
