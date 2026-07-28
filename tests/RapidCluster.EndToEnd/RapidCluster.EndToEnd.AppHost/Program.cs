using System.Globalization;

var builder = DistributedApplication.CreateBuilder(args);

const int clusterSize = 5;

// Create a unique bootstrap file path for this test run
// All nodes will use this file to coordinate bootstrap
var bootstrapFilePath = Path.Combine(Path.GetTempPath(), $"rapidcluster-e2e-{Guid.NewGuid():N}.json");

// Create individual cluster node resources instead of replicas
// This allows each node to have references to all other nodes for peer discovery
//
// Cluster traffic uses HTTPS/HTTP2. Health probes use the separate HTTP endpoint so
// CI does not need to trust each runner's ephemeral development certificate.
var nodes = new IResourceBuilder<ProjectResource>[clusterSize];
for (var i = 0; i < clusterSize; i++)
{
    nodes[i] = builder.AddProject<Projects.RapidCluster_EndToEnd_Node>(string.Create(CultureInfo.InvariantCulture, $"cluster-{i}"))
        .WithEnvironment("CLUSTER_SIZE", clusterSize.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("CLUSTER_NODE_INDEX", i.ToString(CultureInfo.InvariantCulture))
        .WithEnvironment("CLUSTER_BOOTSTRAP_FILE", bootstrapFilePath)
        .WithHttpHealthCheck("/health", endpointName: "http");
}

// Wire up each node to reference all nodes (including itself) for service discovery
foreach (var node in nodes)
{
    foreach (var peer in nodes)
    {
        node.WithReference(peer);
    }
}

builder.Build().Run();
