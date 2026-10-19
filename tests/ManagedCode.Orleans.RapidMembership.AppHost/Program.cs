using System.Security.Cryptography;
using Aspire.Hosting.ApplicationModel;
using ManagedCode.Orleans.RapidMembership.AppHost.Commands;
using Microsoft.Extensions.Configuration;

var builder = DistributedApplication.CreateBuilder(args);
var secret = builder.AddParameter("rapid-secret", () => Convert.ToHexString(RandomNumberGenerator.GetBytes(32)), secret: true);
var clusterId = Guid.NewGuid().ToString("N");
var count = builder.Configuration.GetValue("TestCluster:Nodes", 4);
var explicitStart = builder.Configuration.GetValue("TestCluster:ExplicitStart", false);
ArgumentOutOfRangeException.ThrowIfLessThan(count, 3);
var nodes = new IResourceBuilder<ProjectResource>[count];
for (var i = 0; i < nodes.Length; i++)
{
    var name = string.Create(System.Globalization.CultureInfo.InvariantCulture, $"silo-{i}");
    var node = builder.AddProject<Projects.ManagedCode_Orleans_RapidMembership_Node>(name, launchProfileName: null)
        .WithHttpEndpoint(name: "http", isProxied: false)
        .WithEndpoint(name: "membership", scheme: "http", isProxied: false)
        .WithEndpoint(name: "silo", scheme: "tcp", isProxied: false)
        .WithEndpoint(name: "gateway", scheme: "tcp", isProxied: false);
    _ = node.WithEnvironment("ASPNETCORE_URLS", node.GetEndpoint("http"))
        .WithEnvironment("CLUSTER_ID", clusterId)
        .WithEnvironment("DOTNET_PROCESSOR_COUNT", "2")
        .WithEnvironment("Logging__LogLevel__RapidCluster", "Warning")
        .WithEnvironment("Logging__LogLevel__ManagedCode.Orleans.RapidMembership.Hosting.ControllerRuntime", "Information")
        .WithEnvironment("RAPID_SECRET", secret)
        .WithEnvironment("RAPID_PORT", node.GetEndpoint("membership").Property(EndpointProperty.TargetPort))
        .WithEnvironment("SILO_PORT", node.GetEndpoint("silo").Property(EndpointProperty.TargetPort))
        .WithEnvironment("GATEWAY_PORT", node.GetEndpoint("gateway").Property(EndpointProperty.TargetPort));
    if (explicitStart || i >= 3)
    {
        _ = node.WithExplicitStart();
    }

    FaultCommands.Apply(node);
    nodes[i] = node;
}

builder.Build().Run();
