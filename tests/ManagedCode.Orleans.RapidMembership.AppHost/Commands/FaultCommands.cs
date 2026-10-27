using System.Diagnostics;
using System.Net.Http.Json;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using ManagedCode.Orleans.RapidMembership.Node.Contracts;

namespace ManagedCode.Orleans.RapidMembership.AppHost.Commands;

internal static class FaultCommands
{
    public static void Apply(IResourceBuilder<ProjectResource> node)
    {
        _ = node.WithHttpCommand("/shutdown", "Gracefully stop test silo", endpointName: "http", commandName: "shutdown");
        _ = node.WithCommand("crash", "Crash test silo", async context =>
        {
            using var client = new HttpClient();
            var endpoint = new Uri(new Uri(node.GetEndpoint("http").Url), "/process");
            var identity = await client.GetFromJsonAsync<NodeProcessIdentity>(endpoint, context.CancellationToken);
            if (identity is null || identity.ProcessId <= 0 || identity.StartTimeUtcTicks <= 0)
            {
                return CommandResults.Failure("Test process identity was not returned.");
            }

            using var process = Process.GetProcessById(identity.ProcessId);
            if (process.HasExited || process.StartTime.ToUniversalTime().Ticks != identity.StartTimeUtcTicks)
            {
                return CommandResults.Failure("Test process identity could not be verified.");
            }

            context.CancellationToken.ThrowIfCancellationRequested();
            process.Kill();
            await process.WaitForExitAsync(context.CancellationToken);
            return CommandResults.Success();
        });
    }
}
