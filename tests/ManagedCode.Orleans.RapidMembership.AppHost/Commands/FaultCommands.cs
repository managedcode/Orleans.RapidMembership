using System.Diagnostics;
using System.Globalization;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;

namespace ManagedCode.Orleans.RapidMembership.AppHost.Commands;

internal static class FaultCommands
{
    public static void Apply(IResourceBuilder<ProjectResource> node)
    {
        _ = node.WithHttpCommand("/shutdown", "Gracefully stop test silo", endpointName: "http", commandName: "shutdown");
        _ = node.WithCommand("crash", "Crash test silo", async context =>
        {
            var notifications = context.Services.GetRequiredService<ResourceNotificationService>();
            if (!notifications.TryGetCurrentState(node.Resource.Name, out var current))
            {
                return CommandResults.Failure("Test resource state was not found.");
            }

            var snapshot = current.Snapshot;
            var pid = Convert.ToInt32(snapshot.Properties.Single(item => item.Name == "executable.pid").Value, CultureInfo.InvariantCulture);
            using var process = Process.GetProcessById(pid);
            if (process.HasExited || snapshot.StartTimeStamp is null || process.StartTime.ToUniversalTime() < snapshot.StartTimeStamp.Value.AddMinutes(-1))
            {
                return CommandResults.Failure("Test process identity could not be verified.");
            }

            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(context.CancellationToken);
            return CommandResults.Success();
        });
    }
}
