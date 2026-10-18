using System.Text;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class ChurnEvidence : IAsyncDisposable
{
    private readonly Lock _writeGate = new();
    private readonly StreamWriter _writer;
    private readonly ITestOutputHelper _output;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<Task> _watchers = [];

    public ChurnEvidence(ITestOutputHelper output, DistributedApplication app)
    {
        _output = output;
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "RapidCluster.slnx")))
        {
            root = root.Parent ?? throw new InvalidOperationException("Repository root was not found.");
        }

        var directory = Path.Combine(root.FullName, "artifacts", "tests", "churn", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(directory);
        _writer = new StreamWriter(Path.Combine(directory, "phases.log"), append: false, Encoding.UTF8) { AutoFlush = true };
        var logs = app.Services.GetRequiredService<ResourceLoggerService>();
        foreach (var resource in app.Services.GetRequiredService<DistributedApplicationModel>().Resources.OfType<ProjectResource>())
        {
            _watchers.Add(WatchAsync(logs, app.ResourceNotifications, resource, Path.Combine(directory, resource.Name + ".log"), _stop.Token));
        }
    }

    public void Write(string message)
    {
        lock (_writeGate)
        {
            _writer.WriteLine(message);
            _output.WriteLine(message);
        }
    }

    private static async Task WatchAsync(ResourceLoggerService logs, ResourceNotificationService notifications, ProjectResource resource, string path, CancellationToken cancellationToken)
    {
        await using var writer = new StreamWriter(path, append: false, Encoding.UTF8) { AutoFlush = true };
        var written = 0;
        while (true)
        {
            await notifications.WaitForResourceAsync(resource.Name, KnownResourceStates.Running, cancellationToken);
            await foreach (var batch in logs.WatchAsync(resource).WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();
                foreach (var line in batch)
                {
                    if (written++ == 5000)
                    {
                        await writer.FlushAsync(cancellationToken);
                        File.Copy(path, path + ".previous", overwrite: true);
                        writer.BaseStream.SetLength(0);
                        writer.BaseStream.Position = 0;
                        written = 1;
                    }

                    await writer.WriteLineAsync(line.Content.AsMemory(), cancellationToken);
                }
            }

            await Task.Delay(100, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        try
        {
            await Task.WhenAll(_watchers);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        finally
        {
            _stop.Dispose();
            await _writer.DisposeAsync();
        }
    }
}
