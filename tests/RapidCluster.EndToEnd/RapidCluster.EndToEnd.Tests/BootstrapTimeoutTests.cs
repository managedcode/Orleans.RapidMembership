using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.EndToEnd.Node;

namespace RapidCluster.EndToEnd.Tests;

public sealed class BootstrapTimeoutTests
{
    [Fact]
    public async Task AnUnavailableBootstrapCoordinatorDoesNotCreateAnIndependentCluster()
    {
        var directory = Path.Combine(Environment.CurrentDirectory, "artifacts", "bootstrap-tests", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(directory);
        try
        {
            using var provider = new FileBasedBootstrapProvider(Path.Combine(directory, "bootstrap.json"), 1,
                new StaticListenAddressProvider(new IPEndPoint(IPAddress.Loopback, 11112)), NullLogger<FileBasedBootstrapProvider>.Instance,
                pollInterval: TimeSpan.FromMilliseconds(5), startupTimeout: TimeSpan.FromMilliseconds(20));
            _ = await Assert.ThrowsAsync<TimeoutException>(async () =>
                _ = await provider.GetSeedsAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
