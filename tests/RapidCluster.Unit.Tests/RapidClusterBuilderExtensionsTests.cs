using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RapidCluster.Monitoring;

namespace RapidCluster.Unit.Tests;

public sealed class RapidClusterBuilderExtensionsTests
{
    [Fact]
    public void Configure_options_applies_values()
    {
        var services = new ServiceCollection();
        services
            .AddRapidClusterManual()
            .ConfigureProtocol(options => options.BatchingWindow = TimeSpan.FromSeconds(2))
            .ConfigureFailureDetector(options => options.Interval = TimeSpan.FromSeconds(3));

        using var serviceProvider = services.BuildServiceProvider();

        Assert.Equal(
            TimeSpan.FromSeconds(2),
            serviceProvider.GetRequiredService<IOptions<RapidClusterProtocolOptions>>().Value.BatchingWindow);
        Assert.Equal(
            TimeSpan.FromSeconds(3),
            serviceProvider.GetRequiredService<IOptions<PingPongFailureDetectorOptions>>().Value.Interval);
    }
}
