using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using ManagedCode.Orleans.RapidMembership.Node.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class ContinuousClients(Action<string>? report = null) : IAsyncDisposable
{
    private readonly List<IHost> _hosts = [];
    private readonly List<Task> _workers = [];
    private readonly CancellationTokenSource _stop = new();
    private readonly long[] _successes = new long[4];
    private readonly ConcurrentDictionary<string, byte> _silos = new(StringComparer.Ordinal);

    private long _failures;

    public long[] Successes => [.. Enumerable.Range(0, _successes.Length).Select(index => Interlocked.Read(ref _successes[index]))];

    public long Failures => Interlocked.Read(ref _failures);

    public string[] Silos => [.. _silos.Keys.Order(StringComparer.Ordinal)];

    public async Task StartAsync(IReadOnlyDictionary<string, string> environment, CancellationToken cancellationToken)
    {
        for (var i = 0; i < _successes.Length; i++)
        {
            var builder = Host.CreateApplicationBuilder();
            _ = builder.Logging.SetMinimumLevel(LogLevel.Warning);
            _ = builder.UseOrleansClient(client =>
            {
                _ = client.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = environment["CLUSTER_ID"];
                    options.ServiceId = "rapid-membership-tests";
                });
                _ = client.Configure<ClientMessagingOptions>(options => options.ResponseTimeout = TimeSpan.FromSeconds(5));
                _ = client.UseRapidMembership(options =>
                {
                    options.SharedSecret = environment["RAPID_SECRET"];
                    options.OperationTimeout = TimeSpan.FromMinutes(2);
                    options.PeerTimeout = TimeSpan.FromSeconds(5);
                });
            });
            var host = builder.Build();
            _hosts.Add(host);
            await host.StartAsync(cancellationToken);
            _workers.Add(RunAsync(host.Services.GetRequiredService<IClusterClient>(), i, _stop.Token));
        }

        if (report is not null)
        {
            _workers.Add(ReportAsync(report, _stop.Token));
        }
    }

    private async Task ReportAsync(Action<string> write, CancellationToken cancellationToken)
    {
        while (true)
        {
            write(JsonSerializer.Serialize(new { Timestamp = DateTimeOffset.UtcNow, Successes, Failures, ExecutingSilos = Silos }));
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
        }
    }

    private async Task RunAsync(IClusterClient client, int index, CancellationToken cancellationToken)
    {
        long sequence = 0;
        var prior = new Dictionary<string, ProbeReply>(StringComparer.Ordinal);
        while (!cancellationToken.IsCancellationRequested)
        {
            var value = string.Create(CultureInfo.InvariantCulture, $"client-{index}:{sequence++}:{Guid.NewGuid():N}");
            var key = string.Create(CultureInfo.InvariantCulture, $"client-{index}:grain-{sequence / 2 % 8192}");
            ProbeReply response;
            try
            {
                response = await client.GetGrain<IProbeGrain>(key).Observe(value).WaitAsync(cancellationToken);
            }
            catch (Exception exception) when (exception is OrleansException or TimeoutException)
            {
                _ = Interlocked.Increment(ref _failures);
                await Task.Delay(25, cancellationToken);
                continue;
            }

            Assert.Equal(value, response.Value);
            Assert.False(string.IsNullOrWhiteSpace(response.Silo));
            Assert.NotEqual(Guid.Empty, response.Activation);
            Assert.True(response.Sequence > 0);
            if (prior.TryGetValue(key, out var previous) && previous.Activation == response.Activation)
            {
                Assert.True(response.Sequence > previous.Sequence);
            }

            prior[key] = response;
            _ = _silos.TryAdd(response.Silo, 0);
            _ = Interlocked.Increment(ref _successes[index]);
            await Task.Delay(25, cancellationToken);
        }
    }

    public async Task RequireProgressAsync(CancellationToken cancellationToken)
    {
        var before = Successes;
        while (!Successes.Zip(before).All(pair => pair.First >= pair.Second + 10))
        {
            foreach (var worker in _workers.Where(worker => worker.IsFaulted))
            {
                await worker;
            }

            await Task.Delay(100, cancellationToken);
        }
    }

    public Task<string[][]> RequireHostsAsync(string[] expected, CancellationToken cancellationToken)
        => Task.WhenAll(_hosts.Select(host => ReadHostsAsync(host.Services.GetRequiredService<IClusterClient>(), expected, cancellationToken)));

    private static async Task<string[]> ReadHostsAsync(IClusterClient client, string[] expected, CancellationToken cancellationToken)
    {
        while (true)
        {
            Dictionary<SiloAddress, SiloStatus> hosts;
            try
            {
                hosts = await client.GetGrain<IManagementGrain>(0).GetHosts(true, cancellationToken);
            }
            catch (Exception exception) when (exception is OrleansException or TimeoutException)
            {
                await Task.Delay(200, cancellationToken);
                continue;
            }

            Assert.All(hosts.Values, status => Assert.Equal(SiloStatus.Active, status));
            var active = hosts.Keys.Select(address => address.ToString()).Order(StringComparer.Ordinal).ToArray();
            if (expected.SequenceEqual(active, StringComparer.Ordinal))
            {
                return active;
            }

            await Task.Delay(200, cancellationToken);
        }
    }

    public Task RequireUnavailableManagementAsync(CancellationToken cancellationToken)
        => Task.WhenAll(_hosts.Select(async host =>
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(TimeSpan.FromSeconds(10));
            Exception? rejected = null;
            try
            {
                _ = await host.Services.GetRequiredService<IClusterClient>().GetGrain<IManagementGrain>(0).GetHosts(true, deadline.Token);
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested
                && exception is OrleansException or TimeoutException or OperationCanceledException)
            {
                rejected = exception;
            }

            Assert.NotNull(rejected);
        }));

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        try
        {
            await Task.WhenAll(_workers);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        finally
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try
            {
                await Task.WhenAll(_hosts.Select(host => host.StopAsync(timeout.Token)));
            }
            finally
            {
                foreach (var host in _hosts)
                {
                    host.Dispose();
                }

                _stop.Dispose();
            }
        }
    }
}
