using System.Globalization;
using System.Net.Http.Json;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class AspireChurnCluster(DistributedApplication app, Action<string> report)
{
    public async Task ChangeAsync(IEnumerable<int> indices, string command, CancellationToken cancellationToken)
    {
        var commands = app.Services.GetRequiredService<ResourceCommandService>();
        await Parallel.ForEachAsync(indices, new ParallelOptions { MaxDegreeOfParallelism = 5, CancellationToken = cancellationToken },
            async (index, token) =>
            {
                var result = await commands.ExecuteCommandAsync(Name(index), command, token);
                Assert.True(result.Success, $"{command} failed for {Name(index)}");
                if (command == "resource-start")
                {
                    await app.ResourceNotifications.WaitForResourceAsync(Name(index), KnownResourceStates.Running, token);
                }

                if (command == "shutdown")
                {
                    while (!app.ResourceNotifications.TryGetCurrentState(Name(index), out var state)
                        || state.Snapshot.State?.Text is not ("Exited" or "Finished"))
                    {
                        await Task.Delay(100, token);
                    }

                    Assert.True(app.ResourceNotifications.TryGetCurrentState(Name(index), out var exited));
                    Assert.Equal(0, exited.Snapshot.ExitCode);
                }
            });
    }

    public async Task<string[]> ConvergeAsync(int[] live, CancellationToken cancellationToken, bool requireVoterView = true)
    {
        var observations = new ParallelOptions { MaxDegreeOfParallelism = 5, CancellationToken = cancellationToken };
        var expected = new string[live.Length];
        await Parallel.ForEachAsync(Enumerable.Range(0, live.Length), observations, async (slot, token) =>
        {
            var index = live[slot];
            report(FormattableString.Invariant($"{Name(index)} reading native identity for {live.Length}-silo convergence"));
            expected[slot] = (await ReadAsync(index, token)).Self;
        });

        Array.Sort(expected, StringComparer.Ordinal);
        await Parallel.ForEachAsync(live, observations, async (index, token) =>
        {
            var polls = 0;
            while (true)
            {
                var state = await ReadAsync(index, token);
                if (polls++ % 25 == 0)
                {
                    report(FormattableString.Invariant($"{Name(index)} active={state.Active} rapid={state.RapidVoters} expected={live.Length}"));
                }

                if (state.Active == live.Length && (!requireVoterView || state.RapidVoters == (state.IsVoter ? live.Count(member => member < 3) : 0))
                    && expected.SequenceEqual(state.Members, StringComparer.Ordinal))
                {
                    break;
                }

                await Task.Delay(200, token);
            }
        });

        return [.. expected];
    }

    private async Task<ChurnState> ReadAsync(int index, CancellationToken cancellationToken)
    {
        using var client = app.CreateHttpClient(Name(index));
        client.Timeout = TimeSpan.FromMinutes(3);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await client.GetFromJsonAsync<ChurnState>("/state", cancellationToken);
            }
            catch (HttpRequestException)
            {
                if (app.ResourceNotifications.TryGetCurrentState(Name(index), out var current)
                    && current.Snapshot.State?.Text is "Exited" or "FailedToStart" or "Finished")
                {
                    await ReportLogsAsync(index, cancellationToken);
                    throw new InvalidOperationException($"{Name(index)} exited before convergence.");
                }
            }

            await Task.Delay(200, cancellationToken);
        }
    }

    private async Task ReportLogsAsync(int index, CancellationToken cancellationToken)
    {
        var lines = new Queue<string>();
        await foreach (var batch in app.Services.GetRequiredService<ResourceLoggerService>().GetAllAsync(app.Services.GetRequiredService<DistributedApplicationModel>().Resources.Single(resource => resource.Name == Name(index))).WithCancellation(cancellationToken))
        {
            foreach (var line in batch)
            {
                lines.Enqueue(line.Content);
                if (lines.Count > 80)
                {
                    _ = lines.Dequeue();
                }
            }
        }

        report(string.Join(Environment.NewLine, lines));
    }

    public async Task<Dictionary<string, string>> EnvironmentAsync(CancellationToken cancellationToken)
    {
        var resource = app.Services.GetRequiredService<DistributedApplicationModel>().Resources.OfType<ProjectResource>()
            .Single(node => node.Name == "silo-0");
        var configuration = await ExecutionConfigurationBuilder.Create(resource).WithEnvironmentVariablesConfig()
            .BuildAsync(new DistributedApplicationExecutionContext(DistributedApplicationOperation.Run), NullLogger.Instance, cancellationToken);
        Assert.Null(configuration.Exception);
        return configuration.EnvironmentVariables.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
    }

    public async Task RequireEmptyAsync(CancellationToken cancellationToken)
    {
        while (Enumerable.Range(0, 50).Any(index =>
            !app.ResourceNotifications.TryGetCurrentState(Name(index), out var current)
            || current.Snapshot.State?.Text is not ("NotStarted" or "Exited" or "Finished")))
        {
            await Task.Delay(100, cancellationToken);
        }
    }

    private static string Name(int index) => "silo-" + index.ToString(CultureInfo.InvariantCulture);

    private readonly record struct ChurnState(int Active, int RapidVoters, string[] Members, bool IsVoter, string Self);
}
