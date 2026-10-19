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
        var (Indices, Members, Configuration) = await CoordinationAsync(live, cancellationToken);
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

                if (state.Active == live.Length && (!requireVoterView || (state.IsVoter
                    ? state.RapidConfiguration == Configuration && Members.SequenceEqual(state.RapidMembers, StringComparer.Ordinal)
                    : state.RapidVoters == 0))
                    && expected.SequenceEqual(state.Members, StringComparer.Ordinal))
                {
                    break;
                }

                await Task.Delay(200, token);
            }
        });

        return [.. expected];
    }

    public async Task<int[]> VotersAsync(int[] live, CancellationToken cancellationToken)
        => (await CoordinationAsync(live, cancellationToken)).Indices;

    private async Task<(int[] Indices, string[] Members, string Configuration)> CoordinationAsync(int[] live, CancellationToken cancellationToken)
    {
        while (true)
        {
            var states = new ChurnState[live.Length];
            await Parallel.ForEachAsync(Enumerable.Range(0, live.Length), new ParallelOptions
            {
                MaxDegreeOfParallelism = 5,
                CancellationToken = cancellationToken,
            }, async (slot, token) => states[slot] = await ReadAsync(live[slot], token));
            var indices = Enumerable.Range(0, live.Length).Where(slot => states[slot].IsVoter).ToArray();
            if (indices.Length == Math.Min(3, live.Length) && indices.Length > 0)
            {
                var first = states[indices[0]];
                if (first.RapidMembers.Length == indices.Length && indices.All(slot =>
                    first.RapidConfiguration == states[slot].RapidConfiguration
                    && first.RapidMembers.SequenceEqual(states[slot].RapidMembers, StringComparer.Ordinal)))
                {
                    report($"RAPID coordination {first.RapidConfiguration}: {string.Join(',', first.RapidMembers)}");
                    return ([.. indices.Select(slot => live[slot])], first.RapidMembers, first.RapidConfiguration);
                }
            }

            await Task.Delay(200, cancellationToken);
        }
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

    public async Task RequireEmptyAsync(CancellationToken cancellationToken, int resources = 30)
    {
        while (Enumerable.Range(0, resources).Any(index =>
            !app.ResourceNotifications.TryGetCurrentState(Name(index), out var current)
            || current.Snapshot.State?.Text is not ("NotStarted" or "Exited" or "Finished")))
        {
            await Task.Delay(100, cancellationToken);
        }
    }

    public async Task<string> IdentityAsync(int index, CancellationToken cancellationToken)
        => (await ReadAsync(index, cancellationToken)).Self;

    public async Task RequireCrashedAsync(int index, CancellationToken cancellationToken)
    {
        while (!app.ResourceNotifications.TryGetCurrentState(Name(index), out var state)
            || state.Snapshot.State?.Text is not ("Exited" or "Finished"))
        {
            await Task.Delay(100, cancellationToken);
        }

        Assert.True(app.ResourceNotifications.TryGetCurrentState(Name(index), out var exited));
        _ = Assert.NotNull(exited.Snapshot.ExitCode);
        Assert.NotEqual(0, exited.Snapshot.ExitCode);
    }

    private static string Name(int index) => "silo-" + index.ToString(CultureInfo.InvariantCulture);

    private readonly record struct ChurnState(int Active, int RapidVoters, string[] Members, bool IsVoter, string Self,
        string[] RapidMembers, string RapidConfiguration);
}
