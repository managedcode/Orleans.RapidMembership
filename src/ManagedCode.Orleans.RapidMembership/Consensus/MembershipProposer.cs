using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal sealed class MembershipProposer(PeerTransport transport, RapidMembershipOptions options, Serializer<MembershipSnapshot> serializer, PeerDirectory directory) : IDisposable, IAsyncDisposable
{
    public PeerDirectory Directory => directory;

    private readonly SemaphoreSlim _admission = new(1);
    private readonly CancellationTokenSource _stopping = new();
    private readonly string _identity = Guid.NewGuid().ToString("N");
    private int _disposed;
    private long _counter;

    public async Task<MembershipSnapshot> ExecuteAsync(Func<MembershipTableData, MembershipTableData?>? mutation, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        using var timer = new CancellationTokenSource(options.OperationTimeout, options.TimeProvider);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token, timer.Token);
        await _admission.WaitAsync(deadline.Token);
        try
        {
            var result = await ProposeAsync(mutation, deadline.Token);
            Diagnostics.MembershipMetrics.Proposal(completed: true);
            return result;
        }
        catch
        {
            Diagnostics.MembershipMetrics.Proposal(completed: false);
            throw;
        }
        finally
        {
            _ = _admission.Release();
        }
    }

    private async Task<MembershipSnapshot> ProposeAsync(Func<MembershipTableData, MembershipTableData?>? mutation, CancellationToken cancellationToken)
    {
        var operationId = Guid.NewGuid();
        var conflicts = 0;
        long? firstIndex = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var configuration = await directory.ResolveAsync(cancellationToken);
            var observed = (await transport.ExchangeAsync(new PeerRequest { Action = PeerAction.Inspect, Configuration = configuration }, configuration, cancellationToken))
                .Where(reply => reply.Granted).ToList();
            if (observed.Count < configuration.Quorum)
            {
                await RetryAsync(++conflicts, cancellationToken);
                continue;
            }

            var previous = observed.MaxBy(reply => reply.State.Committed.Index)!.State.Committed;
            firstIndex ??= checked(previous.Index + 1);
            var completed = previous.Outcomes.FirstOrDefault(outcome => outcome.OperationId == operationId);
            var settled = completed.OperationId == operationId;
            if (!settled && mutation is not null && previous.OutcomeFloor >= firstIndex.Value)
            {
                throw new InvalidOperationException("Membership operation outcome was evicted before settlement; re-read the native membership row and version.");
            }

            // Accepted work within the committed prefix needs repair, not another consensus slot.
            if (settled || mutation is null && observed.All(reply =>
                reply.State.Accepted is not { } pending || pending.Index <= previous.Index))
            {
                var repair = await transport.ExchangeAsync(new PeerRequest
                {
                    Action = PeerAction.Commit,
                    Configuration = configuration,
                    Index = previous.Index,
                    Snapshot = previous,
                }, configuration, cancellationToken);
                if (repair.Count(reply => reply.Granted) >= configuration.Quorum)
                {
                    var result = serializer.Deserialize(serializer.SerializeToArray(previous)) ?? throw new InvalidDataException("Empty membership state.");
                    if (settled)
                    {
                        result.Applied = completed.Applied;
                    }

                    return result;
                }

                await RetryAsync(++conflicts, cancellationToken);
                continue;
            }

            _counter = checked(Math.Max(_counter, observed.Max(reply => reply.State.Promised.Counter)) + 1);
            var ballot = new Ballot(_counter, _identity);
            var index = checked(previous.Index + 1);
            var promises = await transport.ExchangeAsync(new PeerRequest
            {
                Action = PeerAction.Prepare,
                Configuration = configuration,
                Index = index,
                Ballot = ballot,
                Snapshot = previous,
            }, configuration, cancellationToken);
            ObserveCounters(promises);
            var granted = promises.Where(reply => reply.Granted).ToArray();
            if (granted.Length < configuration.Quorum)
            {
                await RetryAsync(++conflicts, cancellationToken);
                continue;
            }

            var accepted = granted.Where(reply => reply.State.Accepted is not null)
                .MaxBy(reply => reply.State.AcceptedBallot)?.State.Accepted;
            var candidate = accepted ?? CreateCandidate(previous, index, operationId, mutation);
            var votes = await transport.ExchangeAsync(new PeerRequest
            {
                Action = PeerAction.Accept,
                Configuration = configuration,
                Index = index,
                Ballot = ballot,
                Snapshot = candidate,
            }, configuration, cancellationToken);
            ObserveCounters(votes);
            if (votes.Count(reply => reply.Granted) < configuration.Quorum)
            {
                await RetryAsync(++conflicts, cancellationToken);
                continue;
            }

            var commits = await transport.ExchangeAsync(new PeerRequest
            {
                Action = PeerAction.Commit,
                Configuration = configuration,
                Index = index,
                Snapshot = candidate,
            }, configuration, cancellationToken);
            if (commits.Count(reply => reply.Granted) >= configuration.Quorum && (candidate.OperationId == operationId || mutation is null))
            {
                return serializer.Deserialize(serializer.SerializeToArray(candidate)) ?? throw new InvalidDataException("Empty membership state.");
            }

            await RetryAsync(++conflicts, cancellationToken);
        }
    }

    private MembershipSnapshot CreateCandidate(MembershipSnapshot previous, long index, Guid operationId,
        Func<MembershipTableData, MembershipTableData?>? mutation)
    {
        var copy = serializer.Deserialize(serializer.SerializeToArray(previous)) ?? throw new InvalidDataException("Empty membership state.");
        var table = mutation?.Invoke(copy.Table);
        if ((table ?? copy.Table).Members.Count > options.MaximumEntries)
        {
            throw new InvalidOperationException("Membership table row capacity was reached.");
        }

        var candidate = new MembershipSnapshot
        {
            Index = index,
            OperationId = operationId,
            Applied = table is not null,
            Table = table ?? copy.Table,
            Outcomes = copy.Outcomes,
            OutcomeFloor = copy.OutcomeFloor,
        };
        if (mutation is not null)
        {
            while (candidate.Outcomes.Count >= options.MaximumOperationHistory)
            {
                candidate.OutcomeFloor = candidate.Outcomes[0].Index;
                candidate.Outcomes.RemoveAt(0);
            }

            candidate.Outcomes.Add(new CompletedMutation(operationId, index, candidate.Applied));
        }

        if (serializer.SerializeToArray(candidate).Length > options.MaximumSnapshotBytes)
        {
            throw new InvalidOperationException("Membership snapshot byte capacity was reached.");
        }

        return candidate;
    }

    private void ObserveCounters(List<PeerReply> replies)
    {
        foreach (var reply in replies)
        {
            _counter = Math.Max(_counter, reply.State.Promised.Counter);
        }
    }

    private Task RetryAsync(int conflicts, CancellationToken cancellationToken)
    {
        var ceiling = Math.Min(1000, 20 << Math.Min(conflicts, 6));
        return Task.Delay(TimeSpan.FromMilliseconds(System.Security.Cryptography.RandomNumberGenerator.GetInt32(10, ceiling)),
            options.TimeProvider, cancellationToken);
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _stopping.CancelAsync();
        await _admission.WaitAsync();
        _admission.Dispose();
        _stopping.Dispose();
    }
}
