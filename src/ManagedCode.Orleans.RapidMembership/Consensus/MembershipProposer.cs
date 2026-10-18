using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal sealed class MembershipProposer(PeerTransport transport, RapidMembershipOptions options, Serializer<MembershipSnapshot> serializer) : IDisposable
{
    private readonly SemaphoreSlim _admission = new(1);
    private readonly string _identity = Guid.NewGuid().ToString("N");
    private long _counter;

    public async Task<MembershipSnapshot> ExecuteAsync(Func<MembershipTableData, MembershipTableData?>? mutation, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(options.OperationTimeout);
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
            var observed = await transport.ExchangeAsync(new PeerRequest { Action = PeerAction.Inspect }, cancellationToken);
            if (observed.Count < transport.Quorum)
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

            if (settled || mutation is null && observed.All(reply => reply.State.Accepted is null))
            {
                var repair = await transport.ExchangeAsync(new PeerRequest
                {
                    Action = PeerAction.Commit,
                    Index = previous.Index,
                    Snapshot = previous,
                }, cancellationToken);
                if (repair.Count(reply => reply.Granted) >= transport.Quorum)
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
                Index = index,
                Ballot = ballot,
                Snapshot = previous,
            }, cancellationToken);
            ObserveCounters(promises);
            var granted = promises.Where(reply => reply.Granted).ToArray();
            if (granted.Length < transport.Quorum)
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
                Index = index,
                Ballot = ballot,
                Snapshot = candidate,
            }, cancellationToken);
            ObserveCounters(votes);
            if (votes.Count(reply => reply.Granted) < transport.Quorum)
            {
                await RetryAsync(++conflicts, cancellationToken);
                continue;
            }

            var commits = await transport.ExchangeAsync(new PeerRequest
            {
                Action = PeerAction.Commit,
                Index = index,
                Snapshot = candidate,
            }, cancellationToken);
            if (commits.Count(reply => reply.Granted) >= transport.Quorum && (candidate.OperationId == operationId || mutation is null))
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

    private static Task RetryAsync(int conflicts, CancellationToken cancellationToken)
    {
        var ceiling = Math.Min(1000, 20 << Math.Min(conflicts, 6));
        return Task.Delay(System.Security.Cryptography.RandomNumberGenerator.GetInt32(10, ceiling), cancellationToken);
    }

    public void Dispose() => _admission.Dispose();
}
