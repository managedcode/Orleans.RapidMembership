namespace ManagedCode.Orleans.RapidMembership.Consensus;

internal sealed class ConfigurationResolver(PeerTransport transport)
{
    public async Task<CoordinationConfiguration?> ResolveAsync(IReadOnlyList<PeerReply> descriptions, CancellationToken cancellationToken)
    {
        var configurations = descriptions.Select(reply => reply.Configuration).OfType<CoordinationConfiguration>()
            .DistinctBy(value => value.Version).OrderByDescending(value => value.Version).ToArray();
        var ready = configurations.FirstOrDefault(configuration => descriptions
            .Count(reply => reply.Granted && configuration.Matches(reply.Configuration)) >= configuration.Quorum);
        var partial = configurations.FirstOrDefault(configuration => ready is null || configuration.Version > ready.Version);
        if (partial is not null)
        {
            var owner = descriptions.First(reply => partial.Matches(reply.Configuration));
            var member = partial.Members.Single(value => value.Incarnation == owner.Incarnation);
            var proof = await transport.SendAsync(member, new PeerRequest { Action = PeerAction.FetchTransfer }, cancellationToken);
            if (proof is { Granted: true, TransferEvidence.IsDefaultOrEmpty: false })
            {
                var previous = proof.TransferEvidence[0].Previous;
                if (await InstallAsync(previous, partial, proof.TransferEvidence, cancellationToken))
                {
                    return partial;
                }
            }
        }

        var candidates = descriptions.Where(reply => reply.Configuration is not null && reply.FrozenFor is not null
            && (ready is null || reply.FrozenFor.Version > ready.Version))
            .Select(reply => (Previous: reply.Configuration!, Next: reply.FrozenFor!)).ToList();
        candidates.AddRange(descriptions.Where(reply => reply.Configuration is not null && reply.NextConfiguration is not null
            && (ready is null || reply.NextConfiguration.Version > ready.Version))
            .Select(reply => (reply.Configuration!, reply.NextConfiguration!)));

        foreach (var (Previous, Next) in candidates.DistinctBy(value => value.Next.Version).OrderBy(value => value.Next.Version))
        {
            if (await TransferAsync(Previous, Next, cancellationToken))
            {
                return Next;
            }
        }

        if (ready is not null)
        {
            await RepairAsync(ready, descriptions, cancellationToken);
        }

        return ready;
    }

    private async Task RepairAsync(CoordinationConfiguration configuration, IReadOnlyList<PeerReply> descriptions, CancellationToken cancellationToken)
    {
        var behind = configuration.Members.Where(member => descriptions.Any(reply => reply.Incarnation == member.Incarnation
            && !configuration.Matches(reply.Configuration))).ToArray();
        if (behind.Length == 0)
        {
            return;
        }

        var source = configuration.Members.First(member => descriptions.Any(reply => reply.Incarnation == member.Incarnation
            && configuration.Matches(reply.Configuration)));
        var proof = await transport.SendAsync(source, new PeerRequest { Action = PeerAction.FetchTransfer }, cancellationToken);
        if (proof is { Granted: true, TransferEvidence.IsDefaultOrEmpty: false })
        {
            var request = new PeerRequest
            {
                Action = PeerAction.InstallConfiguration,
                PreviousConfiguration = proof.TransferEvidence[0].Previous,
                Configuration = configuration,
                FrozenReplicas = proof.TransferEvidence,
            };
            _ = await Task.WhenAll(behind.Select(member => transport.SendAsync(member, request, cancellationToken)));
        }
    }

    private async Task<bool> TransferAsync(CoordinationConfiguration previous, CoordinationConfiguration next, CancellationToken cancellationToken)
    {
        var votes = await transport.ExchangeAsync(new PeerRequest
        {
            Action = PeerAction.FreezeConfiguration,
            PreviousConfiguration = previous,
            Configuration = next,
        }, previous, cancellationToken);
        var frozen = System.Collections.Immutable.ImmutableArray.CreateRange(votes
            .Where(reply => reply.Granted && previous.Matches(reply.Configuration) && next.Matches(reply.FrozenFor))
            .Select(reply => new FrozenReplica(reply.Incarnation, previous, next, reply.State)));
        return frozen.Length >= previous.Quorum && await InstallAsync(previous, next, frozen, cancellationToken);
    }

    private async Task<bool> InstallAsync(CoordinationConfiguration previous, CoordinationConfiguration next,
        System.Collections.Immutable.ImmutableArray<FrozenReplica> frozen, CancellationToken cancellationToken)
    {
        var installed = await transport.ExchangeAsync(new PeerRequest
        {
            Action = PeerAction.InstallConfiguration,
            PreviousConfiguration = previous,
            Configuration = next,
            FrozenReplicas = frozen,
        }, next, cancellationToken);
        return ConfigurationHandoff.HasInstalledQuorum(next, installed.Where(reply => reply.Granted).Select(reply => reply.Incarnation));
    }
}
