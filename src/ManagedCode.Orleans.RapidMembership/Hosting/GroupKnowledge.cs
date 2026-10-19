namespace ManagedCode.Orleans.RapidMembership.Hosting;

internal sealed class GroupKnowledge
{
    private CoordinationConfiguration? _configuration;
    private long? _root;
    private bool _observed;

    public CoordinationConfiguration? Configuration => Volatile.Read(ref _configuration);

    public bool HasObservedCluster => Volatile.Read(ref _observed);

    public void Learn(IReadOnlyList<PeerReply> replies, bool server)
    {
        var configurations = replies.SelectMany(reply => new[]
        {
            reply.Configuration, reply.ObservedConfiguration, reply.KnownConfiguration,
        }).OfType<CoordinationConfiguration>().ToArray();
        var roots = configurations.Select(value => value.ClusterId).Distinct().ToArray();
        if (roots.Length > 1 || server && _root is { } root && roots.Any(value => value != root))
        {
            throw new InvalidOperationException("Independent RAPID clusters with the same service identity were discovered; refusing to merge their tables.");
        }

        if (roots.Length == 0)
        {
            return;
        }

        if (_root != roots[0])
        {
            _configuration = null;
        }

        _root = roots[0];
        Volatile.Write(ref _observed, true);
        var installed = replies.SelectMany(reply => new[] { reply.Configuration, reply.KnownConfiguration })
            .OfType<CoordinationConfiguration>().MaxBy(value => value.Version);
        if (installed is not null && (_configuration is null || installed.Version > _configuration.Version))
        {
            Volatile.Write(ref _configuration, installed);
        }
    }
}
