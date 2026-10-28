using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Serialization;
using RapidCluster;

namespace ManagedCode.Orleans.RapidMembership;

/// <summary>Provides the native Orleans membership table using automatically discovered in-memory quorum replicas.</summary>
public sealed class RapidMembershipTable : IMembershipTable, IAsyncDisposable
{
    private readonly RapidMembershipOptions _options;
    private readonly ClusterOptions _cluster;
    private readonly MembershipProposer _proposer;
    private readonly Serializer<MembershipEntry> _entries;
    private readonly SemaphoreSlim _lifecycle = new(1);
    private int _disposed;
    private bool _initialized;

    internal RapidMembershipTable(IOptions<RapidMembershipOptions> options, IOptions<ClusterOptions> cluster,
        MembershipProposer proposer, Serializer<MembershipEntry> entries)
    {
        _options = options.Value;
        _cluster = cluster.Value;
        _proposer = proposer;
        _entries = entries;
    }

    /// <summary>Gets a value indicating whether this silo currently participates in the automatically formed coordination group.</summary>
    public bool IsVoter => _proposer.Directory.Host?.IsVoter == true;

    /// <summary>Gets the local RAPID voter view. Ordinary silos do not participate in voter consensus.</summary>
    public ClusterMembershipView VoterView => _proposer.Directory.Host is { IsVoter: true } host ? host.Cluster.CurrentView : ClusterMembershipView.Empty;

    /// <inheritdoc />
    public Task InitializeMembershipTable(bool tryInitTableVersion) => InitializeMembershipTableAsync(tryInitTableVersion);

    /// <inheritdoc />
    public async Task InitializeMembershipTableAsync(bool tryInitTableVersion, CancellationToken cancellationToken = default)
    {
        await _lifecycle.WaitAsync(cancellationToken);
        try
        {
            if (!_initialized)
            {
                _options.Validate(server: true);
                await _proposer.Directory.StartServerAsync(cancellationToken);
                await _proposer.Directory.WaitForFormationAsync(cancellationToken);

                _ = await ReadAllAsync(cancellationToken);
                _initialized = true;
            }
        }
        finally
        {
            _ = _lifecycle.Release();
        }
    }

    /// <inheritdoc />
    public Task<MembershipTableData> ReadAll() => ReadAllAsync();

    /// <inheritdoc />
    public async Task<MembershipTableData> ReadAllAsync(CancellationToken cancellationToken = default)
        => (await _proposer.ExecuteAsync(null, cancellationToken)).Table;

    /// <inheritdoc />
    public Task<MembershipTableData> ReadRow(SiloAddress key) => ReadRowAsync(key);

    /// <inheritdoc />
    public async Task<MembershipTableData> ReadRowAsync(SiloAddress key, CancellationToken cancellationToken = default)
    {
        var table = await ReadAllAsync(cancellationToken);
        return table.TryGet(key) is { } row ? new MembershipTableData(row, table.Version) : new MembershipTableData(table.Version);
    }

    /// <inheritdoc />
    public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion) => InsertRowAsync(entry, tableVersion);

    /// <inheritdoc />
    public Task<bool> InsertRowAsync(MembershipEntry entry, TableVersion tableVersion, CancellationToken cancellationToken = default)
        => WriteAsync(entry, null, tableVersion, cancellationToken);

    /// <inheritdoc />
    public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion) => UpdateRowAsync(entry, etag, tableVersion);

    /// <inheritdoc />
    public Task<bool> UpdateRowAsync(MembershipEntry entry, string etag, TableVersion tableVersion, CancellationToken cancellationToken = default)
        => WriteAsync(entry, etag, tableVersion, cancellationToken);

    private async Task<bool> WriteAsync(MembershipEntry entry, string? etag, TableVersion version, CancellationToken cancellationToken)
    {
        var copy = _entries.Deserialize(_entries.SerializeToArray(entry)) ?? throw new InvalidDataException("Empty membership entry.");
        return (await _proposer.ExecuteAsync(table => MembershipMutations.Write(table, copy, etag, version), cancellationToken)).Applied;
    }

    /// <inheritdoc />
    public Task UpdateIAmAlive(MembershipEntry entry) => UpdateIAmAliveAsync(entry);

    /// <inheritdoc />
    public async Task UpdateIAmAliveAsync(MembershipEntry entry, CancellationToken cancellationToken = default)
    {
        var copy = _entries.Deserialize(_entries.SerializeToArray(entry)) ?? throw new InvalidDataException("Empty membership entry.");
        _ = await _proposer.ExecuteAsync(table => MembershipMutations.Heartbeat(table, copy), cancellationToken);
    }

    /// <inheritdoc />
    public Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate) => CleanupDefunctSiloEntriesAsync(beforeDate);

    /// <inheritdoc />
    public async Task CleanupDefunctSiloEntriesAsync(DateTimeOffset beforeDate, CancellationToken cancellationToken = default)
        => _ = await _proposer.ExecuteAsync(table => MembershipMutations.Cleanup(table, beforeDate), cancellationToken);

    /// <inheritdoc />
    public Task DeleteMembershipTableEntries(string clusterId) => DeleteMembershipTableEntriesAsync(clusterId);

    /// <inheritdoc />
    public async Task DeleteMembershipTableEntriesAsync(string clusterId, CancellationToken cancellationToken = default)
    {
        if (!string.Equals(clusterId, _cluster.ClusterId, StringComparison.Ordinal))
        {
            throw new ArgumentException("The requested cluster differs from the configured cluster.", nameof(clusterId));
        }

        _ = await _proposer.ExecuteAsync(table => new MembershipTableData(
            new TableVersion(checked(table.Version.Version + 1), Guid.NewGuid().ToString("N"))), cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _lifecycle.WaitAsync();
        try
        {
            await _proposer.Directory.DisposeAsync();
        }
        finally
        {
            _ = _lifecycle.Release();
            _lifecycle.Dispose();
        }
    }
}
