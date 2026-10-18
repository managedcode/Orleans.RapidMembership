using System.Security.Cryptography;
using Orleans.Serialization;

namespace ManagedCode.Orleans.RapidMembership.Storage;

internal sealed class VoterStorage : IDisposable
{
    private readonly FileStream _ownership;
    private readonly string _path;
    private readonly Serializer<AcceptorState> _serializer;
    private readonly RapidMembershipOptions _options;
    private readonly string _identity;

    public VoterStorage(RapidMembershipOptions options, string identity, Serializer<AcceptorState> serializer)
    {
        _options = options;
        _identity = identity;
        _serializer = serializer;
        _ = Directory.CreateDirectory(options.StorageDirectory);
        _path = Path.Combine(options.StorageDirectory, "membership.bin");
        _ownership = new FileStream(Path.Combine(options.StorageDirectory, "owner.lock"), FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None);
    }

    public AcceptorState Load()
    {
        if (!File.Exists(_path))
        {
            return new AcceptorState { Identity = _identity };
        }

        if (new FileInfo(_path).Length > checked(_options.MaximumSnapshotBytes * 3 + 32))
        {
            throw new InvalidDataException("Membership voter state exceeds the configured bound.");
        }

        var bytes = File.ReadAllBytes(_path);
        if (bytes.Length < 32 || !CryptographicOperations.FixedTimeEquals(bytes.AsSpan(0, 32), SHA256.HashData(bytes.AsSpan(32))))
        {
            throw new InvalidDataException("Membership voter state checksum failed.");
        }

        var state = _serializer.Deserialize(bytes.AsSpan(32)) ?? throw new InvalidDataException("Empty membership state.");
        if (!string.Equals(state.Identity, _identity, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Cluster identity or voter configuration differs from the durable state.");
        }

        return state;
    }

    public void Save(AcceptorState state)
    {
        var bytes = _serializer.SerializeToArray(state);
        if (bytes.Length > checked(_options.MaximumSnapshotBytes * 3))
        {
            throw new InvalidDataException("Membership voter state exceeds the configured bound.");
        }

        var pending = _path + ".pending";
        using (var output = new FileStream(pending, FileMode.Create, FileAccess.Write, FileShare.None, 4096))
        {
            output.Write(SHA256.HashData(bytes));
            output.Write(bytes);
            output.Flush(flushToDisk: true);
        }

        File.Move(pending, _path, overwrite: true);
    }

    public void Dispose() => _ownership.Dispose();
}
