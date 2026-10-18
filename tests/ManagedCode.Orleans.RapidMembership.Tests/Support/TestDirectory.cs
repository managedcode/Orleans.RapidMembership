namespace ManagedCode.Orleans.RapidMembership.Tests.Support;

internal sealed class TestDirectory : IDisposable
{
    public TestDirectory()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(System.IO.Path.Combine(root.FullName, "RapidCluster.slnx")))
        {
            root = root.Parent ?? throw new InvalidOperationException("Repository root was not found.");
        }

        Path = System.IO.Path.Combine(root.FullName, "artifacts", "tests", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose() => Directory.Delete(Path, recursive: true);
}
