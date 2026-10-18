using System.Net;
using ManagedCode.Orleans.RapidMembership;
using Orleans;
using Orleans.Configuration;

var builder = WebApplication.CreateBuilder(args);
_ = builder.Logging.SetMinimumLevel(LogLevel.Warning);
_ = builder.Host.UseOrleans(silo =>
{
    _ = silo.Configure<ClusterOptions>(cluster =>
    {
        cluster.ClusterId = builder.Configuration["CLUSTER_ID"]!;
        cluster.ServiceId = "rapid-membership-tests";
    });
    _ = silo.ConfigureEndpoints(IPAddress.Loopback, builder.Configuration.GetValue<int>("SILO_PORT"),
        builder.Configuration.GetValue<int>("GATEWAY_PORT"));
    _ = silo.Configure<ClusterMembershipOptions>(membership =>
    {
        membership.ProbeTimeout = TimeSpan.FromSeconds(1);
        membership.TableRefreshTimeout = TimeSpan.FromSeconds(1);
        membership.IAmAliveTablePublishTimeout = TimeSpan.FromSeconds(1);
    });
    _ = silo.UseRapidMembership(options =>
    {
        options.ListenEndpoint = builder.Configuration["RAPID_LISTEN"]!;
        options.StorageDirectory = builder.Configuration["RAPID_DIRECTORY"]!;
        options.SharedSecret = builder.Configuration["RAPID_SECRET"]!;
        options.SeedEndpoints = builder.Configuration["RAPID_SEEDS"]!.Split(',');
        options.OperationTimeout = TimeSpan.FromMinutes(2);
        options.PeerTimeout = TimeSpan.FromSeconds(5);
    });
});
var app = builder.Build();
_ = app.MapGet("/state", async (RapidMembershipTable table, global::Orleans.Runtime.ILocalSiloDetails details, CancellationToken cancellationToken) =>
{
    var snapshot = await table.ReadAllAsync(cancellationToken);
    return new
    {
        Environment.ProcessId,
        Self = details.SiloAddress.ToString(),
        ProcessStart = System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime().Ticks,
        Members = snapshot.Members.Where(row => row.Item1.Status == Orleans.Runtime.SiloStatus.Active)
            .Select(row => row.Item1.SiloAddress.ToString()).Order(StringComparer.Ordinal).ToArray(),
        Active = snapshot.Members.Count(row => row.Item1.Status == Orleans.Runtime.SiloStatus.Active),
        snapshot.Version.Version,
        table.IsVoter,
        RapidVoters = table.VoterView.Members.Length,
    };
});
_ = app.MapGet("/echo/{value}", (string value, IGrainFactory grains) => grains.GetGrain<IProbeGrain>("probe").Echo(value));
app.Run();
