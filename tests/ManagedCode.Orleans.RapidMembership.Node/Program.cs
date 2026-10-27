using System.Net;
using ManagedCode.Orleans.RapidMembership;
using ManagedCode.Orleans.RapidMembership.Node.Contracts;
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
        membership.ProbeTimeout = TimeSpan.FromSeconds(20);
        membership.MinProbeTimeout = TimeSpan.FromSeconds(10);
        membership.MaxProbeTimeout = TimeSpan.FromSeconds(40);
        membership.MaxJoinAttemptTime = TimeSpan.FromMinutes(10);
        membership.TableRefreshTimeout = TimeSpan.FromSeconds(5);
        membership.IAmAliveTablePublishTimeout = TimeSpan.FromSeconds(30);
    });
    _ = silo.UseRapidMembership(options =>
    {
        options.ListenPort = builder.Configuration.GetValue<int>("RAPID_PORT");
        options.SharedSecret = builder.Configuration["RAPID_SECRET"]!;
        options.OperationTimeout = TimeSpan.FromMinutes(2);
        options.PeerTimeout = TimeSpan.FromSeconds(5);
    });
});
_ = builder.Services.Configure<Microsoft.Extensions.Hosting.HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromMinutes(3));
var app = builder.Build();
var processIdentity = NodeProcessIdentity.Capture();
_ = app.MapGet("/process", () => processIdentity);
_ = app.MapGet("/state", async (RapidMembershipTable table, global::Orleans.Runtime.ILocalSiloDetails details, CancellationToken cancellationToken) =>
{
    var snapshot = await table.ReadAllAsync(cancellationToken);
    return new
    {
        processIdentity.ProcessId,
        Self = details.SiloAddress.ToString(),
        ProcessStart = processIdentity.StartTimeUtcTicks,
        Members = snapshot.Members.Where(row => row.Item1.Status == Orleans.Runtime.SiloStatus.Active)
            .Select(row => row.Item1.SiloAddress.ToString()).Order(StringComparer.Ordinal).ToArray(),
        Active = snapshot.Members.Count(row => row.Item1.Status == Orleans.Runtime.SiloStatus.Active),
        snapshot.Version.Version,
        table.IsVoter,
        RapidVoters = table.VoterView.Members.Length,
        RapidConfiguration = table.VoterView.ConfigurationId.ToString(),
        RapidMembers = table.VoterView.Members.Select(member =>
            $"{new Guid(member.Metadata["membership-incarnation"].Span):N}@{member.EndPoint}").Order(StringComparer.Ordinal).ToArray(),
        RapidNodeIds = table.VoterView.Members.Select(member =>
            string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{member.Id}@{member.EndPoint}")).Order(StringComparer.Ordinal).ToArray(),
    };
});
_ = app.MapGet("/echo/{value}", (string value, IGrainFactory grains) => grains.GetGrain<IProbeGrain>("probe").Echo(value));
_ = app.MapPost("/shutdown", (HttpContext context, IHostApplicationLifetime lifetime) =>
{
    context.Response.OnCompleted(() =>
    {
        lifetime.StopApplication();
        return Task.CompletedTask;
    });
    return Results.Accepted();
});
app.Run();
