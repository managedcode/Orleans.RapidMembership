using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using Clockwork;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Represents a simulated node in a Rapid cluster.
/// Uses in-memory transport instead of gRPC and does not require a WebApplication.
/// Lifetime is managed by the SimulationHarness via Destroy() - do not implement IDisposable.
/// </summary>
[SuppressMessage("Reliability", "CA1001:Types that own disposable fields should be disposable",
    Justification = "Lifetime is managed by SimulationHarness.Destroy() to avoid CA2000 warnings in tests")]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class RapidSimulationNode : SimulationNode
{
    private readonly RapidSimulationNodeLogger _log;
    private readonly MembershipService _membershipService;
    private readonly IMembershipViewAccessor _viewAccessor;
    private readonly IRapidClusterLifecycle _lifecycle;
    private readonly ServiceProvider _serviceProvider;
    private readonly CancellationTokenSource _disposeCts = new();
    private bool _disposed;
    private bool _isInitialized;

    /// <summary>
    /// Gets the endpoint address of this node.
    /// </summary>
    public Endpoint Address { get; }

    /// <summary>
    /// Gets the network address of this node as a string for routing.
    /// </summary>
    public override string NetworkAddress => RapidClusterUtils.Loggable(Address);

    /// <summary>
    /// Gets the simulation context for this node.
    /// </summary>
    public override SimulationNodeContext Context { get; }

    /// <summary>
    /// Gets the current membership view of this node.
    /// </summary>
    public MembershipView CurrentView => _viewAccessor.CurrentView;

    /// <summary>
    /// Gets the membership view accessor for subscribing to view changes.
    /// </summary>
    public IMembershipViewAccessor ViewAccessor => _viewAccessor;

    /// <summary>
    /// Gets a value indicating whether gets whether this node is initialized and part of a cluster.
    /// </summary>
    public override bool IsInitialized => _isInitialized;

    /// <summary>
    /// Gets the membership size of this node's view.
    /// </summary>
    public int MembershipSize => _viewAccessor.CurrentView.Size;

    /// <summary>
    /// Gets the messaging client for testing purposes.
    /// </summary>
    internal InMemoryMessagingClient MessagingClient { get; }

    /// <summary>
    /// Gets the cancellation token that is triggered when this node is being torn down.
    /// </summary>
    internal CancellationToken TeardownCancellationToken => _disposeCts.Token;

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with a single seed address.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        Endpoint address,
        Endpoint? seedAddress,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
        : this(harness, address, seedAddress != null ? [seedAddress] : null, metadata, protocolOptions, failureDetectorOptions, loggerFactory)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with multiple seed addresses for testing seed failover.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        Endpoint address,
        IList<Endpoint>? seedAddresses,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
    {
        Address = address;

        // Wrap the logger factory to prepend the node name to all log messages
        var baseLoggerFactory = loggerFactory ?? harness.LoggerFactory;
        var nodeName = string.Create(CultureInfo.InvariantCulture, $"{address.Hostname.ToStringUtf8()}:{address.Port}");
        ILoggerFactory nodeLoggerFactory = baseLoggerFactory != null
            ? new NodePrefixedLoggerFactory(baseLoggerFactory, nodeName)
            : NullLoggerFactory.Instance;

        // Create logger for the context (suspend/resume logging happens there)
        var contextLogger = nodeLoggerFactory.CreateLogger<SimulationNodeContext>();
        Context = new SimulationNodeContext(harness.Clock, harness.Guard, harness.CreateDerivedRandom(), harness.TaskQueue, contextLogger);

        _log = new RapidSimulationNodeLogger(nodeLoggerFactory.CreateLogger<RapidSimulationNode>());

        var configuredProtocolOptions = protocolOptions ?? new RapidClusterProtocolOptions();
        var configuredFailureDetectorOptions = failureDetectorOptions ?? new PingPongFailureDetectorOptions();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ILoggerFactory>(_ => nodeLoggerFactory);
        services.AddSingleton<IMeterFactory>(_ => new TestMeterFactory());
        services.AddSingleton<IHostApplicationLifetime>(
            _ => new SimulationHostApplicationLifetime(harness.TeardownCancellationToken));
        services.AddSingleton(new SharedResources(
            Context.TimeProvider,
            Context.TaskScheduler,
            Context.Random,
            Context.Random.NextGuid,
            harness.TeardownCancellationToken));
        services.AddSingleton<IMessagingClient>(sp => new InMemoryMessagingClient(
            harness,
            this,
            address,
            sp.GetRequiredService<IOptions<RapidClusterProtocolOptions>>().Value,
            sp.GetRequiredService<IOptions<PingPongFailureDetectorOptions>>().Value));
        services.AddSingleton(sp => new RapidClusterMetrics(sp.GetRequiredService<IMeterFactory>(), cluster: null));

        services
            .AddRapidClusterManual(
                options =>
                {
                    options.SeedAddresses = seedAddresses?.Select(s => s.ToEndPointPreferIP()).ToList();
                    options.Metadata = metadata?.Metadata_.ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.ToByteArray(),
                        StringComparer.Ordinal) ?? [];
                },
                Context.TimeProvider)
            .UseListenAddress(address.ToEndPointPreferIP())
            .ConfigureProtocol(options => CopyOptions(configuredProtocolOptions, options))
            .ConfigureFailureDetector(options => CopyOptions(configuredFailureDetectorOptions, options));

        // Preserve simulation coverage for protocol edge cases which production validation rejects.
        services.RemoveAll<IValidateOptions<RapidClusterProtocolOptions>>();

        _serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });
        _membershipService = _serviceProvider.GetRequiredService<MembershipService>();
        _viewAccessor = _serviceProvider.GetRequiredService<IMembershipViewAccessor>();
        _lifecycle = _serviceProvider.GetRequiredService<IRapidClusterLifecycle>();
        MessagingClient = (InMemoryMessagingClient)_serviceProvider.GetRequiredService<IMessagingClient>();
    }

    /// <summary>
    /// Initializes the membership service (completes the join or cluster start).
    /// </summary>
    internal async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycle.StartAsync(cancellationToken);
        _isInitialized = true;

        _log.NodeInitialized(RapidClusterUtils.Loggable(Address), CurrentView.Size, CurrentView.ConfigurationId.Version);
    }

    /// <summary>
    /// Handles an incoming request from another node.
    /// </summary>
    internal async Task<RapidClusterResponse> HandleRequestAsync(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        _log.HandlingRequest(RapidClusterUtils.Loggable(Address), request.ContentCase);

        // Link the caller's cancellation token with our disposal token so that
        // in-flight requests complete when this node is disposed
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        return await _membershipService.HandleMessageAsync(request, linkedCts.Token);
    }

    /// <summary>
    /// Gracefully stops the node by notifying observers and waiting for background tasks.
    /// The node can still receive messages after this method returns.
    /// Call <see cref="DisposeAsync"/> after unregistering from the network to release resources.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) return;

        _log.NodeLeaving(RapidClusterUtils.Loggable(Address));

        // Graceful stop: notify observers and wait for background tasks
        // Keep MessagingClient alive so we can still participate in consensus
        await _lifecycle.StopAsync(cancellationToken);

        _log.NodeLeftGracefully(RapidClusterUtils.Loggable(Address));
    }

    /// <summary>
    /// Disposes the node's resources. Should be called after unregistering from the network.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancel any in-flight requests first so they complete promptly
        _disposeCts.SafeCancel(_log.Logger);

        await _serviceProvider.DisposeAsync();
        _disposeCts.Dispose();
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"{RapidClusterUtils.Loggable(Address)} Size={MembershipSize} {(IsSuspended ? "Suspended" : "Running")}");

    private static void CopyOptions(RapidClusterProtocolOptions source, RapidClusterProtocolOptions destination)
    {
        destination.BatchingWindow = source.BatchingWindow;
        destination.ConsensusFallbackTimeoutBaseDelay = source.ConsensusFallbackTimeoutBaseDelay;
        destination.LeaveMessageTimeout = source.LeaveMessageTimeout;
        destination.ObserversPerSubject = source.ObserversPerSubject;
        destination.HighWatermark = source.HighWatermark;
        destination.LowWatermark = source.LowWatermark;
        destination.MaxJoinRetries = source.MaxJoinRetries;
        destination.JoinRetryBaseDelay = source.JoinRetryBaseDelay;
        destination.JoinRetryMaxDelay = source.JoinRetryMaxDelay;
        destination.JoinRetryBackoffMultiplier = source.JoinRetryBackoffMultiplier;
        destination.RejoinBackoffBase = source.RejoinBackoffBase;
        destination.RejoinBackoffMax = source.RejoinBackoffMax;
        destination.StaleViewRefreshInterval = source.StaleViewRefreshInterval;
        destination.UnstableModeTimeout = source.UnstableModeTimeout;
    }

    private static void CopyOptions(PingPongFailureDetectorOptions source, PingPongFailureDetectorOptions destination)
    {
        destination.Interval = source.Interval;
        destination.ProbeTimeout = source.ProbeTimeout;
        destination.ConsecutiveFailuresThreshold = source.ConsecutiveFailuresThreshold;
    }

    /// <summary>
    /// Simple meter factory for test metrics collection.
    /// </summary>
    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }

            _meters.Clear();
        }
    }

    private sealed class SimulationHostApplicationLifetime(CancellationToken applicationStopping) : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => default;

        public CancellationToken ApplicationStopping => applicationStopping;

        public CancellationToken ApplicationStopped => applicationStopping;

        public void StopApplication()
        {
        }
    }
}
