using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using Clockwork;
using Clockwork.Shims.System;
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
/// Lifetime is managed asynchronously by the simulation harness.
/// </summary>
[SuppressMessage("Reliability", "CA1001:Types that own disposable fields should be disposable",
    Justification = "Lifetime is managed by the simulation harness.")]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class RapidSimulationNode : SimulationNode
{
    private readonly RapidSimulationCluster _harness;
    private readonly RapidSimulationNodeLogger _log;
    private readonly ILoggerFactory _nodeLoggerFactory;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly IReadOnlyList<Endpoint>? _seedAddresses;
    private readonly Metadata? _metadata;
    private readonly RapidClusterProtocolOptions _configuredProtocolOptions;
    private readonly PingPongFailureDetectorOptions _configuredFailureDetectorOptions;
    private ServiceProvider _serviceProvider = null!;
    private MembershipService _membershipService = null!;
    private IRapidClusterLifecycle _lifecycle = null!;
    private bool _disposed;
    private bool _isInitialized;
    private bool _servicesInitialized;

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
    public MembershipView CurrentView => ViewAccessor.CurrentView;

    /// <summary>
    /// Gets the membership view accessor for subscribing to view changes.
    /// </summary>
    public IMembershipViewAccessor ViewAccessor { get; private set; } = null!;

    /// <summary>
    /// Gets a value indicating whether gets whether this node is initialized and part of a cluster.
    /// </summary>
    public override bool IsInitialized => _isInitialized;

    /// <summary>
    /// Gets the membership size of this node's view.
    /// </summary>
    public int MembershipSize => ViewAccessor.CurrentView.Size;

    /// <summary>
    /// Gets the messaging client for testing purposes.
    /// </summary>
    internal InMemoryMessagingClient MessagingClient { get; private set; } = null!;

    /// <summary>
    /// Gets the cancellation token that is triggered when this node is being torn down.
    /// </summary>
    internal CancellationToken TeardownCancellationToken => _disposeCts.Token;

    /// <summary>
    /// Executes one ready operation for this node.
    /// </summary>
    internal bool Step() => Context.Step(TestContext.Current.CancellationToken);

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with a single seed address.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        SimulationNodeContext context,
        Endpoint address,
        Endpoint? seedAddress,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
        : this(harness, context, address, seedAddress != null ? [seedAddress] : null, metadata, protocolOptions, failureDetectorOptions, loggerFactory)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with multiple seed addresses for testing seed failover.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        SimulationNodeContext context,
        Endpoint address,
        IReadOnlyList<Endpoint>? seedAddresses,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
    {
        using var guardScope = harness.Guard.Enter();
        _harness = harness;
        Address = address;
        Context = context;

        _nodeLoggerFactory = loggerFactory ?? harness.LoggerFactory;

        _log = new RapidSimulationNodeLogger(_nodeLoggerFactory.CreateLogger<RapidSimulationNode>());
        _seedAddresses = seedAddresses;
        _metadata = metadata;
        _configuredProtocolOptions = protocolOptions ?? new RapidClusterProtocolOptions();
        _configuredFailureDetectorOptions =
            failureDetectorOptions ?? new PingPongFailureDetectorOptions();
    }

    /// <summary>
    /// Resolves services which construct controlled synchronization primitives.
    /// </summary>
    internal bool InitializeServices()
    {
        if (_servicesInitialized)
        {
            return false;
        }

        var simulationTimeProvider = ControlledTimeProvider.System;
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddSingleton(_nodeLoggerFactory);
        _ = services.AddSingleton<IMeterFactory>(_ => new TestMeterFactory());
        _ = services.AddSingleton<IHostApplicationLifetime>(
            _ => new SimulationHostApplicationLifetime(_disposeCts.Token));
        _ = services.AddSingleton(new SharedResources(
            simulationTimeProvider,
            shuttingDownToken: _disposeCts.Token));
        _ = services.AddSingleton<IMessagingClient>(sp => new InMemoryMessagingClient(
            _harness,
            this,
            Address,
            sp.GetRequiredService<IOptions<RapidClusterProtocolOptions>>().Value,
            sp.GetRequiredService<IOptions<PingPongFailureDetectorOptions>>().Value));
        _ = services.AddSingleton(sp => new RapidClusterMetrics(sp.GetRequiredService<IMeterFactory>(), cluster: null));

        _ = services
            .AddRapidClusterManual(
                options =>
                {
                    options.SeedAddresses = _seedAddresses?.Select(s => s.ToEndPointPreferIP()).ToList();
                    options.Metadata = _metadata?.Metadata_.ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.ToByteArray(),
                        StringComparer.Ordinal) ?? [];
                },
                simulationTimeProvider)
            .UseListenAddress(Address.ToEndPointPreferIP())
            .ConfigureProtocol(options => CopyOptions(_configuredProtocolOptions, options))
            .ConfigureFailureDetector(options => CopyOptions(_configuredFailureDetectorOptions, options));

        // Preserve simulation coverage for protocol edge cases which production validation rejects.
        _ = services.RemoveAll<IValidateOptions<RapidClusterProtocolOptions>>();

        _serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });

        _membershipService = _serviceProvider.GetRequiredService<MembershipService>();
        ViewAccessor = _serviceProvider.GetRequiredService<IMembershipViewAccessor>();
        _lifecycle = _serviceProvider.GetRequiredService<IRapidClusterLifecycle>();
        MessagingClient = (InMemoryMessagingClient)_serviceProvider.GetRequiredService<IMessagingClient>();
        _servicesInitialized = true;
        return true;
    }

    /// <summary>
    /// Initializes the membership service (completes the join or cluster start).
    /// </summary>
    internal Task StartAsync(CancellationToken cancellationToken = default)
        => _harness.ScheduleInNodeContext(
            Address,
            () => _lifecycle.StartAsync(cancellationToken));

    internal void CompleteInitialization()
    {
        _isInitialized = true;

        _log.NodeInitialized(RapidClusterUtils.Loggable(Address), CurrentView.Size, CurrentView.ConfigurationId.Version);
    }

    /// <summary>
    /// Handles an incoming request from another node.
    /// </summary>
    internal async Task<RapidClusterResponse> HandleRequestAsync(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        _log.HandlingRequest(RapidClusterUtils.Loggable(Address), request.ContentCase);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        return await _membershipService.HandleMessageAsync(request, linkedCts.Token);
    }

    /// <summary>
    /// Gracefully stops the node by notifying observers and waiting for background tasks.
    /// The node can still receive messages after this method returns.
    /// Call <see cref="DisposeAsync"/> after unregistering from the network to release resources.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) return Task.CompletedTask;

        _log.NodeLeaving(RapidClusterUtils.Loggable(Address));

        // Graceful stop: notify observers and wait for background tasks
        // Keep MessagingClient alive so we can still participate in consensus
        return _harness.ScheduleInNodeContext(
            Address,
            () => _lifecycle.StopAsync(cancellationToken));
    }

    internal void CompleteStop() => _log.NodeLeftGracefully(RapidClusterUtils.Loggable(Address));

    /// <summary>
    /// Disposes the node's resources. Should be called after unregistering from the network.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;
        _isInitialized = false;

        // Cancel any in-flight requests first so they complete promptly
#pragma warning disable CA1849 // Synchronous cancellation keeps callbacks on the simulation thread.
        _disposeCts.Cancel();
#pragma warning restore CA1849
        var disposalTask = _harness.ScheduleInNodeContext(
            Address,
            async () =>
            {
                try
                {
                    await _serviceProvider.DisposeAsync();
                }
                finally
                {
                    _disposeCts.Dispose();
                }
            });
        return new ValueTask(disposalTask);
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
