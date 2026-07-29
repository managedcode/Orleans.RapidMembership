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
[SuppressMessage("Usage", "CA2213:Disposable fields should be disposed",
    Justification = "The service provider owns node services, and DisposeAsync releases the provider and cancellation source.")]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class RapidSimulationNode : SimulationNode, IAsyncDisposable
{
    private readonly RapidSimulationCluster _harness;
    private readonly string _networkAddress;
    private readonly IReadOnlyList<Endpoint>? _seedAddresses;
    private readonly RapidClusterProtocolOptions? _protocolOptions;
    private readonly PingPongFailureDetectorOptions? _failureDetectorOptions;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly CancellationTokenSource _disposeCts = new();
    private RapidSimulationNodeLogger _log = null!;
    private ILoggerFactory _nodeLoggerFactory = null!;
    private MembershipService _membershipService = null!;
    private IRapidClusterLifecycle _lifecycle = null!;
    private ServiceProvider? _serviceProvider;
    private bool _disposed;
    private bool _isInitialized;

    /// <summary>
    /// Gets the endpoint address of this node.
    /// </summary>
    public Endpoint Address { get; }

    /// <summary>
    /// Gets the network address of this node as a string for routing.
    /// </summary>
    public override string NetworkAddress => _networkAddress;

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
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with a single seed address.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        SimulationNodeContext context,
        string networkAddress,
        Endpoint address,
        Endpoint? seedAddress,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
        : this(harness, context, networkAddress, address, seedAddress != null ? [seedAddress] : null, metadata, protocolOptions, failureDetectorOptions, loggerFactory)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with multiple seed addresses for testing seed failover.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        SimulationNodeContext context,
        string networkAddress,
        Endpoint address,
        IReadOnlyList<Endpoint>? seedAddresses,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
    {
        _harness = harness;
        _networkAddress = networkAddress;
        Address = address;
        Context = context;
        _seedAddresses = seedAddresses;
        _protocolOptions = protocolOptions;
        _failureDetectorOptions = failureDetectorOptions;
        _loggerFactory = loggerFactory;
        Metadata = metadata;
    }

    internal Metadata? Metadata { get; }

    internal void Initialize()
    {
        if (_nodeLoggerFactory is not null)
        {
            throw new InvalidOperationException("The simulation node is already initialized.");
        }

        // Wrap the logger factory to prepend the node name to all log messages
        var baseLoggerFactory = _loggerFactory ?? _harness.LoggerFactory ?? NullLoggerFactory.Instance;
        _nodeLoggerFactory = new NonDisposingLoggerFactory(baseLoggerFactory);

        _log = new RapidSimulationNodeLogger(_nodeLoggerFactory.CreateLogger<RapidSimulationNode>());

        var configuredProtocolOptions = _protocolOptions ?? new RapidClusterProtocolOptions();
        var configuredFailureDetectorOptions = _failureDetectorOptions ?? new PingPongFailureDetectorOptions();
        var simulationTimeProvider = ControlledTimeProvider.System;
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddSingleton<ILoggerFactory>(_ => _nodeLoggerFactory);
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
                    options.Metadata = Metadata?.Metadata_.ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.ToByteArray(),
                        StringComparer.Ordinal) ?? [];
                },
                simulationTimeProvider)
            .UseListenAddress(Address.ToEndPointPreferIP())
            .ConfigureProtocol(options => CopyOptions(configuredProtocolOptions, options))
            .ConfigureFailureDetector(options => CopyOptions(configuredFailureDetectorOptions, options));

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
    }

    /// <summary>
    /// Initializes the membership service (completes the join or cluster start).
    /// </summary>
    internal Task StartAsync(CancellationToken cancellationToken = default)
        => _harness.RunInNodeContext(Address, () => _lifecycle.StartAsync(cancellationToken));

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
        return _harness.RunInNodeContext(Address, () => _lifecycle.StopAsync(cancellationToken));
    }

    internal void CompleteStop() => _log.NodeLeftGracefully(RapidClusterUtils.Loggable(Address));

    /// <summary>
    /// Disposes the node's resources. Should be called after unregistering from the network.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        // Cancel any in-flight requests first so they complete promptly
#pragma warning disable CA1849 // Synchronous cancellation keeps callbacks on the simulation thread.
        _disposeCts.Cancel();
#pragma warning restore CA1849
        var disposalTask = Task.Factory.StartNew(
            static async state =>
            {
                var node = (RapidSimulationNode)state!;
                try
                {
                    if (node._serviceProvider is { } serviceProvider)
                    {
                        var providerDisposal = node._harness.RunInNodeContext(
                            node.Address,
                            serviceProvider.DisposeAsync);

                        await providerDisposal;
                    }
                }
                finally
                {
                    node._disposeCts.Dispose();
                }
            },
            state: this,
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            Context.TaskScheduler).Unwrap();
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

    private sealed class NonDisposingLoggerFactory(ILoggerFactory inner) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider) => inner.AddProvider(provider);

        public ILogger CreateLogger(string categoryName) => inner.CreateLogger(categoryName);

        public void Dispose()
        {
        }
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
