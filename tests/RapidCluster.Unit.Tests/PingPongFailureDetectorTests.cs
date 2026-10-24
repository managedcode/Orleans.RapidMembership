using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for PingPongFailureDetector.
/// </summary>
public sealed class PingPongFailureDetectorTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();

    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public void Constructor_WithDefaults_CreatesInstance()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { });

        Assert.NotNull(detector);
    }

    [Fact]
    public void Constructor_WithCustomThreshold_CreatesInstance()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            options: new PingPongFailureDetectorOptions { ConsecutiveFailuresThreshold = 5 });

        Assert.NotNull(detector);
    }

    [Fact]
    public async Task ConsecutiveFailures_AtThreshold_NotifiesFailure()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;

        // All probes fail
        client.ResponseGenerator = _ => throw new InvalidOperationException("Probe failed");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            options: new PingPongFailureDetectorOptions
            {
                ConsecutiveFailuresThreshold = 3,
                Interval = TimeSpan.FromSeconds(1),
            },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // First two failures (not yet at threshold)
        for (var i = 0; i < 2; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(failureNotified);
        }

        // Third failure (at threshold)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(failureNotified);
    }

    [Fact]
    public async Task ConsecutiveFailures_BelowThreshold_DoesNotNotify()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;

        // All probes fail
        client.ResponseGenerator = _ => throw new InvalidOperationException("Probe failed");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            options: new PingPongFailureDetectorOptions
            {
                ConsecutiveFailuresThreshold = 5,
                Interval = TimeSpan.FromSeconds(1),
            },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // Only 4 failures (below threshold of 5)
        for (var i = 0; i < 4; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        Assert.False(failureNotified);
    }

    [Fact]
    public async Task ProbeSuccess_ResetsConsecutiveFailures()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;

        // First probe fails, second succeeds, third fails
        var responseIndex = 0;
        client.ResponseGenerator = _ =>
        {
            var idx = responseIndex++;
            if (idx == 1)
            {
                // Second probe succeeds
                return new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse();
            }

            throw new InvalidOperationException("Probe failed");
        };

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            options: new PingPongFailureDetectorOptions
            {
                ConsecutiveFailuresThreshold = 2,
                Interval = TimeSpan.FromSeconds(1),
            },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // First probe (failure)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Second probe (success - resets count)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Third probe (failure again - count is now 1, not 2)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Should NOT have notified because success reset the counter
        Assert.False(failureNotified);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // Multiple disposes should not throw
        detector.Dispose();
        detector.Dispose();
        detector.Dispose();
    }

    [Fact]
    public async Task Dispose_ThrowingCancellationCallback_PropagatesAfterCleanup()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var callbackRegistered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pendingResponse = new TaskCompletionSource<RapidClusterResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenRegistration registration = default;
        client.AsyncResponseGenerator = (_, cancellationToken) =>
        {
            registration = cancellationToken.Register(static () => throw new InvalidOperationException("Cancellation callback failure."));
            callbackRegistered.SetResult();
            return pendingResponse.Task;
        };

        var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            options: new PingPongFailureDetectorOptions { Interval = TimeSpan.FromSeconds(1) },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);
        detector.Start();
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await callbackRegistered.Task.WaitAsync(TestContext.Current.CancellationToken);

        _ = Assert.Throws<AggregateException>(detector.Dispose);
        detector.Dispose();
        await registration.DisposeAsync();
    }

    [Fact]
    public void Start_AfterDispose_Throws()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Dispose();

        _ = Assert.Throws<ObjectDisposedException>(detector.Start);
    }

    [Fact]
    public async Task StaleViewDetected_WhenRemoteHasHigherConfigId()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var staleViewDetected = false;
        var detectedRemoteConfigId = ConfigurationId.Empty;
        var detectedLocalConfigId = ConfigurationId.Empty;

        client.ResponseGenerator = _ => new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            onStaleViewDetected: (_, remoteConfig, localConfig) =>
            {
                staleViewDetected = true;
                detectedRemoteConfigId = remoteConfig;
                detectedLocalConfigId = localConfig;
            },
            viewAccessor: new TestMembershipViewAccessor(Utils.CreateMembershipView(numNodes: 5, configVersion: 5)),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(staleViewDetected);
        Assert.Equal(10, detectedRemoteConfigId.Version);
        Assert.Equal(5, detectedLocalConfigId.Version);
    }

    [Fact]
    public async Task StaleViewNotDetected_WhenLocalConfigIsHigher()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var staleViewDetected = false;

        client.ResponseGenerator = _ => new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 5).ToProtobuf() }.ToRapidClusterResponse();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            onStaleViewDetected: (_, _, _) => staleViewDetected = true,
            viewAccessor: new TestMembershipViewAccessor(Utils.CreateMembershipView(numNodes: 5, configVersion: 10)),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.False(staleViewDetected);
    }

    [Fact]
    public async Task TimeoutException_CountsAsFailure()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;

        client.ResponseGenerator = _ => throw new TimeoutException("Request timed out");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            options: new PingPongFailureDetectorOptions
            {
                ConsecutiveFailuresThreshold = 2,
                Interval = TimeSpan.FromSeconds(1),
            },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(failureNotified);
    }

    [Fact]
    public async Task ReplyFromUnjoinedProcessOnSubjectEndpoint_CountsAsFailureUntilMemberReplies()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;
        var member = false;

        // A restarted process at the subject's endpoint answers as bootstrapping with no cluster identity.
        client.ResponseGenerator = _ => member
            ? new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 5).ToProtobuf() }.ToRapidClusterResponse()
            : new ProbeResponse { Status = Pb.NodeStatus.Bootstrapping, ConfigurationId = ConfigurationId.Empty.ToProtobuf() }.ToRapidClusterResponse();

        using var detector = CreateMemberDetector(subject, observer, client, sharedResources, () => failureNotified = true, threshold: 3);
        detector.Start();

        // Two unjoined replies are failures; a reply from the joined member of this cluster resets the count.
        await AdvanceProbesAsync(timeProvider, 2);
        member = true;
        await AdvanceProbesAsync(timeProvider, 1);
        member = false;
        await AdvanceProbesAsync(timeProvider, 2);
        Assert.False(failureNotified);

        await AdvanceProbesAsync(timeProvider, 1);
        Assert.True(failureNotified);
    }

    [Fact]
    public async Task ReplyFromAnotherClusterOnSubjectEndpoint_CountsAsFailure()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        await using var client = new TestMessagingClient();
        var failureNotified = false;
        var staleViewDetected = false;

        client.ResponseGenerator = _ => new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(999), 10).ToProtobuf() }.ToRapidClusterResponse();

        using var detector = CreateMemberDetector(subject, observer, client, sharedResources, () => failureNotified = true, threshold: 2,
            onStaleViewDetected: (_, _, _) => staleViewDetected = true);
        detector.Start();

        await AdvanceProbesAsync(timeProvider, 2);

        Assert.True(failureNotified);
        Assert.False(staleViewDetected);
    }

    private static PingPongFailureDetector CreateMemberDetector(Endpoint subject, Endpoint observer, TestMessagingClient client,
        SharedResources sharedResources, Action notifier, int threshold,
        Action<Endpoint, ConfigurationId, ConfigurationId>? onStaleViewDetected = null) => new(
            subject,
            observer,
            client,
            sharedResources,
            notifier,
            options: new PingPongFailureDetectorOptions
            {
                ConsecutiveFailuresThreshold = threshold,
                Interval = TimeSpan.FromSeconds(1),
            },
            onStaleViewDetected: onStaleViewDetected,
            viewAccessor: new TestMembershipViewAccessor(Utils.CreateMembershipView(numNodes: 3, configVersion: 5)),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

    private static async Task AdvanceProbesAsync(FakeTimeProvider timeProvider, int probes)
    {
        for (var probe = 0; probe < probes; probe++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public void Factory_CreatesDetectorWithCorrectConfiguration()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);

        using var client = new TestMessagingClient();
        var localEndpoint = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        var failureDetectorOptions = Microsoft.Extensions.Options.Options.Create(new PingPongFailureDetectorOptions
        {
            ConsecutiveFailuresThreshold = 5,
            Interval = TimeSpan.FromSeconds(2),
        });

        var factory = new PingPongFailureDetectorFactory(
            new StaticListenAddressProvider(localEndpoint.ToEndPoint()),
            client,
            sharedResources,
            failureDetectorOptions,
            CreateMetrics(),
            NullLogger<PingPongFailureDetector>.Instance);

        using var detector = factory.CreateInstance(subject, () => { });

        Assert.NotNull(detector);
        _ = Assert.IsType<PingPongFailureDetector>(detector);
    }

    /// <summary>
    /// Test messaging client that allows customizing responses.
    /// </summary>
    private sealed class TestMembershipViewAccessor(MembershipView view) : IMembershipViewAccessor
    {
        public MembershipView CurrentView => view;

        public BroadcastChannelReader<MembershipView> Updates => throw new NotSupportedException();
    }

    private sealed class TestMessagingClient : IMessagingClient, IDisposable
    {
        public Func<RapidClusterRequest, RapidClusterResponse>? ResponseGenerator { get; set; }

        public Func<RapidClusterRequest, CancellationToken, Task<RapidClusterResponse>>? AsyncResponseGenerator { get; set; }

        public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
        }

        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
            => AsyncResponseGenerator?.Invoke(request, cancellationToken)
                ?? Task.FromResult(ResponseGenerator?.Invoke(request) ?? new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
}
